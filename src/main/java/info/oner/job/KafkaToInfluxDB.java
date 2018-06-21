package info.oner.job;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.TwoBags;
import com.hazelcast.jet.kafka.KafkaSources;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.jet.server.JetBootstrap;
import info.oner.infuxdb.InfluxDBSink;
import info.oner.model.Event;
import info.oner.util.CompareMode;
import info.oner.util.KafkaHelper;
import info.oner.util.RunMode;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.jet.aggregate.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingDouble;
import static com.hazelcast.jet.aggregate.AggregateOperations.toTwoBags;
import static com.hazelcast.jet.pipeline.WindowDefinition.*;
import static info.oner.infuxdb.InfluxDBHelper.mapToPoint;
import static info.oner.util.KafkaHelper.properties;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A sample which consumes two Kafka topics and writes
 * the received items to an {@code IMap}.
 **/
public class KafkaToInfluxDB {

    public static void main(String[] args) throws Exception {
        System.out.println("Arguments: " + Arrays.toString(args));
        System.out.println("  " + KafkaToInfluxDB.class.getSimpleName() + " <runMode>");
        System.out.println();
        System.out.println("<runMode> - \"LOCAL\" or \"CLUSTER\", default is \"LOCAL\"");

        RunMode runMode = RunMode.LOCAL;
        if (args.length > 0) runMode = RunMode.valueOf(args[0]);

        JetInstance jet;
        if (RunMode.CLUSTER == runMode) {
            jet = JetBootstrap.getInstance();
        } else {
            System.setProperty("hazelcast.phone.home.enabled", "false");
            System.setProperty("hazelcast.logging.type", "log4j");
            JetConfig cfg = new JetConfig();
            cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                    Math.max(1, getRuntime().availableProcessors() / 2)));
            Jet.newJetInstance(cfg);
            jet = Jet.newJetInstance(cfg);
        }

        try {
            System.out.println("Submitting Job");

            JobConfig jobConfig = new JobConfig();
            jobConfig.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
            jobConfig.setSnapshotIntervalMillis(SECONDS.toMillis(10));
            jobConfig.setSplitBrainProtection(true);

            Job job = jet.newJob(buildPipeline(), jobConfig);
            System.out.println("Submitted");

            System.out.println("Press any key to cancel the job");

            System.in.read();

            System.out.println("Cancelling job...");
            job.cancel();

            Thread.sleep(5000);

        } finally {
            Jet.shutdownAll();
        }
    }

    public static Properties readProperties(String filename) throws IOException {
        try (InputStream is = KafkaToInfluxDB.class.getClassLoader().getResourceAsStream(filename)) {
            Properties prop = new Properties();
            prop.load(is);
            return prop;
        }
    }

    private static Pipeline buildPipeline() throws Exception {

        Properties sourceProps = readProperties("source.properties");
        Properties sinkProps = readProperties("sink.properties");

        String bootstrapServers = sourceProps.getProperty("bootstrap.servers");
        String offsetReset = sourceProps.getProperty("auto.offset.reset");
        String topicList = sourceProps.getProperty("topics");
        String[] topics = topicList.split(",");

        CompareMode compareMode = CompareMode.valueOf(sourceProps.getProperty("compareMode"));
        long maxLag = Long.parseLong(sourceProps.getProperty("maxLagMs"));
        TimeUnit timeUnit = TimeUnit.valueOf(sourceProps.getProperty("timeUnit", "MINUTES"));
        int srcPar = Integer.parseInt(sourceProps.getProperty("parallelism"));

        int sinkPar = Integer.parseInt(sinkProps.getProperty("parallelism"));

        Pipeline p = Pipeline.create();

        StreamStage<Event> events = p
                .drawFrom(KafkaSources.<String, Double, Event>kafka(properties(bootstrapServers, offsetReset), KafkaHelper::mapToMessage, topics))
                .addTimestamps(Event::getTimestamp, maxLag)
                .setLocalParallelism(srcPar)
                //This is the heartbeat filter
                .filter(e -> e.getValue() >= 0);

        StreamStage<Event> successfulRequests = events.filter(e -> e.getStatus() == 0);
        StreamStage<Event> failedRequests = events.filter(e -> e.getStatus() != 0);

        WindowDefinition avgWindow = null;
        WindowDefinition joinWindow = null;

        switch (compareMode) {
            case LATEST:
                avgWindow = sliding(timeUnit.toMillis(10), timeUnit.toMillis(1));
                joinWindow = tumbling(timeUnit.toMillis(1));
                break;
            case LAST:
            case CLOSEST:
                avgWindow = tumbling(timeUnit.toMillis(10));
                joinWindow = sliding(timeUnit.toMillis(10), timeUnit.toMillis(1));
                break;
        }

        WindowDefinition sumWindow = tumbling(timeUnit.toMillis(1));

        //Create 1 min sum stream grouped by service name
        StreamStage<TimestampedEntry<String, Double>> oneMinSuccessSum = createSumPerService(successfulRequests, sumWindow);

        StreamStage<TimestampedEntry<String, Double>> oneMinFailSum = createSumPerService(failedRequests, sumWindow);

        //Cretae 10 min avearage by service name
        StreamStage<TimestampedEntry<String, Double>> tenMinSuccessAvg = createAvgPerService(oneMinSuccessSum, avgWindow);

        StreamStage<TimestampedEntry<String, Double>> tenMinFailAvg = createAvgPerService(oneMinFailSum, avgWindow);

        //Create dif percentage
        StreamStage<TimestampedEntry<String, Tuple2<TimestampedEntry<String, Double>, TimestampedEntry<String, Double>>>> successDiff =
                createDataToAvgDiff(oneMinSuccessSum, tenMinSuccessAvg, joinWindow, timeUnit, compareMode);

        StreamStage<TimestampedEntry<String, Double>> avgDiffSuccess = successDiff
                .map(te -> new TimestampedEntry(te.getTimestamp(), te.getKey(),
                        ((te.getValue().f0().getValue() - te.getValue().f1().getValue()) / te.getValue().f0().getValue()) * 100));


        StreamStage<TimestampedEntry<String, Tuple2<TimestampedEntry<String, Double>, TimestampedEntry<String, Double>>>> failDiff =
                createDataToAvgDiff(oneMinFailSum, tenMinFailAvg, joinWindow, timeUnit, compareMode);

        StreamStage<TimestampedEntry<String, Double>> avgDiffFail = failDiff
                .map(te -> new TimestampedEntry(te.getTimestamp(), te.getKey(),
                        ((te.getValue().f0().getValue() - te.getValue().f1().getValue()) / te.getValue().f0().getValue()) * 100));


        //This is just to see something in the logs, can be commented out.
        successDiff.drainTo(Sinks.logger());

        //Write each min sum to InfluxDB
        oneMinSuccessSum
                .map(te -> mapToPoint("success1m", te))
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);

        oneMinFailSum
                .map(te -> mapToPoint("fail1m", te))
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);

        tenMinSuccessAvg
                .map(te -> mapToPoint("success10m", te))
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);

        tenMinFailAvg
                .map(te -> mapToPoint("fail10m", te))
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);


        avgDiffSuccess.map(te -> mapToPoint("avgDiffSuccess", te))
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);


        avgDiffFail.map(te -> mapToPoint("avgDiffFail", te))
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);

        //See processed events per windows with process latency
        //Format: TIME, NUMBER OF EVENTS, LATENCY(ms)
        events
                .window(tumbling(SECONDS.toMillis(1)))
                .aggregate(counting(), (start, end, result) -> {
                    long timeMs = System.currentTimeMillis();
                    long latencyMs = timeMs - end - maxLag;
                    return mapToPoint("STATS_" + topicList, result, latencyMs, end);
                })
                .drainTo(InfluxDBSink.newSink(sinkProps))
                .setLocalParallelism(sinkPar);

        return p;
    }

    private static StreamStage<TimestampedEntry<String, Double>> createSumPerService(StreamStage<Event> stream, WindowDefinition windowDef) {
        return stream
                .groupingKey(Event::getService)
                .window(windowDef)
                .aggregate(summingDouble(Event::getValue));
    }


    private static StreamStage<TimestampedEntry<String, Double>> createAvgPerService(StreamStage<TimestampedEntry<String, Double>> stream, WindowDefinition windowDef) {
        return stream
                .groupingKey(TimestampedEntry::getKey)
                .window(windowDef)
                .aggregate(averagingDouble(TimestampedEntry::getValue));
    }


    private static StreamStage<TimestampedEntry<String, Tuple2<TimestampedEntry<String, Double>, TimestampedEntry<String, Double>>>> createDataToAvgDiff(
            StreamStage<TimestampedEntry<String, Double>> dataStream,
            StreamStage<TimestampedEntry<String, Double>> avgStream,
            WindowDefinition joinWindow,
            TimeUnit timeUnit,
            CompareMode compareMode) {


        StreamStage<TimestampedEntry<String, TwoBags<TimestampedEntry<String, Double>, TimestampedEntry<String, Double>>>> agg = dataStream
                .groupingKey(TimestampedEntry::getKey)
                .window(joinWindow)
                .aggregate2(avgStream.groupingKey(TimestampedEntry::getKey), toTwoBags())
                .filter(e -> !e.getValue().bag0().isEmpty() && !e.getValue().bag1().isEmpty());

        if (compareMode == CompareMode.CLOSEST) {
            return agg.flatMap(e -> {
                TimestampedEntry<String, Double> max = Collections.max(e.getValue().bag0(), Comparator.comparing(TimestampedEntry::getTimestamp));
                TimestampedEntry<String, Double> avg = Collections.max(e.getValue().bag1(), Comparator.comparing(TimestampedEntry::getTimestamp));

                //If equal, new window is starting. Previous close ones waiting, get all of them
                if(max.getTimestamp() == avg.getTimestamp()) {
                    return Traversers.traverseStream(
                            e.getValue().bag0().stream()
                            .filter(t -> avg.getTimestamp() - t.getTimestamp() <= timeUnit.toMillis(5))
                            .map(t -> new TimestampedEntry<>(t.getTimestamp(), e.getKey(), Tuple2.tuple2(t, avg))));
                } else if (max.getTimestamp() - avg.getTimestamp() >= timeUnit.toMillis(5)) {
                    //skip those since they're close to the next one
                    return Traversers.empty();
                } else {
                    //Just return the last one
                    return Traverser.over(new TimestampedEntry<>(max.getTimestamp(), e.getKey(), Tuple2.tuple2(max, avg)));
                }

            });
        } else {
            return agg.map(e -> {
                TimestampedEntry<String, Double> max = Collections.max(e.getValue().bag0(), Comparator.comparing(TimestampedEntry::getTimestamp));
                TimestampedEntry<String, Double> avg = Collections.max(e.getValue().bag1(), Comparator.comparing(TimestampedEntry::getTimestamp));
                return new TimestampedEntry<>(e.getTimestamp(), e.getKey(), Tuple2.tuple2(max, avg));
            });
        }
    }
}
