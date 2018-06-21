import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.WindowDefinition;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.Map;

import static com.hazelcast.jet.aggregate.AggregateOperations.*;

public class Test {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        JetConfig conf = new JetConfig();
        conf.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig().setMapName("source"));

        JetInstance instance = Jet.newJetInstance(conf);

        Map<Integer, Integer> source = instance.getMap("source");

        for (int i = 0; i < 30; i++) {
            source.put(i, i);
            //source.put(i, i);
        }

        Pipeline p = Pipeline.create();

        StreamStage<TimestampedItem<Long>> tumbling1 =
                p.drawFrom(Sources.<Integer, Integer>mapJournal("source", JournalInitialPosition.START_FROM_OLDEST))
                        .addTimestamps(e -> (long) e.getValue(), 0)
                        .window(WindowDefinition.tumbling(1))
                        .aggregate(summingLong(Map.Entry::getValue));

        //tumbling1.drainTo(Sinks.logger(e -> "1 min window: " + e));

        WindowDefinition sliding10 = WindowDefinition.sliding(10, 1);
        WindowDefinition tumbling10 = WindowDefinition.tumbling(10);

        StreamStage<TimestampedItem<Double>> average = tumbling1
                .window(tumbling10)
                .aggregate(averagingLong(TimestampedItem::item));
        //.drainTo(Sinks.logger(e -> "10 min window: " + e));

        tumbling1
                .window(sliding10)
                .aggregate2(average, toTwoBags())
                .drainTo(Sinks.logger(e -> "Average window: " + e));

        instance.newJob(p).join();
    }
}
