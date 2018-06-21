package info.oner.generator;

import info.oner.job.KafkaToInfluxDB;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

@Log4j
public class PushDataToKafka {

    private static final int MIN_HOST_PER_SERVICE = 100;
    private static final int HOST_PER_SERVICE_INC = 100;

    private static final String KEY_FORMAT = "%d host=%s service=%s colo=slca status=%d";


    private int NUMBER_OF_SERVICES = 1;

    public static void main(String[] args) {
        new PushDataToKafka().run();
    }


    private void run() {
        try {
            //Fill with TEST_TIME_MINUTES worth of data
            fillTopics();
        } catch (Exception e) {
            log.error("", e);
        } finally {
            System.out.println("completed");
        }
    }

    // Creates 2 topics (t1, t2) with different partition counts (32, 64) and fills them with items
    private void fillTopics() throws IOException {
        System.out.println("Filling Topics. Press Ctrl+C to stop");

        Properties sourceProps = KafkaToInfluxDB.readProperties("source.properties");

        String bootstrapServers = sourceProps.getProperty("bootstrap.servers");
        String topic = sourceProps.getProperty("topics").split(",")[0];

        try (KafkaProducer<String, Double> producer = new KafkaProducer<>(kafkaProps(bootstrapServers))) {
            IntStream.iterate(0, i -> i + 1)
                    .forEach(i -> {
                        //long ts = finalStart + TimeUnit.MINUTES.toMillis(i);
                        System.out.println(i);
                        IntStream.range(0, NUMBER_OF_SERVICES)
                                .parallel()
                                .forEach(j -> {
                                    ThreadLocalRandom random = ThreadLocalRandom.current();
                                    int hostPerService = MIN_HOST_PER_SERVICE + j * HOST_PER_SERVICE_INC;
                                    for (int k = 0; k < hostPerService; k++) {
                                        double reqPerSecS = random.nextDouble(0.1, 4.0);
                                        double reqPerSecF = random.nextDouble(0.1, 2.0);
                                        //
                                        long ts = System.currentTimeMillis();
                                        try {
                                            producer.send(new ProducerRecord<>(topic, createMessage(ts, k, j, 0), reqPerSecS));
                                            producer.send(new ProducerRecord<>(topic, createMessage(ts, k, j, 1), reqPerSecF));
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                            //NOOP
                                        }
                                    }
                                });
                    });
        }
    }

    private String createMessage(long ts, int hostid, int serviceid, int status) {
        return String.format(KEY_FORMAT, ts, "host" + hostid, "service" + serviceid, status);
    }

    public Properties kafkaProps(String bootstrapServers) {
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DoubleSerializer.class.getCanonicalName());
        return prop;
    }
}
