package info.oner.util;


import info.oner.model.Event;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;
import java.util.UUID;

public class KafkaHelper {

    public static Properties properties(String bootstrapServers, String offsetReset) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getCanonicalName());
        prop.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offsetReset);
        return prop;
    }

    public static Event mapToMessage(ConsumerRecord<String, Double> entry) {
        //"%d host=%s service=%s colo=slca status=%s"
        String[] parts = entry.key().split(" ");

        long timestamp = Long.parseLong(parts[0]);
        String host = parts[1].split("=")[1];
        String service = parts[2].split("=")[1];
        String colo = parts[3].split("=")[1];
        int status = Integer.parseInt(parts[4].split("=")[1]);
        return Event.of(service, host, colo, status, timestamp, entry.value());
    }
}
