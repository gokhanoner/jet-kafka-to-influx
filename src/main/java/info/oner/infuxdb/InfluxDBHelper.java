package info.oner.infuxdb;

import com.hazelcast.jet.datamodel.TimestampedEntry;
import org.influxdb.dto.Point;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InfluxDBHelper {

    public static Point mapToPoint(String measurement, TimestampedEntry<String, ? extends Number> te) {
        return Point.measurement(measurement)
                .tag("service",te.getKey())
                .addField("value", te.getValue())
                .time(te.getTimestamp(), MILLISECONDS)
                .build();
    }

    public static Point mapToPoint(String measurement, long noop, long latency, long timestamp) {
        return Point.measurement(measurement)
                .addField("nOfMessages", noop)
                .addField("latency", latency)
                .time(timestamp, MILLISECONDS)
                .build();
    }
}
