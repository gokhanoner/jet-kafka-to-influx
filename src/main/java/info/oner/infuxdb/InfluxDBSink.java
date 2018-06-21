package info.oner.infuxdb;

import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class InfluxDBSink {

    public static Sink<Point> newSink(Properties props) {
        return Sinks.<InfluxDB, Point>builder((unused) -> InfluxDBSink.createConnection(props))
                .onReceiveFn(InfluxDBSink::insertPoint)
                .flushFn(InfluxDBSink::flushData)
                .destroyFn(InfluxDBSink::cleanup)
                .build();
    }

    private static InfluxDB createConnection(Properties props) {
        InfluxDB influxDB = InfluxDBFactory.connect(
                props.getProperty("url"),
                props.getProperty("username"),
                props.getProperty("password"));
        influxDB.setDatabase(props.getProperty("database"));
        influxDB.setRetentionPolicy(props.getProperty("retentionPolicy"));
        influxDB.enableBatch(
                Integer.parseInt(props.getProperty("batch.actions")),
                Integer.parseInt(props.getProperty("batch.flushDurationMs")),
                TimeUnit.MILLISECONDS);
        return influxDB;
    }

    private static void insertPoint(InfluxDB influxDB, Point data) {
        influxDB.write(data);
    }

    private static void flushData(InfluxDB influxDB) {
        influxDB.flush();
    }

    private static void cleanup(InfluxDB influxDB) {
        influxDB.close();
    }

}
