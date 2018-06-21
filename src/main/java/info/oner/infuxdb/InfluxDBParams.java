package info.oner.infuxdb;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Builder
@Getter
public class InfluxDBParams implements Serializable {
    private String url;
    private String username;
    private String password;
    private String database;
    private String retentionPolicy;
    private int batchActions;
    private int batchFlushDuration;
}

