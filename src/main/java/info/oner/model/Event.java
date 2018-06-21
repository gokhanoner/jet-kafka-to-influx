package info.oner.model;

import lombok.Data;

import java.io.Serializable;

@Data(staticConstructor = "of")
public class Event implements Serializable {

    private final String service;
    private final String hostname;
    private final String colo;
    private final int status;
    private final long timestamp;
    private final double value;
}
