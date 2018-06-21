package info.oner.util;

public enum RunMode {

    /**
     * Create a 2 node embedded Jet Cluster & run the aggregation
     */
    LOCAL,

    /**
     * Connect to a remote Jet cluster as a client & submit the job
     */
    CLUSTER
}
