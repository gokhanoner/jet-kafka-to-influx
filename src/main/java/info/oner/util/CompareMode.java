package info.oner.util;

public enum CompareMode {

    /**
     * In this mode, averages will be calculated in a sliding window so each value aggregated with the latest
     * avg of last n items
     */
    LATEST,

    /**
     * Average calculated with a tumbling window & each value aggregated with last calculated avg
     */
    LAST,

    /**
     * Average calculated with a tumbling window & each value aggregated with closest avg. If closes avg is not
     * available, values will wait until it is available
     */
    CLOSEST
}
