package org.apache.solr.update;

public enum SegmentSort {
    /**
     * No segment sort
     */
    NONE,
    /**
     * Sort leaf reader by segment creation time ascending order
     */
    TIME_ASC,
    /**
     * Sort leaf reader by segment creation time descending order
     */
    TIME_DESC
}
