package org.apache.solr.update;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;

import java.lang.invoke.MethodHandles;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LeafSorter {

    Comparator<LeafReader> getLeafSorter();
}

final class SegmentTimeLeafSorter implements LeafSorter {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static final String TIME_FIELD = "timestamp";
    private static final SegmentSort defaultSortOptions = SegmentSort.NONE;

    private SegmentSort sortOptions;
    private Comparator<LeafReader> leafSorter;

    public SegmentTimeLeafSorter(SegmentSort sortOptions) {
        this.sortOptions = sortOptions;
        this.leafSorter = null;
    }

    @Override
    public Comparator<LeafReader> getLeafSorter() {
        if (leafSorter != null) {
            return leafSorter;
        }
        if (SegmentSort.NONE == sortOptions) {
            return null;
        }
        boolean ascSort = SegmentSort.TIME_ASC == sortOptions;
        long missingValue = ascSort? Long.MAX_VALUE : Long.MIN_VALUE;
        leafSorter = Comparator.comparingLong(r -> {
            try {
                return Long.parseLong(((SegmentReader) r).getSegmentInfo().info.getDiagnostics().get(TIME_FIELD));
            } catch (Exception e) {
                log.error("Error getting time stamp for SegmentReader", e);
                return missingValue;
            }
        });
        if (!ascSort) {
            leafSorter = leafSorter.reversed();
        }
        return leafSorter;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(50);
        sb.append("SegmentTimeLeafSorter{");
        sb.append(sortOptions.toString());
        sb.append('}');
        return sb.toString();
    }
}

