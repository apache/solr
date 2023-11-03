package org.apache.solr.crossdc.manager;

import org.apache.solr.crossdc.common.MirroredSolrRequest;
import org.apache.solr.crossdc.common.ResubmitBackoffPolicy;

public class NoOpResubmitBackoffPolicy implements ResubmitBackoffPolicy {

    public long getBackoffTimeMs(MirroredSolrRequest resubmitRequest) {
        return 0;
    }
}
