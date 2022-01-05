package org.apache.solr.search;

import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class BackgroundJoinWarmerQP extends JoinQParserPlugin{
    static AtomicInteger inserts = new AtomicInteger();

    public BackgroundJoinWarmerQP(){
        super((query, index) -> new EventualJoinCacheWrapper(query, index) {
            @Override
            protected DocsetTimestamp createEntry(SolrIndexSearcher solrIndexSearcher, Query joinQuery, long fromCoreTimestamp) throws IOException {
                inserts.incrementAndGet();
                return super.createEntry(solrIndexSearcher, joinQuery, fromCoreTimestamp);
            }
        });
    }
}
