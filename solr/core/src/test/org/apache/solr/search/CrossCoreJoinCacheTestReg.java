package org.apache.solr.search;

import org.apache.lucene.search.Query;
import org.apache.solr.search.join.CrossCoreJoinCacheRegenerator;

import java.util.concurrent.atomic.AtomicInteger;

public class CrossCoreJoinCacheTestReg extends CrossCoreJoinCacheRegenerator {
    final static AtomicInteger regenerate = new AtomicInteger();
    final static AtomicInteger bypass = new AtomicInteger();

    @Override
    public void onRegenerate(Query oldKey){
        regenerate.incrementAndGet();
    }

    @Override
    protected void onBypass(Query oldKey) {
        bypass.incrementAndGet();
    }
}
