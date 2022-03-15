package org.apache.solr.search.join;

import org.apache.lucene.search.Query;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.*;

import java.io.IOException;
import java.util.List;

public class CrossCoreJoinCacheRegenerator implements CacheRegenerator {

    @Override
    public <K, V> boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache<K, V> newCache,
                                                  SolrCache<K, V> oldCache, K oldKey, V oldVal) throws IOException {
        if (!((ExtendedQuery) oldKey).getCache()){
            @SuppressWarnings("unchecked")
            final V docSet = (V) newSearcher.getDocSet((Query) oldKey);
            newCache.put(oldKey, docSet);
            return true;
        } else {
            throw new IllegalArgumentException(this + " regenerates only cache=false queries, but got " + oldKey);
        }
    }
}
