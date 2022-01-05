package org.apache.solr.search.join;

import org.apache.lucene.search.Query;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.*;
import org.apache.solr.util.RefCounted;

import java.io.IOException;
import java.util.List;

/**
 * It regenerates user cache of {!join cache=false}.. -&gt; (docset,from_index_timestamp)
 * */
public class CrossCoreJoinCacheRegenerator implements CacheRegenerator {

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache<K, V> newCache, SolrCache<K, V> oldCache, K oldKey, V oldVal) throws IOException {
        if (!((ExtendedQuery) oldKey).getCache()) {
            String fromIndex = null;
            final Query wrappedQuery = ((WrappedQuery) oldKey).getWrappedQuery();
            if (wrappedQuery instanceof  JoinQuery) {
                fromIndex = ((JoinQuery) wrappedQuery).fromIndex;
            } else {
                if (wrappedQuery instanceof ScoreJoinQParserPlugin.OtherCoreJoinQuery){
                    fromIndex = ((ScoreJoinQParserPlugin.OtherCoreJoinQuery) wrappedQuery).fromIndex;
                } else {
                    throw new IllegalArgumentException("Unable to regenerate " + wrappedQuery);
                }
            }
            JoinQParserPlugin.DocsetTimestamp cached = (JoinQParserPlugin.DocsetTimestamp) oldVal;

            long fromCoreTimestamp;
            final SolrCore fromCore = newSearcher.getCore().getCoreContainer().getCore(fromIndex);
            try {
                final RefCounted<SolrIndexSearcher> fromSearcher = fromCore.getSearcher();
                try {
                    fromCoreTimestamp = fromSearcher.get().getOpenNanoTime();
                } finally {
                    fromSearcher.decref();
                }
            } finally {
                fromCore.close();
            }
            // this is non-enforced warming.
            // Left side commit occurs, some entries might already be regenerated
            //final boolean toSideCommitRegeneration = oldCache != newCache;
            final boolean freshEntry = cached.getTimestamp() == fromCoreTimestamp;
            final Query oldQuery = (Query) oldKey;
            // toSideCommitRegeneration &&
            if (freshEntry) {
                onBypass(oldQuery);
                return true; // query cache warming already warmed this entry
            }
            onRegenerate(oldQuery);
            final DocSet docSet =  newSearcher.getDocSet(oldQuery);
            newCache.put(oldKey, (V) new JoinQParserPlugin.DocsetTimestamp(docSet, fromCoreTimestamp));
            return true;
        } else {
            throw new IllegalArgumentException(this + " regenerates only cache=false queries, but got " + oldKey);
        }
    }

    protected void onRegenerate(Query oldKey) {
        //test injection
    }

    protected void onBypass(Query oldKey) {
        //test injection
    }
}
