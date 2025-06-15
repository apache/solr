package org.apache.solr.search.combine;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocList;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class QueryAndResponseCombiner {

    protected int upTo;

    protected QueryAndResponseCombiner(SolrParams requestParams) {
        this.upTo = requestParams.getInt(CombinerParams.COMBINER_UP_TO, CombinerParams.COMBINER_UP_TO_DEFAULT);
    }

    public abstract QueryResult combine(QueryResult[] rankedLists);

    public abstract List<ShardDoc> combine(Map<String, List<ShardDoc>> shardDocMap);
    public abstract NamedList<Explanation> getExplanations(
            String[] queryKeys,
            List<Query> queries,
            List<DocList> resultsPerQuery,
            SolrIndexSearcher searcher,
            IndexSchema schema)
            throws IOException;

    public static QueryAndResponseCombiner getImplementation(SolrParams requestParams) {
        String algorithm = requestParams.get(CombinerParams.COMBINER_ALGORITHM, CombinerParams.RECIPROCAl_RANK_FUSION);
        if (algorithm.equals(CombinerParams.RECIPROCAl_RANK_FUSION)) {
            return new ReciprocalRankFusion(requestParams);
        }
        throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Unknown Combining algorithm: " + algorithm);
    }
}
