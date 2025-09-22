package org.apache.solr.search.neural;

import org.apache.lucene.search.KnnByteVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.knn.KnnSearchStrategy;

public class SolrKnnByteVectorQuery extends KnnByteVectorQuery {
  public SolrKnnByteVectorQuery(String field, byte[] target, int k, Query filter, KnnSearchStrategy searchStrategy) {
    super(field, target, k, filter, searchStrategy);
  }

  public KnnSearchStrategy getStrategy() {
    return searchStrategy;
  }
}
