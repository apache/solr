package org.apache.solr.search.neural;

import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.knn.KnnSearchStrategy;

public class SolrKnnFloatVectorQuery extends KnnFloatVectorQuery {
  public SolrKnnFloatVectorQuery(String field, float[] target, int k, Query filter, KnnSearchStrategy searchStrategy) {
    super(field, target, k, filter, searchStrategy);
  }

  public KnnSearchStrategy getStrategy() {
    return searchStrategy;
  }
}
