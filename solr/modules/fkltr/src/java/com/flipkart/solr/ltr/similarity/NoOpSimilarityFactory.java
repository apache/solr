package com.flipkart.solr.ltr.similarity;

import org.apache.lucene.search.similarities.Similarity;
import org.apache.solr.schema.SimilarityFactory;

public class NoOpSimilarityFactory extends SimilarityFactory {
  @Override
  public Similarity getSimilarity() {
    return new NoOpSimilarity();
  }
}
