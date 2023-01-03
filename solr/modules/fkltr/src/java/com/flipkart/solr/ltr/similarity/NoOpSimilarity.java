package com.flipkart.solr.ltr.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;

public class NoOpSimilarity extends Similarity {
  private static final SimScorer simScorer = new NoOpSimScorer();

  @Override
  public long computeNorm(FieldInvertState state) {
    return 0;
  }

  @Override
  public SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
    return simScorer;
  }

  private static class NoOpSimScorer extends SimScorer {

    @Override
    public float score(float freq, long norm) {
      return 0;
    }
  }
}
