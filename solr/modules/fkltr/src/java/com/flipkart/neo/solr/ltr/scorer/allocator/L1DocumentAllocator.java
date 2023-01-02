package com.flipkart.neo.solr.ltr.scorer.allocator;

import java.util.List;
import java.util.Map;

import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import org.apache.lucene.search.ScoreDoc;

public interface L1DocumentAllocator {
  /**
   * Allocates top contents per group.
   * @param docs input docs
   * @param rescoringContext rescoringContext
   * @return map containing top docs per group.
   */
  Map<String, List<ScoreDoc>> allocatePerGroup(ScoreDoc[] docs, NeoRescoringContext rescoringContext);

  /**
   * Performs unordered partial sorting.
   * Reorders the input docs in such a way that topN docs are in first topN positions.
   * @param docs input docs.
   * @param topN topN.
   */
  void allocate(ScoreDoc[] docs, int topN);
}
