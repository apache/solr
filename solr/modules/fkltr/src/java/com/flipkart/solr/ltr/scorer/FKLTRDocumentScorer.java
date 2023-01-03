package com.flipkart.solr.ltr.scorer;

import com.flipkart.solr.ltr.query.BaseRescoringContext;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import org.apache.lucene.search.ScoreDoc;
import org.apache.solr.search.SolrIndexSearcher;

public interface FKLTRDocumentScorer<RescoringContext extends BaseRescoringContext, ScoreMeta> {
  /**
   * Rescores the hits according to rescoringContext and returns the final sorted docs.
   * Also updates the scoreMeta for each of the final docs.
   * @param hits hits
   * @param rescoringContext rescoringContext
   * @param scoreMetaHolder scoreMetaHolder
   * @param searcher searcher
   * @return final sorted docs.
   */
  ScoreDoc[] scoreAndReturnSortedDocs(ScoreDoc[] hits,
                                      RescoringContext rescoringContext,
                                      ScoreMetaHolder<ScoreMeta> scoreMetaHolder,
                                      SolrIndexSearcher searcher);
}
