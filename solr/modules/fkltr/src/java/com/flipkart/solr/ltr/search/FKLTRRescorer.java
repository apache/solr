package com.flipkart.solr.ltr.search;

import com.flipkart.solr.ltr.query.BaseRescoringContext;
import com.flipkart.solr.ltr.query.FKLTRScoringQuery;
import com.flipkart.solr.ltr.query.score.meta.ConcurrentMapBasedScoreMetaHolder;
import com.flipkart.solr.ltr.scorer.FKLTRDocumentScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Rescorer;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Implements the rescoring logic. The top documents returned by solr will be processed
 * according to {@link FKLTRScoringQuery} and will be assigned a new score.
 * The top documents will be resorted based on the new score.
 * */
public class FKLTRRescorer<RescoringContext extends BaseRescoringContext, ScoreMeta> extends Rescorer {
  private final FKLTRDocumentScorer<RescoringContext, ScoreMeta> fkltrDocumentScorer;
  private final FKLTRScoringQuery<RescoringContext, ScoreMeta> scoringQuery;

  FKLTRRescorer(FKLTRDocumentScorer<RescoringContext, ScoreMeta> fkltrDocumentScorer,
                FKLTRScoringQuery<RescoringContext, ScoreMeta> scoringQuery) {
    this.fkltrDocumentScorer = fkltrDocumentScorer;
    this.scoringQuery = scoringQuery;
  }

  /**
   * rescores the documents:
   *
   * @param searcher
   *          current IndexSearcher
   * @param firstPassTopDocs
   *          documents to rerank;
   * @param topN
   *          documents to return;
   */
  @Override
  public TopDocs rescore(IndexSearcher searcher, TopDocs firstPassTopDocs, int topN) {
    if ((topN == 0) || (firstPassTopDocs.totalHits.value == 0)) {
      return firstPassTopDocs;
    }

    scoringQuery.setScoreMetaHolder(new ConcurrentMapBasedScoreMetaHolder<>());

    SolrIndexSearcher solrSearcher = (SolrIndexSearcher) searcher;
    ScoreDoc[] hits = firstPassTopDocs.scoreDocs;

    ScoreDoc[] finalHits = fkltrDocumentScorer.scoreAndReturnSortedDocs(hits, scoringQuery.getRescoringContext(),
        scoringQuery.getScoreMetaHolder(), solrSearcher);

    return new TopDocs(firstPassTopDocs.totalHits, finalHits);
  }

  @Override
  public Explanation explain(IndexSearcher searcher,
                             Explanation firstPassExplanation, int docID) {
    // NOTE:fkltr not very useful for our use case.
    return Explanation.match(0, "placeHolder");
  }
}
