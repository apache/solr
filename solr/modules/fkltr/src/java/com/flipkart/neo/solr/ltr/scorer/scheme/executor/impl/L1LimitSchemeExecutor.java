package com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl;

import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.scorer.allocator.L1DocumentAllocator;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.ScoringSchemeExecutor;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import com.flipkart.solr.ltr.utils.MetricPublisher;
import org.apache.lucene.search.ScoreDoc;

public class L1LimitSchemeExecutor extends ScoringSchemeExecutor {

  private final L1DocumentAllocator l1DocumentAllocator;
  private final MetricPublisher metricPublisher;

  public L1LimitSchemeExecutor(L1DocumentAllocator l1DocumentAllocator, NeoFKLTRConfigs neoFKLTRConfigs,
                               MetricPublisher metricPublisher) {
    super(neoFKLTRConfigs);
    this.l1DocumentAllocator = l1DocumentAllocator;
    this.metricPublisher = metricPublisher;
  }

  // Note - Not overly interested in removing redundancy between Limit and LimitAndL2Score because Limit is
  // supposed to be temporary and do not want to compromise on performance of either.
  @Override
  public ScoreDoc[] executeAndReturnSortedDocs(ScoreDoc[] hits, NeoRescoringContext rescoringContext,
                                               ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    long l1LimitStartTime = System.currentTimeMillis();
    int limit = rescoringContext.getScoringSchemeConfig().getL2Limit();
    ScoreDoc[] finalHits;
    if (hits.length > limit) {
      l1DocumentAllocator.allocate(hits, limit);
      finalHits = new ScoreDoc[limit];
      System.arraycopy(hits, 0, finalHits, 0, limit);
    } else {
      finalHits = hits;
    }
    metricPublisher.updateTimer(L1_LIMIT, System.currentTimeMillis() - l1LimitStartTime);

    return finalHits;
  }
}
