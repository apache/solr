package com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.scorer.allocator.L1DocumentAllocator;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.ScoringSchemeExecutor;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import com.flipkart.solr.ltr.utils.MetricPublisher;
import org.apache.lucene.search.ScoreDoc;

public class L1GroupLimitSchemeExecutor extends ScoringSchemeExecutor {
  private final L1DocumentAllocator l1DocumentAllocator;
  private final MetricPublisher metricPublisher;

  public L1GroupLimitSchemeExecutor(L1DocumentAllocator l1DocumentAllocator, NeoFKLTRConfigs neoFKLTRConfigs,
                                    MetricPublisher metricPublisher) {
    super(neoFKLTRConfigs);
    this.l1DocumentAllocator = l1DocumentAllocator;
    this.metricPublisher = metricPublisher;
  }

  // Note - Not overly interested in removing redundancy between groupAndLimit and groupAndL2Score because groupAndLimit is
  // supposed to be temporary and do not want to compromise on performance of either.
  // NOTE:fkltr l1Explore(according to current definition) would not work properly because -
  // i) A shard would explore for a group only if the number of docs local to it exceed the limit.
  // ii) Currently explore just alters the order and does not change actual score and shard-merge layer re-orders based on score.
  // Although above pitfalls will can be avoided by having only one shard and that is too prohibitive.
  @Override
  public ScoreDoc[] executeAndReturnSortedDocs(ScoreDoc[] hits, NeoRescoringContext rescoringContext,
                                               ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    long l1GroupAndLimitStartTime = System.currentTimeMillis();
    Map<String, List<ScoreDoc>> groupedScoreDocs =
        l1DocumentAllocator.allocatePerGroup(hits, rescoringContext);
    Collection<ScoreDoc> uniqueScoreDocs = getUniqueScoreDocs(groupedScoreDocs);
    // returned docs are in random order and should not be used for pagination.
    ScoreDoc[] finalHits = uniqueScoreDocs.toArray(new ScoreDoc[0]);
    metricPublisher.updateTimer(L1_GROUP_AND_LIMIT, System.currentTimeMillis() - l1GroupAndLimitStartTime);
    return finalHits;
  }

  /**
   * @param groupedScoreDocs grouped scoreDocs.
   * @return returns unique scoreDocs across all the groups.
   */
  private Collection<ScoreDoc> getUniqueScoreDocs(Map<String, List<ScoreDoc>> groupedScoreDocs) {
    int upperLimitForUniqueDocs = groupedScoreDocs.values().stream().mapToInt(List::size).sum();
    Map<Integer, ScoreDoc> scoreDocMap = new HashMap<>(upperLimitForUniqueDocs, 1);

    groupedScoreDocs.values().forEach(groupDocs -> {
      for (ScoreDoc scoreDoc: groupDocs) {
        scoreDocMap.putIfAbsent(scoreDoc.doc, scoreDoc);
      }
    });

    return scoreDocMap.values();
  }
}
