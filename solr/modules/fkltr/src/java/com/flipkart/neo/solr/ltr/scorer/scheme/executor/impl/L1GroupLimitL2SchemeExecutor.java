
package com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flipkart.neo.solr.ltr.banner.entity.HitBanner;
import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.scorer.allocator.L1DocumentAllocator;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.ScoringSchemeExecutor;
import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import com.flipkart.solr.ltr.utils.MetricPublisher;
import org.apache.lucene.search.ScoreDoc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class L1GroupLimitL2SchemeExecutor extends ScoringSchemeExecutor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final L1DocumentAllocator l1DocumentAllocator;
  private final BannerScorer bannerScorer;
  private final MetricPublisher metricPublisher;

  public L1GroupLimitL2SchemeExecutor(L1DocumentAllocator l1DocumentAllocator, BannerScorer bannerScorer,
                                      NeoFKLTRConfigs neoFKLTRConfigs, MetricPublisher metricPublisher) {
    super(neoFKLTRConfigs);
    this.l1DocumentAllocator = l1DocumentAllocator;
    this.bannerScorer = bannerScorer;
    this.metricPublisher = metricPublisher;
  }

  @Override
  public ScoreDoc[] executeAndReturnSortedDocs(ScoreDoc[] hits, NeoRescoringContext rescoringContext,
                                               ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    // L1 group and limit.
    long l1GroupAndLimitStartTime = System.currentTimeMillis();
    Map<String, List<ScoreDoc>> groupedScoreDocs =
        l1DocumentAllocator.allocatePerGroup(hits, rescoringContext);
    Collection<HitBanner> l2Banners = getUniqueBanners(groupedScoreDocs);
    metricPublisher.updateTimer(L1_GROUP_AND_LIMIT, System.currentTimeMillis() - l1GroupAndLimitStartTime);

    // L2 ranking
    long l2RankingStartTime = System.currentTimeMillis();
    l2Banners = l2Rank(bannerScorer, l2Banners, rescoringContext, scoreMetaHolder);
    metricPublisher.updateTimer(L2_RANKING, System.currentTimeMillis() - l2RankingStartTime);
    metricPublisher.updateHistogram(L2_RANKING_SIZE, l2Banners.size());
//    log.info("l2RankingSize: {}", l2Banners.size());

    // Collecting finalHits.
    long l2GroupCollectionStartTime = System.currentTimeMillis();
    ScoreDoc[] finalHits = collectHitsForGroupQuery(l2Banners, groupedScoreDocs);
    metricPublisher.updateTimer(L2_GROUP_COLLECTION, System.currentTimeMillis() - l2GroupCollectionStartTime);

    return finalHits;
  }

  private ScoreDoc[] collectHitsForGroupQuery(Collection<HitBanner> banners,
                                              Map<String, List<ScoreDoc>> groupedScoreDocs) {
    ScoreDoc[] hits;

    if (neoFKLTRConfigs.isPaginationEnabledForGroupQuery()) {
      hits = collectCustomGroupSortedScoreDocs(groupedScoreDocs, banners.size());
    } else {
      hits = collectScoreDocsFromBanners(banners);
    }

    return hits;
  }

  /**
   * @param groupedScoreDocs groupedScoreDocs
   * @param numberOfUniqueDocs numberOfUniqueDocs
   * @return flat array of scoreDocs which are arranged in such a way that resulting array can be cached and
   * used for pagination. Any cut of the flat array should have equal representation of all groups.
   */
  private ScoreDoc[] collectCustomGroupSortedScoreDocs(Map<String, List<ScoreDoc>> groupedScoreDocs,
                                                       int numberOfUniqueDocs) {
    ScoreDoc[] scoreDocs = new ScoreDoc[numberOfUniqueDocs];
    groupedScoreDocs.values().parallelStream()
        .forEach(list -> list.sort((o1, o2) -> Float.compare(o2.score, o1.score)));

    int iteration = 0;
    int index = 0;
    Set<Integer> selectedDocIds = new HashSet<>(numberOfUniqueDocs, 1);
    while (index < numberOfUniqueDocs) {
      for (List<ScoreDoc> groupedList: groupedScoreDocs.values()) {
        if (iteration >= groupedList.size()) continue;

        ScoreDoc scoreDoc = groupedList.get(iteration);
        if (!selectedDocIds.contains(scoreDoc.doc)) {
          scoreDocs[index++] = scoreDoc;
          selectedDocIds.add(scoreDoc.doc);
        }
      }
      iteration++;
    }

    return scoreDocs;
  }

  /**
   * @param groupedScoreDocs grouped scoreDocs.
   * @return returns unique banners across all the groups.
   */
  private Collection<HitBanner> getUniqueBanners(Map<String, List<ScoreDoc>> groupedScoreDocs) {
    int upperLimitForUniqueBanners = groupedScoreDocs.values().stream().mapToInt(List::size).sum();
    Map<Integer, HitBanner> bannerMap = new HashMap<>(upperLimitForUniqueBanners, 1);

    groupedScoreDocs.values().forEach(groupDocs -> {
      for (ScoreDoc scoreDoc: groupDocs) {
        if (!bannerMap.containsKey(scoreDoc.doc)) {
          HitBanner banner = new HitBanner((IndexedBanner) scoreDoc.meta, scoreDoc);
          bannerMap.put(scoreDoc.doc, banner);
        }
      }
    });

    return bannerMap.values();
  }
}
