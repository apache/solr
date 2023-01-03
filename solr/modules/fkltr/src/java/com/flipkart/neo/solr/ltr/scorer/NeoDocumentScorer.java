package com.flipkart.neo.solr.ltr.scorer;

import java.lang.invoke.MethodHandles;
import java.util.*;

import com.flipkart.ad.selector.filter.UserSegmentFilter;
import com.flipkart.neo.content.ranking.model.ModelManager;
import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L1ScoringConfig;
import com.flipkart.neo.solr.ltr.response.models.DebugResponse;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.scorer.allocator.L1DocumentAllocator;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.ScoringSchemeExecutor;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl.L1GroupLimitL2SchemeExecutor;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl.L1GroupLimitSchemeExecutor;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl.L1LimitL2SchemeExecutor;
import com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl.L1LimitSchemeExecutor;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import com.flipkart.neo.solr.ltr.query.models.ScoringScheme;
import com.flipkart.solr.ltr.scorer.FKLTRDocumentScorer;
import com.flipkart.solr.ltr.utils.MetricPublisher;
import org.apache.lucene.search.ScoreDoc;
import com.flipkart.solr.ltr.utils.Parallelizer;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NeoDocumentScorer implements FKLTRDocumentScorer<NeoRescoringContext, ScoreMeta> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String L1_RANKING = "l1Ranking";
  private static final String L1_RANKING_SIZE = "l1RankingSize";
  private static final String RETURNED_BANNERS_SIZE = "returnedBannersSize";
  private static final String SEARCHER_BANNER_CACHE_MISS = "searcherBannerCacheMiss";

  private final String searcherBannerCacheName;
  private final Parallelizer parallelizer;

  private final NeoFKLTRConfigs neoFKLTRConfigs;

  private final ModelManager modelManager;

  private final MetricPublisher metricPublisher;

  private final Map<ScoringScheme, ScoringSchemeExecutor> scoringSchemeExecutorMap;

  public NeoDocumentScorer(String searcherBannerCacheName,
                           Parallelizer parallelizer,
                           L1DocumentAllocator l1DocumentAllocator,
                           BannerScorer bannerScorer,
                           ModelManager modelManager,
                           NeoFKLTRConfigs neoFKLTRConfigs,
                           MetricPublisher metricPublisher) {
    this.searcherBannerCacheName = searcherBannerCacheName;
    this.parallelizer = parallelizer;
    this.neoFKLTRConfigs = neoFKLTRConfigs;
    this.metricPublisher = metricPublisher;
    this.scoringSchemeExecutorMap = getScoringSchemeExecutorMap(l1DocumentAllocator, bannerScorer, neoFKLTRConfigs, metricPublisher);
    this.modelManager = modelManager;
  }

  private Map<ScoringScheme, ScoringSchemeExecutor> getScoringSchemeExecutorMap(L1DocumentAllocator l1DocumentAllocator,
                                                                                BannerScorer bannerScorer,
                                                                                NeoFKLTRConfigs neoFKLTRConfigs,
                                                                                MetricPublisher metricPublisher) {
    // building the map here, not polluting the ScoringScheme enum.
    Map<ScoringScheme, ScoringSchemeExecutor> scoringSchemeToExecutorMap = new EnumMap<>(ScoringScheme.class);
    scoringSchemeToExecutorMap.put(ScoringScheme.L1_GROUP_LIMIT,
        new L1GroupLimitSchemeExecutor(l1DocumentAllocator, neoFKLTRConfigs, metricPublisher));
    scoringSchemeToExecutorMap.put(ScoringScheme.L1_LIMIT,
        new L1LimitSchemeExecutor(l1DocumentAllocator, neoFKLTRConfigs, metricPublisher));
    scoringSchemeToExecutorMap.put(ScoringScheme.L1_GROUP_L2,
        new L1GroupLimitL2SchemeExecutor(l1DocumentAllocator, bannerScorer, neoFKLTRConfigs, metricPublisher));
    scoringSchemeToExecutorMap.put(ScoringScheme.L1_L2,
        new L1LimitL2SchemeExecutor(l1DocumentAllocator, bannerScorer, neoFKLTRConfigs, metricPublisher));
    return scoringSchemeToExecutorMap;
  }

  @Override
  public ScoreDoc[] scoreAndReturnSortedDocs(ScoreDoc[] hits, NeoRescoringContext rescoringContext,
                                             ScoreMetaHolder<ScoreMeta> scoreMetaHolder, SolrIndexSearcher searcher) {
    l1ScoreAndUpdateMetaAndSegmentCheck(hits, rescoringContext, searcher);

    ScoreDoc[] finalHits = scoringSchemeExecutorMap.get(rescoringContext.getScoringSchemeConfig().getScoringScheme())
        .executeAndReturnSortedDocs(hits, rescoringContext, scoreMetaHolder);

    metricPublisher.updateHistogram(RETURNED_BANNERS_SIZE, finalHits.length);

    if (rescoringContext.isDebugInfoRequired()) {
      populateDebugInfo(hits, finalHits, scoreMetaHolder, rescoringContext);
    }

    return finalHits;
  }

  private void l1ScoreAndUpdateMetaAndSegmentCheck(ScoreDoc[] hits,
                                                   NeoRescoringContext rescoringContext,
                                                   SolrIndexSearcher searcher) {
    long l1RankingStartTime = System.currentTimeMillis();

    @SuppressWarnings({"unchecked"})
    SolrCache<Integer, IndexedBanner> newBannerCache = searcher.getCache(searcherBannerCacheName);

    boolean segmentFilterRequired = rescoringContext.getSegmentFilterRequired();
    Map<Integer, Set<Integer>> userSegments =
        UserSegmentFilter.adaptUserSegmentInfo(rescoringContext.getUserSegments());
    parallelizer.parallelConsume(hits.length, neoFKLTRConfigs.getL1ParallelizationLevel(), offset -> {
      ScoreDoc hit = hits[offset];
      IndexedBanner banner = null;
      try {
        banner = newBannerCache.get(hit.doc);
        if (banner != null &&
            (!segmentFilterRequired || isSegmentFilterPassed(banner, userSegments))) {
          hit.meta = banner;
          hit.score = getAdjustedL1Score(banner, rescoringContext.getL1ScoringConfig()).floatValue();
        } else {
          if (banner == null) {
            log.error("banner not found in searcherBannerCache");
            metricPublisher.updateMeter(SEARCHER_BANNER_CACHE_MISS);
          }
          // penalising the missedBanner, so it never reflects up in the order.
          hit.score = -1;
        }
      } catch (Exception e) {
        log.error(String.format("error while getting l1Score for bannerId: %s using ModelId: %s",
            (banner != null ? banner.getId() : null), rescoringContext.getL1ScoringConfig().getModelId()), e);
        // penalising the missedBanner, so it never reflects up in the order.
        hit.score = -1;
      }
    });

    metricPublisher.updateTimer(L1_RANKING, System.currentTimeMillis() - l1RankingStartTime);
    metricPublisher.updateHistogram(L1_RANKING_SIZE, hits.length);
    log.info("l1RankingSize: {}", hits.length);
  }

  private boolean isSegmentFilterPassed(IndexedBanner banner, Map<Integer, Set<Integer>> userSegments) {
    return UserSegmentFilter.isSegmentFilterPassed(banner.getBannerSegmentInfo(), userSegments);
  }

  private Double getAdjustedL1Score(IndexedBanner banner,
                                    L1ScoringConfig l1ScoringConfig) {
    Double multiplier = l1ScoringConfig.getCreativeTemplateIdToL1ScoreMultiplier() == null ? null :
        l1ScoringConfig.getCreativeTemplateIdToL1ScoreMultiplier().get(banner.getCreativeTemplateId());
    Double score = banner.getL1Score(l1ScoringConfig.getModelId());
    Double adjustedScore = multiplier == null ? score : multiplier * score;
//    log.debug("bannerId, ctId, originalScore, adjustedScore: {} {} {} {}", banner.getId(),
//        banner.getCreativeTemplateId(), score, adjustedScore);
    return adjustedScore;
  }

  // Ideally debugResponse should be per shard but not possible with existing solrResponse format.
  // Setting debugResponse in the hit with highest score to prevent possibility of elimination during shardMerge.
  private void populateDebugInfo(ScoreDoc[] hits, ScoreDoc[] finalHits, ScoreMetaHolder<ScoreMeta> scoreMetaHolder,
                                 NeoRescoringContext rescoringContext) {
    if (finalHits.length < 1) return;

    ScoreDoc hitWithMaxScore = finalHits[0];
    float maxScore = hitWithMaxScore.score;

    for (ScoreDoc hit: finalHits) {
      if (hit.score > maxScore) {
        maxScore = hit.score;
        hitWithMaxScore = hit;
      }
    }

    ScoreMeta scoreMeta = scoreMetaHolder.getScoreMeta(hitWithMaxScore.doc);
    if (scoreMeta == null) {
      scoreMeta = new ScoreMeta();
    }
    scoreMeta.setDebugResponse(createDebugResponse(hits, rescoringContext));
    scoreMetaHolder.putScoreMeta(hitWithMaxScore.doc, scoreMeta);
  }

  private DebugResponse createDebugResponse(ScoreDoc[] hits, NeoRescoringContext rescoringContext) {
    DebugResponse debugResponse = new DebugResponse();

    // setting matchedBanners.
    Set<String> matchedBanners = new HashSet<>(hits.length, 1);
    for (ScoreDoc hit: hits) {
      if (hit.meta == null) {
        log.error("meta null for doc: {}", hit.doc);
        continue;
      }

      IndexedBanner indexedBanner = (IndexedBanner) hit.meta;
      matchedBanners.add(indexedBanner.getId());
    }
    debugResponse.setMatchedBanners(matchedBanners);

    // setting featureToFeatureWeightMap.
    debugResponse.setFeatureToFeatureWeightMap(rescoringContext.getFeatureToFeatureWeightMap());

    // setting filteredRejectedContents.
    debugResponse.setFilteredRejectedContents(rescoringContext.getFilteredRejectedContents());

    // setting activeModelKeys.
    Set<String> activeModels = new HashSet<>();
    modelManager.getModelWrapperIterator().forEachRemaining(entry ->
        activeModels.add(entry.getModel().getModelData().getModelKey().getKey()));
    debugResponse.setActiveModelKeys(activeModels);

    return debugResponse;
  }
}