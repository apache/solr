package com.flipkart.neo.solr.ltr.scorer.scheme.executor.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

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

public class L1LimitL2SchemeExecutor extends ScoringSchemeExecutor {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final L1DocumentAllocator l1DocumentAllocator;
  private final BannerScorer bannerScorer;
  private final MetricPublisher metricPublisher;

  public L1LimitL2SchemeExecutor(L1DocumentAllocator l1DocumentAllocator, BannerScorer bannerScorer,
                                 NeoFKLTRConfigs neoFKLTRConfigs, MetricPublisher metricPublisher) {
    super(neoFKLTRConfigs);
    this.l1DocumentAllocator = l1DocumentAllocator;
    this.bannerScorer = bannerScorer;
    this.metricPublisher = metricPublisher;
  }

  @Override
  public ScoreDoc[] executeAndReturnSortedDocs(ScoreDoc[] hits, NeoRescoringContext rescoringContext,
                                               ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    long l1LimitStartTime = System.currentTimeMillis();
    Collection<HitBanner> l2Banners = getLimitedBannersForL2(hits, rescoringContext.getScoringSchemeConfig().getL2Limit());
    metricPublisher.updateTimer(L1_LIMIT, System.currentTimeMillis() - l1LimitStartTime);

    long l2RankingStartTime = System.currentTimeMillis();
    l2Banners = l2Rank(bannerScorer, l2Banners, rescoringContext, scoreMetaHolder);
    metricPublisher.updateTimer(L2_RANKING, System.currentTimeMillis() - l2RankingStartTime);
    metricPublisher.updateHistogram(L2_RANKING_SIZE, l2Banners.size());
//    log.info("l2RankingSize: {}", l2Banners.size());

    long l2CollectionStartTime = System.currentTimeMillis();
    ScoreDoc[] finalHits = collectHits(l2Banners);
    metricPublisher.updateTimer(L2_COLLECTION, System.currentTimeMillis() - l2CollectionStartTime);

    return finalHits;
  }

  private Collection<HitBanner> getLimitedBannersForL2(ScoreDoc[] hits, int limit) {
    if (hits.length > limit) {
      l1DocumentAllocator.allocate(hits, limit);
    } else {
      limit = hits.length;
    }

    List<HitBanner> banners = new ArrayList<>(limit);
    for (int i=0; i<limit; i++) {
      ScoreDoc hit = hits[i];
      if (hit.meta == null) continue;
      banners.add(new HitBanner((IndexedBanner) hit.meta, hit));
    }

    return banners;
  }

  private ScoreDoc[] collectHits(Collection<HitBanner> banners) {
    ScoreDoc[] hits = collectScoreDocsFromBanners(banners);

    if (neoFKLTRConfigs.isPaginationEnabledForNonGroupQuery()) {
      Arrays.sort(hits, (o1, o2) -> Double.compare(o2.score, o1.score));
    }

    return hits;
  }
}
