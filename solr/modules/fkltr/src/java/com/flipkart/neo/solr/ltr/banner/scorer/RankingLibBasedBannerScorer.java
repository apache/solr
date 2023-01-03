package com.flipkart.neo.solr.ltr.banner.scorer;

import java.util.Collection;

import com.flipkart.ad.selector.model.context.ScoringContext;
import com.flipkart.neo.content.ranking.scorers.L1Scorer;
import com.flipkart.neo.content.ranking.scorers.L2Scorer;
import com.flipkart.neo.solr.ltr.banner.entity.HitBanner;
import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.banner.scorer.context.NeoScoringContext;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;

public class RankingLibBasedBannerScorer implements BannerScorer {
  private final L1Scorer l1Scorer;
  private final L2Scorer l2Scorer;
  private final NeoFKLTRConfigs neoFKLTRConfigs;

  public RankingLibBasedBannerScorer(L1Scorer l1Scorer, L2Scorer l2Scorer, NeoFKLTRConfigs neoFKLTRConfigs) {
    this.l1Scorer = l1Scorer;
    this.l2Scorer = l2Scorer;
    this.neoFKLTRConfigs = neoFKLTRConfigs;
  }

  @Override
  public void updateL1ScoreAndDemandFeatures(IndexedBanner indexedBanner) {
    l1Scorer.updateFeaturesAndL1Score(indexedBanner);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<HitBanner> l2Score(Collection<HitBanner> hitBanners, NeoRescoringContext rescoringContext) {
    ScoringContext scoringContext = new NeoScoringContext(rescoringContext);

    hitBanners = (Collection<HitBanner>) l2Scorer.score(scoringContext, hitBanners);

    if (neoFKLTRConfigs.isIsMultiShardSetup()) {
      // Updating modelInfo for each banner.
      // Ideally it should be per shard, but it is not possible to convey this message via solrResponse.
      hitBanners.forEach(hitBanner -> hitBanner.setModelTypeToModelInfo(scoringContext.getModelTypeToModelInfo()));
    } else {
      // For single shard scenario, we can get away with setting in only one banner.
      if (!hitBanners.isEmpty()) {
        hitBanners.iterator().next().setModelTypeToModelInfo(scoringContext.getModelTypeToModelInfo());
      }
    }

    if (rescoringContext.isDebugInfoRequired()) {
      rescoringContext.setFeatureToFeatureWeightMap(scoringContext.getFeatureToFeatureWeightMap());
      rescoringContext.setFilteredRejectedContents(scoringContext.getFilteredRejectedContents());
    }

    return hitBanners;
  }
}
