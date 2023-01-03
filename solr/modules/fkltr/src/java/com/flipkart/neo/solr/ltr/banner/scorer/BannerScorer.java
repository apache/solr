package com.flipkart.neo.solr.ltr.banner.scorer;

import java.util.Collection;

import com.flipkart.neo.solr.ltr.banner.entity.HitBanner;
import com.flipkart.neo.solr.ltr.banner.entity.IndexedBanner;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;

public interface BannerScorer {
  void updateL1ScoreAndDemandFeatures(IndexedBanner indexedBanner);
  Collection<HitBanner> l2Score(Collection<HitBanner> hitBanners, NeoRescoringContext rescoringContext);
}
