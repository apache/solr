package com.flipkart.neo.solr.ltr.module;

import com.flipkart.neo.content.ranking.model.ModelManager;
import com.flipkart.neo.solr.ltr.banner.cache.BannerCache;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;

public interface NeoScoringModule {

  BannerCache getBannerCache();

  BannerScorer getBannerScorer();

  ModelManager getModelManager();

  /**
   * to be called once at the start of module's lifecycle.
   */
  void initialize();

  /**
   * to be called once the module's lifecycle has ended.
   */
  void cleanup();

}
