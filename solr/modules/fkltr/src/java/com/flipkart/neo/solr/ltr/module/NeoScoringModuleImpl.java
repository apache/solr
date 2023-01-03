package com.flipkart.neo.solr.ltr.module;

import java.lang.invoke.MethodHandles;
import java.util.EnumMap;
import java.util.Map;

import com.codahale.metrics.MetricRegistry;
import com.flipkart.ad.selector.api.Score;
import com.flipkart.ads.contentranking.scorer.impl.FacetScorer;
import com.flipkart.neo.content.ranking.model.ActiveModelsRetriever;
import com.flipkart.neo.content.ranking.model.CachedBannerModelUpdateListener;
import com.flipkart.neo.content.ranking.model.ModelManager;
import com.flipkart.neo.content.ranking.model.ModelUpdateListener;
import com.flipkart.ad.selector.model.allocation.AggregateScorerType;
import com.flipkart.ads.contentranking.aggregatescorer.IAggregateScorer;
import com.flipkart.ads.contentranking.aggregatescorer.impl.DefaultCTRCVRMultiplierCalculator;
import com.flipkart.ads.contentranking.aggregatescorer.impl.DefaultScoreCalculator;
import com.flipkart.ads.contentranking.filter.RelevanceScoreFilter;
import com.flipkart.ads.contentranking.scorer.AggregateScorerFactory;
import com.flipkart.ads.contentranking.scorer.IScorer;
import com.flipkart.ads.contentranking.scorer.ModelScorerFactory;
import com.flipkart.ads.contentranking.scorer.RankingScorerFactory;
import com.flipkart.ads.contentranking.scorer.impl.ASPScorer;
import com.flipkart.ads.contentranking.scorer.impl.PriceScorer;
import com.flipkart.ads.contentranking.scorer.impl.RelevanceScorer;
import com.flipkart.ads.model.client.ModelClient;
import com.flipkart.ads.model.client.ReadWriteClient;
import com.flipkart.ads.model.entities.ModelMysqlConfig;
import com.flipkart.neo.content.ranking.scorers.L1Scorer;
import com.flipkart.neo.content.ranking.scorers.L2Scorer;
import com.flipkart.neo.content.ranking.scorers.impl.DiversityScorer;
import com.flipkart.neo.content.ranking.scorers.impl.L1ScorerImpl;
import com.flipkart.neo.content.ranking.scorers.impl.L2ScorerImpl;
import com.flipkart.neo.solr.ltr.banner.cache.BannerCache;
import com.flipkart.neo.solr.ltr.banner.cache.InMemoryBannerCache;
import com.flipkart.neo.solr.ltr.banner.scorer.BannerScorer;
import com.flipkart.neo.solr.ltr.banner.scorer.RankingLibBasedBannerScorer;
import com.flipkart.neo.solr.ltr.configs.NeoFKLTRConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// todo:fkltr evaluate immediate cleanups post collection reload.
public class NeoScoringModuleImpl implements NeoScoringModule {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final BannerScorer bannerScorer;
  private final BannerCache bannerCache;
  private final ModelManager modelManager;
  private final long modelRefreshPeriodInMillis;
  private final MetricRegistry metricRegistry;

  public NeoScoringModuleImpl(ModelMysqlConfig modelMysqlConfig, ActiveModelsRetriever activeModelsRetriever,
                              int modelRefreshPeriodInSeconds, int bannerCacheInitialSize,
                              NeoFKLTRConfigs neoFKLTRConfigs,
                              MetricRegistry metricRegistry, boolean isStaticModelsEnabled) {
    log.info("INITIALIZING NeoScoringModule");
    log.warn("INITIALIZING NeoScoringModule");

    // initialize bannerCache.
    this.bannerCache = new InMemoryBannerCache(bannerCacheInitialSize);
    log.warn("creating banner cache {}", bannerCache);
    // initialize modelScorerFactory.
    ModelUpdateListener modelUpdateListener = new CachedBannerModelUpdateListener(bannerCache);
    log.warn("creating modelUpdateListener {}", modelUpdateListener);

    this.metricRegistry = metricRegistry;
    this.modelRefreshPeriodInMillis = (long) modelRefreshPeriodInSeconds * 1000;
    // initialize modelManager.
    try {
      Class.forName("com.mysql.cj.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    this.modelManager = getStaticOrDynamicModelManager(modelMysqlConfig, modelUpdateListener, activeModelsRetriever, isStaticModelsEnabled);
    // initialize bannerScorer.
    ModelScorerFactory modelScorerFactory = new ModelScorerFactory(this.modelManager, metricRegistry);
    RelevanceScorer relevanceScorer = new RelevanceScorer(new FacetScorer());
    RelevanceScoreFilter relevanceScoreFilter = new RelevanceScoreFilter(metricRegistry,null);
    RankingScorerFactory rankingScorerFactory = getRankingScorerFactory();
    AggregateScorerFactory aggregateScorerFactory = getAggregateScorerFactory();
    L1Scorer l1Scorer = new L1ScorerImpl(this.modelManager);
    L2Scorer l2Scorer = new L2ScorerImpl(relevanceScorer, modelScorerFactory, rankingScorerFactory,
        aggregateScorerFactory, relevanceScoreFilter);
    this.bannerScorer = new RankingLibBasedBannerScorer(l1Scorer, l2Scorer, neoFKLTRConfigs);
  }

  @Override
  public BannerScorer getBannerScorer() {
    return bannerScorer;
  }

  @Override
  public ModelManager getModelManager() {
    return this.modelManager;
  }

  @Override
  public BannerCache getBannerCache() {
    return bannerCache;
  }

  @Override
  public void initialize() {
    // making a waiting-call for modelRefresh.
    modelManager.refreshModels();
    // starting cron with delay.
    modelManager.startModelRefreshCron(modelRefreshPeriodInMillis);
  }

  @Override
  public void cleanup() {
    modelManager.stopModelRefreshCron();
  }

  private RankingScorerFactory getRankingScorerFactory() {
    Map<Score.Dimension, IScorer> rankingScorerMap = new EnumMap<>(Score.Dimension.class);
    rankingScorerMap.put(Score.Dimension.PRICE, new PriceScorer(Score.Dimension.PRICE));
    rankingScorerMap.put(Score.Dimension.ASP, new ASPScorer(Score.Dimension.ASP));
    rankingScorerMap.put(Score.Dimension.DIVERSITY, new DiversityScorer(Score.Dimension.DIVERSITY));
    return new RankingScorerFactory(rankingScorerMap);
  }

  private AggregateScorerFactory getAggregateScorerFactory() {
    Map<AggregateScorerType, IAggregateScorer> aggregateScorerMap = new EnumMap<>(AggregateScorerType.class);
    aggregateScorerMap.put(AggregateScorerType.DEFAULT, new DefaultScoreCalculator());
    aggregateScorerMap.put(AggregateScorerType.CTRCVR, new DefaultCTRCVRMultiplierCalculator());
    return new AggregateScorerFactory(aggregateScorerMap);
  }

  private ModelManager getStaticOrDynamicModelManager(ModelMysqlConfig modelMysqlConfig, ModelUpdateListener modelUpdateListener,
                                                      ActiveModelsRetriever activeModelsRetriever, boolean isStaticModelsEnabled) {
    //TODO gracefully handle isStaticModelsEnabled = true
    log.warn("getMysqlModelClientHost {}", modelMysqlConfig.getMysqlModelClientHost());
    log.warn("getMysqlModelClientPort {}", modelMysqlConfig.getMysqlModelClientPort());
    log.warn("getMysqlModelClientDbName {}", modelMysqlConfig.getMysqlModelClientDbName());
    log.warn("getMysqlModelClientPassword {}", modelMysqlConfig.getMysqlModelClientPassword());
    log.warn("getMysqlModelClientUser {}", modelMysqlConfig.getMysqlModelClientUser());

    if(isStaticModelsEnabled){
      throw new RuntimeException("StaticModel based management has been deprecated");
    }
    ModelClient modelClient = new ModelClient(new ReadWriteClient(), modelMysqlConfig);
    return new ModelManager(modelClient, this.modelRefreshPeriodInMillis, modelUpdateListener,
        activeModelsRetriever, metricRegistry);
  }

}