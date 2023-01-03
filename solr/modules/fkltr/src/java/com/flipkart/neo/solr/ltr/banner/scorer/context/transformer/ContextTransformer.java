package com.flipkart.neo.solr.ltr.banner.scorer.context.transformer;

import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.allocation.AggregateScorerType;
import com.flipkart.ad.selector.model.internals.InternalIntentInfo;
import com.flipkart.ad.selector.model.ranking.AggregateScoringConfig;
import com.flipkart.ad.selector.model.ranking.ContextualContentScoringConfig;
import com.flipkart.ad.selector.model.ranking.DefaultScoringConfig;
import com.flipkart.ad.selector.model.ranking.DiversityScoringConfig;
import com.flipkart.ad.selector.model.ranking.ModelScoringConfig;
import com.flipkart.ad.selector.model.ranking.ScoringConfig;
import com.flipkart.ad.selector.model.ranking.ViewScoringConfig;
import com.flipkart.ad.selector.score.ModelExecutionContextUtils;
import com.flipkart.ads.ranking.model.context.SupplyContext;
import com.flipkart.ads.ranking.model.context.UserContext;
import com.flipkart.ads.ranking.model.context.entities.PageContext;
import com.flipkart.ads.ranking.model.context.entities.OSInfo;
import com.flipkart.ads.ranking.model.context.entities.BrowserInfo;
import com.flipkart.ads.ranking.model.context.entities.DeviceInfo;
import com.flipkart.ads.ranking.utils.NeoContextTransformer;
import com.flipkart.m3.varys.features.util.UADUtils;
import com.flipkart.neo.selector.context.UserInsights;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import com.flipkart.neo.solr.ltr.query.models.context.IntentInfo;
import com.flipkart.neo.solr.ltr.query.models.context.SupplyInfo;
import com.flipkart.neo.solr.ltr.query.models.context.UserInfo;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.CommonScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L2ScoringConfig;
import org.apache.commons.collections4.CollectionUtils;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class ContextTransformer {

  private static final String UIE_VERSION = "VERSION_2";

  private ContextTransformer(){}

  public static UserContext buildUserContext(UserInfo userInfo) {
    //TPC is not used as feature hence passing null marketplace.
    UserInsights neoUserInsights =
        ModelExecutionContextUtils.transformUserInsights(userInfo.getInsightsInfo(), userInfo.getInsightsThresholds(),
            UIE_VERSION, null);
    com.flipkart.neo.selector.context.UserContext userContext = com.flipkart.neo.selector.context.UserContext.newBuilder()
        .setAccountId(userInfo.getAccountId())
        .setDeviceId(userInfo.getDeviceId())
        .setUserActivityFeatures(UADUtils.pojoToAvro(userInfo.getUserActivityData()))
        .setUserInsights(neoUserInsights)
        .build();
    return NeoContextTransformer.userContext(userContext);
  }

  public static SupplyContext buildSupplyContext(NeoRescoringContext rescoringContext) {
    PageContext pageContext = buildPageContext(rescoringContext.getSupplyInfo());

    SupplyContext.SupplyContextBuilder supplyContextBuilder = SupplyContext.builder()
        .trafficSource(rescoringContext.getSupplyInfo().getTrafficSource())
        .pageContext(pageContext);

    setTimeInformationInSupplyContext(supplyContextBuilder, rescoringContext.getRequestTimeStampInMillis());

    return supplyContextBuilder.build();
  }

  public static AggregateScoringConfig buildAggregateScoringConfig(L2ScoringConfig l2ScoringConfig,
                                                                   boolean isAggregateScoreRequired) {
    Map<Score.Dimension, ScoringConfig> scoringConfigs = new HashMap<>(6,1);

    for (Score.Dimension dimension: l2ScoringConfig.getScorers()) {
      switch (dimension) {
        case PCTR:
          scoringConfigs.put(dimension,
              transformModelScoringConfig(l2ScoringConfig.getPctrScoringConfig(), dimension));
          break;
        case PCVR:
          scoringConfigs.put(dimension,
              transformModelScoringConfig(l2ScoringConfig.getPcvrScoringConfig(), dimension));
          break;
        case ASP:
          scoringConfigs.put(dimension,
              transformCommonScoringConfig(l2ScoringConfig.getAspScoringConfig(), dimension));
          break;
        case PRICE:
          scoringConfigs.put(dimension,
              transformCommonScoringConfig(l2ScoringConfig.getPriceScoringConfig(), dimension));
          break;
        case VIEW:
          scoringConfigs.put(dimension,
              transformViewScoringConfig(l2ScoringConfig.getViewScoringConfig(), dimension));
          break;
        case DIVERSITY:
          scoringConfigs.put(dimension,
              transformDiversityScoringConfig(l2ScoringConfig.getDiversityScoringConfig(),dimension));
          break;
        default:
          //currently, possible for dimension like RELEVANCE, (scoring is done in selector)
      }
    }

    ContextualContentScoringConfig contextualContentScoringConfig = null;
    if (l2ScoringConfig.getContextualContentScoringConfig() != null) {
      contextualContentScoringConfig = new ContextualContentScoringConfig(
          l2ScoringConfig.getContextualContentScoringConfig().isExcludeStoresHardFilterEnabled(),
          l2ScoringConfig.getContextualContentScoringConfig().getRelevanceScoringThreshold(),
          l2ScoringConfig.getContextualContentScoringConfig().getIntentStoreLimit(),
          l2ScoringConfig.getContextualContentScoringConfig().getDemandStoreLimit(),
          l2ScoringConfig.getContextualContentScoringConfig().getBrandMatchSet(),
          l2ScoringConfig.getContextualContentScoringConfig().getRelevantStoresScoringStrategy(),
          l2ScoringConfig.getContextualContentScoringConfig().getSimilarStoresScoringStrategy(),
          l2ScoringConfig.getContextualContentScoringConfig().getCrossStoresScoringStrategy(),
          l2ScoringConfig.getContextualContentScoringConfig().getScoringWeights());
    }

    boolean isCTRCVRMultiplierEnabled = l2ScoringConfig.getAggregateScorerType() == AggregateScorerType.CTRCVR;

    return new AggregateScoringConfig(scoringConfigs, isCTRCVRMultiplierEnabled, contextualContentScoringConfig,
        isAggregateScoreRequired);
  }

  private static DefaultScoringConfig transformCommonScoringConfig(
      CommonScoringConfig scoringConfig,
      Score.Dimension dimension) {
    return new DefaultScoringConfig(dimension, scoringConfig.getMaxValue(), scoringConfig.getFkWeight(),
        scoringConfig.getBuWeight());
  }

  private static ModelScoringConfig transformModelScoringConfig(
      com.flipkart.neo.solr.ltr.query.models.scoring.config.ModelScoringConfig scoringConfig,
      Score.Dimension dimension) {
    return new ModelScoringConfig(scoringConfig.getModelId(), dimension, scoringConfig.getMaxValue(),
        scoringConfig.getFkWeight(), scoringConfig.getExplore(), scoringConfig.getHyperParameterAlpha(),
        scoringConfig.getTimeBucketInMins(), scoringConfig.getBuWeight());
  }

  private static ViewScoringConfig transformViewScoringConfig(
      com.flipkart.neo.solr.ltr.query.models.scoring.config.ViewScoringConfig scoringConfig,
      Score.Dimension dimension) {
    return new ViewScoringConfig(dimension, scoringConfig.getFkWeight(), scoringConfig.getBuWeight(), scoringConfig.getBoostFactor(),
        scoringConfig.getViewPerImpression());
  }

  private static DiversityScoringConfig transformDiversityScoringConfig(
      com.flipkart.neo.solr.ltr.query.models.scoring.config.DiversityScoringConfig scoringConfig,
      Score.Dimension dimension) {
    return new DiversityScoringConfig(dimension, scoringConfig.getMaxValue(), scoringConfig.getFkWeight(), scoringConfig.getBuWeight(),
        scoringConfig.getStoreLevelToPenalty(), scoringConfig.getUadSignalToDiversityScore());
  }

  private static PageContext buildPageContext(SupplyInfo supplyInfo) {
    PageContext.PageContextBuilder pageContextBuilder = PageContext.builder().pageType(supplyInfo.getPageType());

    if (supplyInfo.getDeviceInfo() != null) {
      OSInfo osInfo = OSInfo.builder()
          .family(supplyInfo.getDeviceInfo().getOs())
          .version(supplyInfo.getDeviceInfo().getOsVersion())
          .build();
      BrowserInfo browserInfo = BrowserInfo.builder()
          .family(supplyInfo.getDeviceInfo().getBrowser())
          .version(null)
          .build();
      DeviceInfo deviceInfo = DeviceInfo.builder()
          .family(supplyInfo.getDeviceInfo().getMake())
          .version(CollectionUtils.isNotEmpty(supplyInfo.getDeviceInfo().getImeiModels()) ?
              supplyInfo.getDeviceInfo().getImeiModels().toString() : supplyInfo.getDeviceInfo().getModel())
          .build();

      pageContextBuilder.osInfo(osInfo).browserInfo(browserInfo).deviceInfo(deviceInfo);
    }

    return pageContextBuilder.build();
  }

  private static void setTimeInformationInSupplyContext(SupplyContext.SupplyContextBuilder supplyContextBuilder,
                                                        long requestTimeStampInMillis) {
    // presence-check. 0 is a valid check here.
    if (requestTimeStampInMillis != 0) {
      DateTime requestTime = new DateTime(requestTimeStampInMillis);
      supplyContextBuilder.dayOfTheWeek(requestTime.dayOfWeek().get());
      supplyContextBuilder.hour(requestTime.getHourOfDay());
      supplyContextBuilder.weekOfMonth(requestTime.dayOfMonth().get() / 7 + 1);
      supplyContextBuilder.requestTimeInMillis(requestTime.getMillis());
    }
  }

  public static InternalIntentInfo buildIntentInfo(IntentInfo intentInfo) {
    if (intentInfo == null) return null;
    return InternalIntentInfo.builder()
        .searchQuery(intentInfo.getSearchQuery())
        .searchQueryType(intentInfo.getSearchQueryType())
        .topIntentStores(intentInfo.getTopIntentStores())
        .intentParentStores(intentInfo.getIntentParentStores())
        .relevantStores(intentInfo.getRelevantStores())
        //InternalIntentInfo's POJO updated
        //.relevantFacets(intentInfo.getRelevantFacets())
        .build();
  }
}