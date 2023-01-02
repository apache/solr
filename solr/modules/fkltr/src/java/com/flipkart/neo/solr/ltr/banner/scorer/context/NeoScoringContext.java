package com.flipkart.neo.solr.ltr.banner.scorer.context;

import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.RejectReasonCode;
import com.flipkart.ad.selector.model.context.ScoringContext;
import com.flipkart.ad.selector.model.internals.InternalIntentInfo;
import com.flipkart.ad.selector.model.ranking.AggregateScoringConfig;
import com.flipkart.ad.selector.model.supply.PlacementInfo;
import com.flipkart.ads.ranking.model.context.SupplyContext;
import com.flipkart.ads.ranking.model.context.UserContext;
import com.flipkart.ads.ranking.model.entities.ModelKey;
import com.flipkart.ads.ranking.model.features.entities.Feature;
import com.flipkart.ads.ranking.model.features.entities.FeatureWeight;
import com.flipkart.neo.selector.ModelInfo;
import com.flipkart.neo.solr.ltr.banner.scorer.context.transformer.ContextTransformer;
import com.flipkart.neo.solr.ltr.query.NeoRescoringContext;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toMap;

public class NeoScoringContext implements ScoringContext {
  private final String requestId;
  private final String userId;
  private final DateTime requestTimeStamp;
  private final UserContext userContext;
  private final SupplyContext supplyContext;
  private final InternalIntentInfo intentInfo;

  private final AggregateScoringConfig aggregateScoringConfig;

  private final boolean isDebugInfoRequired;

  private final Map<Score.Dimension, ModelInfo> modelTypeToModelInfo = new ConcurrentHashMap<>();
  private final Set<PlacementInfo> placementInfos;
  private final Map<String, RejectReasonCode> filteredRejectedContents = new HashMap<>();
  private Map<String, Map<String, Map<String, String>>> featureToFeatureWeightMap = new HashMap<>();
  private Map<String, Double> rankingAttributesWeight = new HashMap<>();

  public NeoScoringContext(NeoRescoringContext rescoringContext) {
    this.requestId = rescoringContext.getRequestId();
    this.userId = rescoringContext.getUserId();
    this.requestTimeStamp = new DateTime(rescoringContext.getRequestTimeStampInMillis());

    this.userContext = ContextTransformer.buildUserContext(rescoringContext.getUserInfo());
    this.supplyContext = ContextTransformer.buildSupplyContext(rescoringContext);
    this.intentInfo = ContextTransformer.buildIntentInfo(rescoringContext.getIntentInfo());

    this.aggregateScoringConfig = ContextTransformer.buildAggregateScoringConfig(rescoringContext.getL2ScoringConfig(),
        rescoringContext.isAggregateScoreRequired());

    this.isDebugInfoRequired = rescoringContext.isDebugInfoRequired();
    this.placementInfos = rescoringContext.getPlacementInfos();
  }

  @Override
  public String getRequestId() {
    return requestId;
  }

  @Override
  public DateTime getRequestTimestamp() {
    return requestTimeStamp;
  }

  @Override
  public String getUserId() {
    return userId;
  }

  @Override
  public UserContext getRankingUserContext() {
    return userContext;
  }

  @Override
  public SupplyContext getRankingSupplyContext() {
    return supplyContext;
  }

  @Override
  public InternalIntentInfo getInternalIntentInfo() {
    return intentInfo;
  }

  @Override
  public AggregateScoringConfig getAggregateScoringConfig() {
    return aggregateScoringConfig;
  }

  @Override
  public Map<Score.Dimension, ModelInfo> getModelTypeToModelInfo() {
    return modelTypeToModelInfo;
  }

  @Override
  public boolean isDebugInfoRequired() {
    return isDebugInfoRequired;
  }

  @Override
  public void addRejectedBanner(String bannerId, RejectReasonCode reasonCode) {
    filteredRejectedContents.putIfAbsent(bannerId, reasonCode);
  }

  @Override
  public void updateFeatureWeights(String bannerId, Map<Feature, FeatureWeight> featureToWeightMap,
                                   ModelKey modelKey) {
    Map<String, Map<String, String>> scoringMap = featureToFeatureWeightMap.getOrDefault(bannerId, new HashMap<>());
    scoringMap.put(modelKey.getKey(),
        featureToWeightMap.entrySet().stream().collect(toMap(k -> k.getKey().toString(),
            k -> ("weight=" + k.getValue().getPrimaryWeight() + " secWeight=" + k.getValue().getSecondaryWeight()))));
    featureToFeatureWeightMap.put(bannerId, scoringMap);
  }

  @Override
  public Map<String, RejectReasonCode> getFilteredRejectedContents() {
    return filteredRejectedContents;
  }

  @Override
  public Map<String, Map<String, Map<String, String>>> getFeatureToFeatureWeightMap() {
    return featureToFeatureWeightMap;
  }

  @Override
  public void updateRankingWeights(Score.Dimension dimension, Double weight) {
    rankingAttributesWeight.put(dimension.name(), weight);
  }

  @Override
  public Map<String, Double> getRankingAttributesWeight() {
    return rankingAttributesWeight;
  }

  @Override
  public Set<PlacementInfo> getAvailablePlacement() {
    return placementInfos;
  }
}
