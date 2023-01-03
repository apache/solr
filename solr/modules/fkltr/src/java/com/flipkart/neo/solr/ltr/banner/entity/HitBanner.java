package com.flipkart.neo.solr.ltr.banner.entity;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.search.RequestBanner;
import com.flipkart.ad.selector.model.supply.PlacementInfo;
import com.flipkart.ads.ranking.model.context.DemandContext;
import com.flipkart.ads.ranking.model.entities.FeaturesWeightSum;
import com.flipkart.ads.ranking.model.features.FeatureTypeId;
import com.flipkart.ads.ranking.model.features.entities.Feature;
import com.flipkart.mm.hazelcast.model.bannergroup.BannerGroupPriceType;
import com.flipkart.neo.selector.ModelInfo;
import com.flipkart.neo.solr.ltr.response.models.ScoreMeta;
import com.flipkart.neo.solr.ltr.response.models.ScoreMetaBuilder;
import com.flipkart.solr.ltr.query.score.meta.ScoreMetaHolder;
import org.apache.commons.lang3.Range;
import org.apache.lucene.search.ScoreDoc;
import com.flipkart.mm.hazelcast.model.campaign.TeamType;

public class HitBanner implements RequestBanner {
  private final IndexedBanner indexedBanner;
  private final ScoreDoc hit;
  private final Map<Score.Dimension, Score> scores = new EnumMap<>(Score.Dimension.class);
  private Map<PlacementInfo, Map<Score.Dimension, Score>> placementScoreMap;
  private String relevanceType;
  private boolean isExplored;
  private Map<Score.Dimension, ModelInfo> modelTypeToModelInfo;

  public HitBanner(IndexedBanner indexedBanner, ScoreDoc hit) {
    this.indexedBanner = indexedBanner;
    this.hit = hit;
  }

  @Override
  public String getId() {
    return indexedBanner.getId();
  }

  //Not getting used
  @Override
  public boolean isEligibleForL2Ranking() {
    return true;
  }

  @Override
  public String getCreativeId() {
    return indexedBanner.getCreativeId();
  }

  @Override
  public double getAsp() {
    return indexedBanner.getAsp();
  }

  @Override
  public String getBannerGroupId() {
    return indexedBanner.getBannerGroupId();
  }

  @Override
  public Map<FeatureTypeId, Feature> getGroupedDemandFeatures(String modelId) {
    return indexedBanner.getGroupedDemandFeatures(modelId);
  }

  @Override
  public FeaturesWeightSum getDemandFeatureSum(String modelId) {
    return indexedBanner.getDemandFeatureSum(modelId);
  }

  @Override
  public DemandContext getRankingDemandContext() {
    return indexedBanner.getDemandContext();
  }

  @Override
  public Map<Score.Dimension, Score> getScore() {
    return scores;
  }

  @Override
  public Score getScore(Score.Dimension dimension) {
    return scores.get(dimension);
  }

  // TODO: This implementation will be used when solr does the AggregateScore calculation. Rectify the implemention then.
  @Override
  public TeamType getTeamType() {
    return indexedBanner.getTeamType();
  }

  @Override
  public BannerGroupPriceType getBannerGroupPriceType() { return null; }

  @Override
  public void setScore(Score.Dimension dimension, Score score) {
    scores.put(dimension, score);
  }

  @Override
  public double getPriceValue() {
    return indexedBanner.getPriceValue();
  }

  @Override
  public List<String> getBrands() {
    return indexedBanner.getBrands();
  }

  @Override
  public Map<String, LinkedHashMap<String, Double>> getSortedRelevantStoreScores() {
    return indexedBanner.getSortedRelevantStoreScores();
  }

  @Override
  public Map<String, LinkedHashMap<String, Double>> getSortedSimilarStoreScores() {
    return indexedBanner.getSortedSimilarStoreScores();
  }

  @Override
  public Map<String, LinkedHashMap<String, Double>> getSortedCrossStoreScores() {
    return indexedBanner.getSortedCrossStoreScores();
  }

  @Override
  public Set<String> getContextualExcludedStores() {
    return indexedBanner.getExcludedStores();
  }

  @Override
  public void setRelevanceType(String relevanceType) {
    this.relevanceType = relevanceType;
  }

  @Override
  public boolean isExplore() {
    return isExplored;
  }

  @Override
  public void setExplore(boolean isExplored) {
    this.isExplored = isExplored;
  }

  public String getRelevanceType() {
    return relevanceType;
  }

  public Map<Score.Dimension, ModelInfo> getModelTypeToModelInfo() {
    return modelTypeToModelInfo;
  }

  public ScoreDoc getHit() {
    return hit;
  }

  @Override
  public Map<PlacementInfo, Map<Score.Dimension, Score>> getPlacementScoreMap() {
    return placementScoreMap;
  }

  @Override
  public Map<Score.Dimension, Score> getScoreForPlacement(PlacementInfo placementInfo) {
    return placementScoreMap != null ? placementScoreMap.get(placementInfo) : null;
  }

  @Override
  public void updatePlacementScore(PlacementInfo placementInfo, Score.Dimension dimension, Score score) {
    if(placementScoreMap == null)
      placementScoreMap = new HashMap<>(1,1);

    if(placementScoreMap.get(placementInfo) != null) {
      placementScoreMap.get(placementInfo).put(dimension, score);
    } else {
      Map<Score.Dimension, Score> scoreMap = new HashMap<>(1);
      scoreMap.put(dimension, score);
      placementScoreMap.put(placementInfo, scoreMap);
    }
  }

  @Override
  public Double getPriceForPlacement(PlacementInfo placementInfo) {
    return indexedBanner.getPlacementPriceMap() != null ? indexedBanner.getPlacementPriceMap().get(placementInfo.getPlacementType().name()) : null;
  }

  @Override
  public String getServingStore() {
    return this.indexedBanner.getServingStore();
  }

  @Override
  public Range<Integer> getPriceRange() {
    //No specific usecase. Had to override because of design.
    return null;
  }

  @Override
  public Range<Integer> getDiscountRange() {
    //No specific usecase. Had to override because of design.
    return null;
  }

  @Override
  public int getRelevancePriority() {
    //No specific usecase. Had to override because of design.
    return 0;
  }

  @Override
  public void setRelevancePriority(int i) {
    //No specific usecase. Had to override because of design.
  }

  @Override
  public String getServingTeamId() {
    return null;
  }

  public void setModelTypeToModelInfo(Map<Score.Dimension, ModelInfo> modelTypeToModelInfo) {
    this.modelTypeToModelInfo = modelTypeToModelInfo;
  }

  public void addScoreMeta(ScoreMetaHolder<ScoreMeta> scoreMetaHolder) {
    scoreMetaHolder.putScoreMeta(hit.doc, ScoreMetaBuilder.buildScoreMeta(this));
  }

  public void updateHitScoreWithAggregate() {
    hit.score = scores.get(Score.Dimension.AGGREGATE).getValue().floatValue();
  }

}