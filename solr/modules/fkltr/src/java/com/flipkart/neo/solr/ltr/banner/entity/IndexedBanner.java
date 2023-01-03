package com.flipkart.neo.solr.ltr.banner.entity;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.flipkart.ad.selector.model.search.CachedBanner;
import com.flipkart.ads.ranking.model.context.DemandContext;
import com.flipkart.ads.ranking.model.entities.FeaturesWeightSum;
import com.flipkart.ads.ranking.model.features.FeatureTypeId;
import com.flipkart.ads.ranking.model.features.entities.Feature;
import com.flipkart.mm.hazelcast.model.campaign.TeamType;
import com.flipkart.ad.selector.model.search.BannerSegmentInfo;

public class IndexedBanner implements CachedBanner {
  private final String bannerId;
  private final long version;

  private String creativeTemplateId;
  private String creativeId;
  private String bannerGroupId;
  private String vernacularCreativeGroupId;
  private TeamType teamType;

  private String locale;

  private Collection<String> themeNamePlusTemplateIdStrings;
  private Collection<String> themes;
  private Collection<String> servingTeamId;
  /**
   * Today, it is indexed with relevantStores(including parents), crossStores, similarStores.
   * But, for MERCH banners it effectively holds only relevantStores as cross and similar are not populated for MERCH.
   */
  private Collection<String> storeIds;
  private String servingStore;

  private double asp;
  private double priceValue;
  private Map<String, Double> placementPriceMap;
  private DemandContext demandContext;
  private final Map<String, Double> modelIdToL1Score = new ConcurrentHashMap<>(4,1);
  private final Map<String, FeaturesWeightSum> modelIdToDemandFeaturesSum = new ConcurrentHashMap<>(4,1);
  private final Map<String, Map<FeatureTypeId, Feature>> modelIdToGroupedDemandFeatures = new ConcurrentHashMap<>(4,1);

  private List<String> brands;
  private Map<String, LinkedHashMap<String, Double>> sortedRelevantStoreScores;
  private Map<String, LinkedHashMap<String, Double>> sortedSimilarStoreScores;
  private Map<String, LinkedHashMap<String, Double>> sortedCrossStoreScores;
  private Set<String> excludedStores;
  private BannerSegmentInfo bannerSegmentInfo;

  public IndexedBanner(String bannerId, long version) {
    this.bannerId = bannerId;
    this.version = version;
  }

  @Override
  public DemandContext getDemandContext() {
    return demandContext;
  }

  @Override
  public void putGroupedDemandFeatures(String modelId, Map<FeatureTypeId, Feature> groupedDemandFeatures) {
    modelIdToGroupedDemandFeatures.put(modelId, groupedDemandFeatures);
  }

  @Override
  public void removeGroupedDemandFeatures(String modelId) {
    modelIdToGroupedDemandFeatures.remove(modelId);
  }

  @Override
  public void putL1Score(String modelId, double score) {
    modelIdToL1Score.put(modelId, score);
  }

  @Override
  public void removeL1Score(String modelId) {
    modelIdToL1Score.remove(modelId);
  }

  @Override
  public void putDemandFeaturesSum(String modelId, FeaturesWeightSum featuresWeightSum) {
    modelIdToDemandFeaturesSum.put(modelId, featuresWeightSum);
  }

  @Override
  public void removeDemandFeaturesSum(String modelId) {
    modelIdToDemandFeaturesSum.remove(modelId);
  }

  public String getId() {
    return bannerId;
  }

  public long getVersion() {
    return version;
  }

  public String getCreativeTemplateId() {
    return creativeTemplateId;
  }

  public String getCreativeId() {
    return creativeId;
  }

  public String getBannerGroupId() {
    return bannerGroupId;
  }

  public String getVernacularCreativeGroupId() {
    return vernacularCreativeGroupId;
  }

  public String getLocale() {
    return locale;
  }

  public Collection<String> getThemeNamePlusTemplateIdStrings() {
    return themeNamePlusTemplateIdStrings;
  }

  public Collection<String> getThemes() {
    return themes;
  }

  public Collection<String> getStoreIds() {
    return storeIds;
  }

  public Double getL1Score(String modelId) {
    return modelIdToL1Score.get(modelId);
  }

  Map<FeatureTypeId, Feature> getGroupedDemandFeatures(String modelId) {
    return modelIdToGroupedDemandFeatures.get(modelId);
  }

  FeaturesWeightSum getDemandFeatureSum(String modelId) {
    return modelIdToDemandFeaturesSum.get(modelId);
  }

  public TeamType getTeamType() {
    return teamType;
  }

  public double getAsp() {
    return asp;
  }

  public double getPriceValue() {
    return priceValue;
  }

  public List<String> getBrands() {
    return brands;
  }

  public Map<String, LinkedHashMap<String, Double>> getSortedRelevantStoreScores() {
    return sortedRelevantStoreScores;
  }

  public Map<String, LinkedHashMap<String, Double>> getSortedSimilarStoreScores() {
    return sortedSimilarStoreScores;
  }

  public Map<String, LinkedHashMap<String, Double>> getSortedCrossStoreScores() {
    return sortedCrossStoreScores;
  }

  public Set<String> getExcludedStores() {
    return excludedStores;
  }

  public void setCreativeTemplateId(String creativeTemplateId) {
    this.creativeTemplateId = creativeTemplateId;
  }

  public void setCreativeId(String creativeId) {
    this.creativeId = creativeId;
  }

  public void setBannerGroupId(String bannerGroupId) {
    this.bannerGroupId = bannerGroupId;
  }

  public void setVernacularCreativeGroupId(String vernacularCreativeGroupId) {
    this.vernacularCreativeGroupId = vernacularCreativeGroupId;
  }

  public void setTeamType(TeamType teamType) {
    this.teamType = teamType;
  }

  public void setLocale(String locale) {
    this.locale = locale;
  }

  public void setThemeNamePlusTemplateIdStrings(Collection<String> themeNamePlusTemplateIdStrings) {
    this.themeNamePlusTemplateIdStrings = themeNamePlusTemplateIdStrings;
  }

  public void setThemes(Collection<String> themes) {
    this.themes = themes;
  }

  public void setStoreIds(Collection<String> storeIds) {
    this.storeIds = storeIds;
  }

  public void setDemandContext(DemandContext demandContext) {
    this.demandContext = demandContext;
  }

  public void setAsp(double asp) {
    this.asp = asp;
  }

  public void setPriceValue(double priceValue) {
    this.priceValue = priceValue;
  }

  public void setBrands(List<String> brands) {
    this.brands = brands;
  }

  public void setSortedRelevantStoreScores(Map<String, LinkedHashMap<String, Double>> sortedRelevantStoreScores) {
    this.sortedRelevantStoreScores = sortedRelevantStoreScores;
  }

  public void setSortedSimilarStoreScores(Map<String, LinkedHashMap<String, Double>> sortedSimilarStoreScores) {
    this.sortedSimilarStoreScores = sortedSimilarStoreScores;
  }

  public void setSortedCrossStoreScores(Map<String, LinkedHashMap<String, Double>> sortedCrossStoreScores) {
    this.sortedCrossStoreScores = sortedCrossStoreScores;
  }

  public void setExcludedStores(Set<String> excludedStores) {
    this.excludedStores = excludedStores;
  }

  public Map<String, Double> getPlacementPriceMap() {
    return placementPriceMap;
  }

  public void setPlacementPriceMap(Map<String, Double> placementPriceMap) {
    this.placementPriceMap = placementPriceMap;
  }

  public BannerSegmentInfo getBannerSegmentInfo() {
    return bannerSegmentInfo;
  }

  public void setBannerSegmentInfo(BannerSegmentInfo bannerSegmentInfo) {
    this.bannerSegmentInfo = bannerSegmentInfo;
  }

  public String getServingStore() {
    return servingStore;
  }

  public void setServingStore(String servingStore) {
    this.servingStore = servingStore;
  }

  public Collection<String> getServingTeamId() {
    return servingTeamId;
  }

  public void setServingTeamId(Collection<String> servingTeamId) {
    this.servingTeamId = servingTeamId;
  }

}