package com.flipkart.neo.solr.ltr.query.models.scoring.config;

import java.util.Map;
import java.util.Set;

public class ContextualContentScoringConfig {
  private boolean isExcludeStoresHardFilterEnabled;
  private int intentStoreLimit;
  private int demandStoreLimit;
  private Double relevanceScoringThreshold;
  private String relevantStoresScoringStrategy;
  private String similarStoresScoringStrategy;
  private String crossStoresScoringStrategy;
  private Set<String> brandMatchSet;
  private Map<String, Double> scoringWeights;

  public boolean isExcludeStoresHardFilterEnabled() {
    return isExcludeStoresHardFilterEnabled;
  }

  public void setExcludeStoresHardFilterEnabled(boolean excludeStoresHardFilterEnabled) {
    isExcludeStoresHardFilterEnabled = excludeStoresHardFilterEnabled;
  }

  public int getIntentStoreLimit() {
    return intentStoreLimit;
  }

  public void setIntentStoreLimit(int intentStoreLimit) {
    this.intentStoreLimit = intentStoreLimit;
  }

  public int getDemandStoreLimit() {
    return demandStoreLimit;
  }

  public void setDemandStoreLimit(int demandStoreLimit) {
    this.demandStoreLimit = demandStoreLimit;
  }

  public Double getRelevanceScoringThreshold() {
    return relevanceScoringThreshold;
  }

  public void setRelevanceScoringThreshold(Double relevanceScoringThreshold) {
    this.relevanceScoringThreshold = relevanceScoringThreshold;
  }

  public String getRelevantStoresScoringStrategy() {
    return relevantStoresScoringStrategy;
  }

  public void setRelevantStoresScoringStrategy(String relevantStoresScoringStrategy) {
    this.relevantStoresScoringStrategy = relevantStoresScoringStrategy;
  }

  public String getSimilarStoresScoringStrategy() {
    return similarStoresScoringStrategy;
  }

  public void setSimilarStoresScoringStrategy(String similarStoresScoringStrategy) {
    this.similarStoresScoringStrategy = similarStoresScoringStrategy;
  }

  public String getCrossStoresScoringStrategy() {
    return crossStoresScoringStrategy;
  }

  public void setCrossStoresScoringStrategy(String crossStoresScoringStrategy) {
    this.crossStoresScoringStrategy = crossStoresScoringStrategy;
  }

  public Set<String> getBrandMatchSet() {
    return brandMatchSet;
  }

  public void setBrandMatchSet(Set<String> brandMatchSet) {
    this.brandMatchSet = brandMatchSet;
  }

  public Map<String, Double> getScoringWeights() {
    return scoringWeights;
  }

  public void setScoringWeights(Map<String, Double> scoringWeights) {
    this.scoringWeights = scoringWeights;
  }
}
