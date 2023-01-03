package com.flipkart.neo.solr.ltr.query.models.scoring.config;

import java.util.Map;

public class L1ScoringConfig {
  private String modelId;
  private int explorePercentage;
  private int timeBucketInMins;
  private Map<String, Double> creativeTemplateIdToL1ScoreMultiplier;

  public String getModelId() {
    return modelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public int getExplorePercentage() {
    return explorePercentage;
  }

  public void setExplorePercentage(int explorePercentage) {
    this.explorePercentage = explorePercentage;
  }

  public int getTimeBucketInMins() {
    return timeBucketInMins;
  }

  public void setTimeBucketInMins(int timeBucketInMins) {
    this.timeBucketInMins = timeBucketInMins;
  }

  public Map<String, Double> getCreativeTemplateIdToL1ScoreMultiplier() {
    return creativeTemplateIdToL1ScoreMultiplier;
  }

  public void setCreativeTemplateIdToL1ScoreMultiplier(Map<String, Double> creativeTemplateIdToL1ScoreMultiplier) {
    this.creativeTemplateIdToL1ScoreMultiplier = creativeTemplateIdToL1ScoreMultiplier;
  }
}
