package com.flipkart.neo.solr.ltr.query.models.scoring.config;

public class ModelScoringConfig extends CommonScoringConfig {
  private String modelId;
  private Double explore;
  private Double hyperParameterAlpha;
  private Integer timeBucketInMins;

  public String getModelId() {
    return modelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public Double getExplore() {
    return explore;
  }

  public void setExplore(Double explore) {
    this.explore = explore;
  }

  public Double getHyperParameterAlpha() {
    return hyperParameterAlpha;
  }

  public void setHyperParameterAlpha(Double hyperParameterAlpha) {
    this.hyperParameterAlpha = hyperParameterAlpha;
  }

  public Integer getTimeBucketInMins() {
    return timeBucketInMins;
  }

  public void setTimeBucketInMins(Integer timeBucketInMins) {
    this.timeBucketInMins = timeBucketInMins;
  }
}
