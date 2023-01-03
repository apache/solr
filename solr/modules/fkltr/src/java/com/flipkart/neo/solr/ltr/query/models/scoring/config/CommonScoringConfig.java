package com.flipkart.neo.solr.ltr.query.models.scoring.config;

public class CommonScoringConfig {
  private Double maxValue;
  private Double fkWeight;
  private Double buWeight;

  public Double getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(Double maxValue) {
    this.maxValue = maxValue;
  }

  public Double getFkWeight() {
    return fkWeight;
  }

  public void setFkWeight(Double fkWeight) {
    this.fkWeight = fkWeight;
  }

  public Double getBuWeight() {
    return buWeight;
  }

  public void setBuWeight(Double buWeight) {
    this.buWeight = buWeight;
  }
}
