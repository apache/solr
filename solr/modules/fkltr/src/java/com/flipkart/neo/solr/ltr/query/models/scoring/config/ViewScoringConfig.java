package com.flipkart.neo.solr.ltr.query.models.scoring.config;

public class ViewScoringConfig {
  private Double fkWeight;
  private Double buWeight;
  private Double boostFactor;
  private Double viewPerImpression;

  public Double getFkWeight() {
    return fkWeight;
  }

  public void setFkWeight(Double fkWeight) {
    this.fkWeight = fkWeight;
  }

  public Double getBoostFactor() {
    return boostFactor;
  }

  public void setBoostFactor(Double boostFactor) {
    this.boostFactor = boostFactor;
  }

  public Double getViewPerImpression() {
    return viewPerImpression;
  }

  public void setViewPerImpression(Double viewPerImpression) {
    this.viewPerImpression = viewPerImpression;
  }

  public Double getBuWeight() {
    return buWeight;
  }

  public void setBuWeight(Double buWeight) {
    this.buWeight = buWeight;
  }
}