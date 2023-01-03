package com.flipkart.neo.solr.ltr.query.models.scoring.config;

import java.util.Map;

public class DiversityScoringConfig {
  private Double maxValue;
  private Double fkWeight;
  private Double buWeight;
  private Map<Integer,Double> storeLevelToPenalty;
  private Map<Integer,Double> uadSignalToDiversityScore;

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

  public Map<Integer, Double> getStoreLevelToPenalty() {
    return storeLevelToPenalty;
  }

  public void setStoreLevelToPenalty(Map<Integer, Double> storeLevelToPenalty) {
    this.storeLevelToPenalty = storeLevelToPenalty;
  }

  public Map<Integer, Double> getUadSignalToDiversityScore() {
    return uadSignalToDiversityScore;
  }

  public void setUadSignalToDiversityScore(Map<Integer, Double> uadSignalToDiversityScore) {
    this.uadSignalToDiversityScore = uadSignalToDiversityScore;
  }
}
