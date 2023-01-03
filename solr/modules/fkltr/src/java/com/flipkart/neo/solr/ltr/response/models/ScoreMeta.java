package com.flipkart.neo.solr.ltr.response.models;

import java.util.Map;
import java.util.Set;

import com.flipkart.ad.selector.api.Score;

public class ScoreMeta {
  // todo:fkltr check if common model for score can be used, will avoid transformation.
  private Map<Score.Dimension, Double> scoreSplit;
  // todo:fkltr check if common model for ModelInfo  can be used, will avoid transformation.
  private Map<Score.Dimension, ModelInfo> modelInfoMap;
  private Set<PlacementScore> placementScoresSet;

  private boolean isExplored;
  private String relevanceType;

  private DebugResponse debugResponse;

  public Map<Score.Dimension, Double> getScoreSplit() {
    return scoreSplit;
  }

  public void setScoreSplit(Map<Score.Dimension, Double> scoreSplit) {
    this.scoreSplit = scoreSplit;
  }

  public Map<Score.Dimension, ModelInfo> getModelInfoMap() {
    return modelInfoMap;
  }

  public void setModelInfoMap(Map<Score.Dimension, ModelInfo> modelInfoMap) {
    this.modelInfoMap = modelInfoMap;
  }

  public boolean isExplored() {
    return isExplored;
  }

  public void setExplored(boolean explored) {
    isExplored = explored;
  }

  public String getRelevanceType() {
    return relevanceType;
  }

  public void setRelevanceType(String relevanceType) {
    this.relevanceType = relevanceType;
  }

  public DebugResponse getDebugResponse() {
    return debugResponse;
  }

  public void setDebugResponse(DebugResponse debugResponse) {
    this.debugResponse = debugResponse;
  }

  public Set<PlacementScore> getPlacementScoresSet() { return placementScoresSet; }

  public void setPlacementScoresSet(Set<PlacementScore> placementScoresSet) { this.placementScoresSet = placementScoresSet; }
}
