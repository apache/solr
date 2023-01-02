package com.flipkart.neo.solr.ltr.response.models;

import java.util.Map;

import com.flipkart.ad.selector.api.Score;
import com.flipkart.ad.selector.model.supply.PlacementInfo;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class PlacementScore {
  private PlacementInfo placementInfo;
  private Map<Score.Dimension, Score> scoreMap;
  public PlacementScore(PlacementInfo placementInfo, Map<Score.Dimension, Score> scoreMap) {
    this.placementInfo = placementInfo;
    this.scoreMap = scoreMap;
  }

  public PlacementScore() {}

  public PlacementInfo getPlacementInfo() {
    return placementInfo;
  }

  public void setPlacementInfo(PlacementInfo placementInfo) {
    this.placementInfo = placementInfo;
  }

  public Map<Score.Dimension, Score> getScoreMap() {
    return scoreMap;
  }

  public void setScoreMap(Map<Score.Dimension, Score> scoreMap) {
    this.scoreMap = scoreMap;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (!(o instanceof PlacementScore)) return false;

    PlacementScore that = (PlacementScore) o;

    return new EqualsBuilder()
        .append(getPlacementInfo(), that.getPlacementInfo())
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(getPlacementInfo())
        .toHashCode();
  }
}
