package com.flipkart.neo.solr.ltr.response.models;

import java.util.Map;
import java.util.Set;

import com.flipkart.ad.selector.model.RejectReasonCode;

public class DebugResponse {
  private Set<String> matchedBanners;
  private Map<String, Map<String, Map<String, String>>> featureToFeatureWeightMap;
  private Map<String, RejectReasonCode> filteredRejectedContents;
  private Set<String> activeModelKeys;

  public Set<String> getMatchedBanners() {
    return matchedBanners;
  }

  public void setMatchedBanners(Set<String> matchedBanners) {
    this.matchedBanners = matchedBanners;
  }

  public Map<String, Map<String, Map<String, String>>> getFeatureToFeatureWeightMap() {
    return featureToFeatureWeightMap;
  }

  public void setFeatureToFeatureWeightMap(Map<String, Map<String, Map<String, String>>> featureToFeatureWeightMap) {
    this.featureToFeatureWeightMap = featureToFeatureWeightMap;
  }

  public Map<String, RejectReasonCode> getFilteredRejectedContents() {
    return filteredRejectedContents;
  }

  public void setFilteredRejectedContents(Map<String, RejectReasonCode> filteredRejectedContents) {
    this.filteredRejectedContents = filteredRejectedContents;
  }

  public Set<String> getActiveModelKeys() {
    return activeModelKeys;
  }

  public void setActiveModelKeys(Set<String> activeModelKeys) {
    this.activeModelKeys = activeModelKeys;
  }
}
