package com.flipkart.neo.solr.ltr.query.models;

import java.util.Map;
import java.util.Set;

public class ScoringSchemeConfig {
  // scoring scheme info
  private ScoringScheme scoringScheme;

  /** required for L1_L2 scheme. */
  private int l2Limit;

  /** required for L1_GROUP_L2 scheme. */
  private String groupField;
  // todo:fkltr check if hashmap and hashset should be enforced while deserialization.
  private Map<String, Integer> groupsRequiredWithLimits;
  private Map<String, Set<String>> groupsWithPreferredSecondaryIds;

  public ScoringScheme getScoringScheme() {
    return scoringScheme;
  }

  public void setScoringScheme(ScoringScheme scoringScheme) {
    this.scoringScheme = scoringScheme;
  }

  public int getL2Limit() {
    return l2Limit;
  }

  public void setL2Limit(int l2Limit) {
    this.l2Limit = l2Limit;
  }

  public String getGroupField() {
    return groupField;
  }

  public void setGroupField(String groupField) {
    this.groupField = groupField;
  }

  public Map<String, Integer> getGroupsRequiredWithLimits() {
    return groupsRequiredWithLimits;
  }

  public void setGroupsRequiredWithLimits(Map<String, Integer> groupsRequiredWithLimits) {
    this.groupsRequiredWithLimits = groupsRequiredWithLimits;
  }

  public Map<String, Set<String>> getGroupsWithPreferredSecondaryIds() {
    return groupsWithPreferredSecondaryIds;
  }

  public void setGroupsWithPreferredSecondaryIds(Map<String, Set<String>> groupsWithPreferredSecondaryIds) {
    this.groupsWithPreferredSecondaryIds = groupsWithPreferredSecondaryIds;
  }
}
