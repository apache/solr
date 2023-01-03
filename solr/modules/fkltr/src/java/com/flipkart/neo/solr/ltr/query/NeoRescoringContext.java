package com.flipkart.neo.solr.ltr.query;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.flipkart.ad.selector.model.RejectReasonCode;
import com.flipkart.ad.selector.model.supply.PlacementInfo;
import com.flipkart.neo.solr.ltr.query.models.ScoringScheme;
import com.flipkart.neo.solr.ltr.query.models.ScoringSchemeConfig;
import com.flipkart.neo.solr.ltr.query.models.context.IntentInfo;
import com.flipkart.neo.solr.ltr.query.models.context.SupplyInfo;
import com.flipkart.neo.solr.ltr.query.models.context.UserInfo;
import com.flipkart.neo.solr.ltr.query.models.context.VernacularInfo;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L1ScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L2ScoringConfig;
import com.flipkart.solr.ltr.query.BaseRescoringContext;

public class NeoRescoringContext implements BaseRescoringContext {
  // scoring scheme configs
  private ScoringSchemeConfig scoringSchemeConfig;

  // request context
  private String requestId;
  private long requestTimeStampInMillis;
  private String userId;

  private VernacularInfo vernacularInfo;

  private L1ScoringConfig  l1ScoringConfig;

  private L2ScoringConfig l2ScoringConfig;

  // Context required only if L2 scoring.
  private UserInfo userInfo;
  private SupplyInfo supplyInfo;
  private IntentInfo intentInfo;

  private Map<String, List<String>> userSegments;

  private boolean segmentFilterRequired = false;

  private boolean isDebugInfoRequired;

  private boolean isAggregateScoreRequired;

  private Map<String, Map<String, Map<String, String>>> featureToFeatureWeightMap;
  private Map<String, RejectReasonCode> filteredRejectedContents;
  private Set<PlacementInfo> placementInfos;

  @Override
  public Optional<String> getCustomGroupField() {
    if (scoringSchemeConfig.getScoringScheme() == ScoringScheme.L1_GROUP_L2) {
      return Optional.of(scoringSchemeConfig.getGroupField());
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Set<String> getGroupsRequired() {
    return scoringSchemeConfig.getGroupsRequiredWithLimits().keySet();
  }

  @Override
  public int getLimitPerGroup() {
    // Returning the limit for the first group.
    // For now, multi-shard grouping is only supported when groups have equal limits.
    return scoringSchemeConfig.getGroupsRequiredWithLimits().values().iterator().next();
  }

  public int getLimitForGroup(String groupId) {
    return scoringSchemeConfig.getGroupsRequiredWithLimits().get(groupId);
  }

  public Set<String> getPreferredSecondaryIdsForGroup(String groupId) {
    if (scoringSchemeConfig.getGroupsWithPreferredSecondaryIds() == null) return null;
    return scoringSchemeConfig.getGroupsWithPreferredSecondaryIds().get(groupId);
  }

  public ScoringSchemeConfig getScoringSchemeConfig() {
    return scoringSchemeConfig;
  }

  public void setScoringSchemeConfig(ScoringSchemeConfig scoringSchemeConfig) {
    this.scoringSchemeConfig = scoringSchemeConfig;
  }

  public String getRequestId() {
    return requestId;
  }

  public void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  public long getRequestTimeStampInMillis() {
    return requestTimeStampInMillis;
  }

  public void setRequestTimeStampInMillis(long requestTimeStampInMillis) {
    this.requestTimeStampInMillis = requestTimeStampInMillis;
  }

  public String getUserId() {
    return userId;
  }

  public void setUserId(String userId) {
    this.userId = userId;
  }

  public VernacularInfo getVernacularInfo() {
    return vernacularInfo;
  }

  public void setVernacularInfo(VernacularInfo vernacularInfo) {
    this.vernacularInfo = vernacularInfo;
  }

  public L1ScoringConfig getL1ScoringConfig() {
    return l1ScoringConfig;
  }

  public void setL1ScoringConfig(L1ScoringConfig l1ScoringConfig) {
    this.l1ScoringConfig = l1ScoringConfig;
  }

  public L2ScoringConfig getL2ScoringConfig() {
    return l2ScoringConfig;
  }

  public void setL2ScoringConfig(L2ScoringConfig l2ScoringConfig) {
    this.l2ScoringConfig = l2ScoringConfig;
  }

  public UserInfo getUserInfo() {
    return userInfo;
  }

  public void setUserInfo(UserInfo userInfo) {
    this.userInfo = userInfo;
  }

  public SupplyInfo getSupplyInfo() {
    return supplyInfo;
  }

  public void setSupplyInfo(SupplyInfo supplyInfo) {
    this.supplyInfo = supplyInfo;
  }

  public IntentInfo getIntentInfo() {
    return intentInfo;
  }

  public void setIntentInfo(IntentInfo intentInfo) {
    this.intentInfo = intentInfo;
  }

  public boolean isDebugInfoRequired() {
    return isDebugInfoRequired;
  }

  public void setDebugInfoRequired(boolean debugInfoRequired) {
    isDebugInfoRequired = debugInfoRequired;
  }

  public boolean isAggregateScoreRequired() {
    return isAggregateScoreRequired;
  }

  public void setAggregateScoreRequired(boolean aggregateScoreRequired) {
    isAggregateScoreRequired = aggregateScoreRequired;
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

  public Set<PlacementInfo> getPlacementInfos() {
    return placementInfos;
  }

  public void setPlacementInfos(Set<PlacementInfo> placementInfos) {
    this.placementInfos = placementInfos;
  }

  public Map<String, List<String>> getUserSegments() {
    return userSegments;
  }

  public void setUserSegments(Map<String, List<String>> userSegments) {
    this.userSegments = userSegments;
  }

  public boolean getSegmentFilterRequired() {
    return segmentFilterRequired;
  }

  public void setSegmentFilterRequired(boolean segmentFilterRequired) {
    this.segmentFilterRequired = segmentFilterRequired;
  }
}
