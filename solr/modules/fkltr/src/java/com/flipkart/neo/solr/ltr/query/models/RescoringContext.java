package com.flipkart.neo.solr.ltr.query.models;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.flipkart.ad.selector.model.supply.PlacementInfo;
import com.flipkart.neo.solr.ltr.query.models.context.IntentInfo;
import com.flipkart.neo.solr.ltr.query.models.context.SupplyInfo;
import com.flipkart.neo.solr.ltr.query.models.context.UserInfo;
import com.flipkart.neo.solr.ltr.query.models.context.VernacularInfo;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L1ScoringConfig;
import com.flipkart.neo.solr.ltr.query.models.scoring.config.L2ScoringConfig;

public class RescoringContext {
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

  private boolean segmentFilterRequired;

  private boolean isDebugInfoRequired;
  private Set<PlacementInfo> placementInfos;

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
