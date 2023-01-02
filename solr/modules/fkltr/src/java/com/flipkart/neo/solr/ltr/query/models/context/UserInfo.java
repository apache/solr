package com.flipkart.neo.solr.ltr.query.models.context;

import java.util.Map;

import com.flipkart.m3.varys.features.models.uad.activity.UserActivityData;
import com.flipkart.userservice.entity.response.user.insight.InsightsInfo;

public class UserInfo {
  private String accountId;
  private String deviceId;
  private InsightsInfo insightsInfo;
  private Map<String, Double> insightsThresholds;
  private UserActivityData userActivityData;

  public String getAccountId() {
    return accountId;
  }

  public void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
  }

  public InsightsInfo getInsightsInfo() {
    return insightsInfo;
  }

  public void setInsightsInfo(InsightsInfo insightsInfo) {
    this.insightsInfo = insightsInfo;
  }

  public Map<String, Double> getInsightsThresholds() {
    return insightsThresholds;
  }

  public void setInsightsThresholds(Map<String, Double> insightsThresholds) {
    this.insightsThresholds = insightsThresholds;
  }

  public UserActivityData getUserActivityData() {
    return userActivityData;
  }

  public void setUserActivityData(UserActivityData userActivityData) {
    this.userActivityData = userActivityData;
  }
}