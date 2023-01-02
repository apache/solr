package com.flipkart.neo.solr.ltr.query.models.context;

public class SupplyInfo {
  private String trafficSource;
  private String pageType;
  private DeviceInfo deviceInfo;

  public String getTrafficSource() {
    return trafficSource;
  }

  public void setTrafficSource(String trafficSource) {
    this.trafficSource = trafficSource;
  }

  public String getPageType() {
    return pageType;
  }

  public void setPageType(String pageType) {
    this.pageType = pageType;
  }

  public DeviceInfo getDeviceInfo() {
    return deviceInfo;
  }

  public void setDeviceInfo(DeviceInfo deviceInfo) {
    this.deviceInfo = deviceInfo;
  }
}