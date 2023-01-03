package com.flipkart.neo.solr.ltr.query.models.context;

import java.util.List;

public class DeviceInfo {
  private String make;
  private String model;
  private List<String> imeiModels;

  private String os;
  private String osVersion;

  private String browser;

  public String getMake() {
    return make;
  }

  public void setMake(String make) {
    this.make = make;
  }

  public String getModel() {
    return model;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public List<String> getImeiModels() {
    return imeiModels;
  }

  public void setImeiModels(List<String> imeiModels) {
    this.imeiModels = imeiModels;
  }

  public String getOs() {
    return os;
  }

  public void setOs(String os) {
    this.os = os;
  }

  public String getOsVersion() {
    return osVersion;
  }

  public void setOsVersion(String osVersion) {
    this.osVersion = osVersion;
  }

  public String getBrowser() {
    return browser;
  }

  public void setBrowser(String browser) {
    this.browser = browser;
  }
}
