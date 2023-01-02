package com.flipkart.neo.solr.ltr.query.models.context;

public class VernacularInfo {
  private String locale;
  private boolean isLocaleRelaxed;

  public String getLocale() {
    return locale;
  }

  public void setLocale(String locale) {
    this.locale = locale;
  }

  public boolean isLocaleRelaxed() {
    return isLocaleRelaxed;
  }

  public void setLocaleRelaxed(boolean localeRelaxed) {
    isLocaleRelaxed = localeRelaxed;
  }
}
