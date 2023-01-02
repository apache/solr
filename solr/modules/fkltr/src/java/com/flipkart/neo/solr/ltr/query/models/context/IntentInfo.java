package com.flipkart.neo.solr.ltr.query.models.context;

import java.util.List;
import java.util.Map;

public class IntentInfo {
  private String searchQuery;
  private String searchQueryType;
  private List<String> topIntentStores;
  private List<String> intentParentStores;
  private Map<String, Double> relevantStores;
  private Map<String, String> relevantFacets;

  public String getSearchQuery() {
    return searchQuery;
  }

  public void setSearchQuery(String searchQuery) {
    this.searchQuery = searchQuery;
  }

  public String getSearchQueryType() {
    return searchQueryType;
  }

  public void setSearchQueryType(String searchQueryType) {
    this.searchQueryType = searchQueryType;
  }

  public List<String> getTopIntentStores() {
    return topIntentStores;
  }

  public void setTopIntentStores(List<String> topIntentStores) {
    this.topIntentStores = topIntentStores;
  }

  public List<String> getIntentParentStores() {
    return intentParentStores;
  }

  public void setIntentParentStores(List<String> intentParentStores) {
    this.intentParentStores = intentParentStores;
  }

  public Map<String, Double> getRelevantStores() {
    return relevantStores;
  }

  public void setRelevantStores(Map<String, Double> relevantStores) {
    this.relevantStores = relevantStores;
  }

  public Map<String, String> getRelevantFacets() {
    return relevantFacets;
  }

  public void setRelevantFacets(Map<String, String> relevantFacets) {
    this.relevantFacets = relevantFacets;
  }
}