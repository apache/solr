package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskStatusResponse extends SolrJerseyResponse {
  @JsonProperty
  public boolean taskStatus;
}
