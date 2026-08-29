package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CancelTaskResponse {

  @JsonProperty
  public CancelTaskResponse.TaskStatus status;

}
