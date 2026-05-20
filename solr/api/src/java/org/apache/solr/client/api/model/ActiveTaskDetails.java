package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ActiveTaskDetails {

  public ActiveTaskDetails() {}

  public ActiveTaskDetails(String taskUUID, String taskQuery) {
    this.taskUUID = taskUUID;
    this.taskQuery = taskQuery;
  }

  @JsonProperty public String taskUUID;
  @JsonProperty public String taskQuery;
}
