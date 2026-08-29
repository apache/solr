package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CancelTaskResponse extends SolrJerseyResponse {

  public enum CancellationStatus {
    SUCCESS,
    NOT_FOUND
  }

  @JsonProperty
  public CancelTaskResponse.CancellationStatus status;

}
