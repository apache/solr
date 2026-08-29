package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CancelTaskResponse extends SolrJerseyResponse {

  public enum CancellationStatus {
    SUCCESS("success"),
    NOT_FOUND("not_found");

    private final String value;

    CancellationStatus(String value) {
      this.value = value;
    }

    public String getValue() {
      return this.value;
    }
  }

  @JsonProperty
  public CancelTaskResponse.CancellationStatus status;

}
