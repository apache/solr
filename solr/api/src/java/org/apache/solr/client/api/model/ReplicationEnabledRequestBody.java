package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplicationEnabledRequestBody {

  public ReplicationEnabledRequestBody() {}

  public ReplicationEnabledRequestBody(boolean enabled) {
    this.enabled = enabled;
  }

  @JsonProperty(required = true)
  public boolean enabled;
}
