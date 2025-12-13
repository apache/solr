package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public class UpgradeCoreIndexRequestBody {

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty
  public String async;

  @JsonProperty public String updateChain;
}
