package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

public class UpgradeCoreIndexResponse extends SolrJerseyResponse {
  @Schema(description = "The name of the core.")
  @JsonProperty
  public String core;

  @Schema(description = "The total number of segments eligible for upgrade.")
  @JsonProperty
  public Integer numSegmentsEligibleForUpgrade;

  @Schema(description = "The number of segments successfully upgraded.")
  @JsonProperty
  public Integer numSegmentsUpgraded;

  @Schema(description = "Status of the core index upgrade operation.")
  @JsonProperty
  public String upgradeStatus;
}
