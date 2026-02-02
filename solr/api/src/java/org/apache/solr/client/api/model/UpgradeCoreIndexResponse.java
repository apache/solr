/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
