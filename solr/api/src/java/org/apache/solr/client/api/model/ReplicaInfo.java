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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.HashMap;
import java.util.Map;

/**
 * Replica metadata as returned by CLUSTERSTATUS and the shard/replica v2 read APIs.
 *
 * <p>Well-known fields match those CLUSTERSTATUS already serializes per replica. Additional replica
 * properties (for example {@code property.preferredleader}) are included as extra JSON fields.
 */
public class ReplicaInfo {

  @Schema(description = "The SolrCore name for this replica.")
  @JsonProperty
  public String core;

  @Schema(description = "The base URL of the node hosting this replica.")
  @JsonProperty("base_url")
  public String baseUrl;

  @Schema(description = "The name of the node hosting this replica.")
  @JsonProperty("node_name")
  public String nodeName;

  @Schema(description = "The replica state, e.g. active or down.")
  @JsonProperty
  public String state;

  @Schema(description = "The replica type, e.g. NRT, TLOG, or PULL.")
  @JsonProperty
  public String type;

  @Schema(description = "True when this replica is the shard leader. Omitted otherwise.")
  @JsonProperty
  public Boolean leader;

  @Schema(description = "Whether the replica state was forced via FORCESETSTATE.")
  @JsonProperty("force_set_state")
  public Boolean forceSetState;

  private Map<String, Object> additionalProperties = new HashMap<>();

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String field, Object value) {
    additionalProperties.put(field, value);
  }
}
