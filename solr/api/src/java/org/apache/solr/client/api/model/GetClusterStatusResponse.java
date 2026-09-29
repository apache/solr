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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Response body for {@code GET /api/cluster}.
 *
 * <p>Shards, replicas, health, and the other stable collection-state fields are typed. A collection
 * state document is otherwise open: router settings, replica placement counts, user properties, and
 * per-replica state are preserved on the object rather than dropped. Each collection still lists the
 * aliases that point at it.
 */
public class GetClusterStatusResponse extends SolrJerseyResponse {

  @Schema(description = "Cluster geometry: collections, shards, and replicas.")
  @JsonProperty("cluster")
  public Cluster cluster;

  public static class Cluster {
    @Schema(description = "Collections keyed by name.")
    @JsonProperty("collections")
    public Map<String, CollectionState> collections;
  }

  /** State of one collection, including its shards and replicas. */
  public static class CollectionState {
    @JsonProperty public Map<String, ShardState> shards;

    @Schema(description = "Worst shard health in this collection: GREEN, YELLOW, ORANGE, or RED.")
    @JsonProperty
    public String health;

    @JsonProperty public String configName;
    @JsonProperty public Integer znodeVersion;
    @JsonProperty public Long creationTimeMillis;

    @Schema(description = "Aliases that point at this collection.")
    @JsonProperty
    public List<String> aliases;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  /** State of one shard. */
  public static class ShardState {
    @JsonProperty public String state;
    @JsonProperty public String range;

    @Schema(description = "Shard health: GREEN, YELLOW, ORANGE, or RED.")
    @JsonProperty
    public String health;

    @JsonProperty public Map<String, ReplicaState> replicas;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }

  /** State of one replica. */
  public static class ReplicaState {
    @JsonProperty public String state;
    @JsonProperty public String core;

    @JsonProperty("node_name")
    public String nodeName;

    @JsonProperty("base_url")
    public String baseUrl;

    /**
     * {@code "true"} when this replica is the leader. Absent otherwise. state.json stores this as a
     * string.
     */
    @JsonProperty public String leader;

    @JsonProperty public String type;

    private final Map<String, Object> additionalProperties = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> unknownProperties() {
      return additionalProperties;
    }

    @JsonAnySetter
    public void setUnknownProperty(String field, Object value) {
      additionalProperties.put(field, value);
    }
  }
}
