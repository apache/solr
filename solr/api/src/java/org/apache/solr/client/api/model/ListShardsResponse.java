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
import java.util.Map;

/** Response body for {@code GET /api/collections/{collectionName}/shards}. */
public class ListShardsResponse extends SolrJerseyResponse {

  @Schema(description = "Shard-level status keyed by shard name.")
  @JsonProperty("shards")
  public Map<String, ShardSummary> shards;

  /** Summary of a single shard, without replica core or segment detail. */
  public static class ShardSummary {
    @Schema(
        description =
            "Hash range assigned to this shard, if the collection uses a hash-based router.")
    @JsonProperty
    public String range;

    @Schema(
        description = "Shard state (active, inactive, construction, recovery, or recovery_failed).")
    @JsonProperty
    public String state;

    @Schema(
        description =
            "GREEN/YELLOW/ORANGE/RED replica-health signal from CLUSTERSTATUS: fraction of ACTIVE replicas on live nodes, plus leader presence.")
    @JsonProperty
    public String replicaHealth;

    @Schema(description = "Total number of replicas in this shard.")
    @JsonProperty
    public Integer replicaCount;

    @Schema(description = "Number of replicas that are ACTIVE and on a live node.")
    @JsonProperty
    public Integer activeReplicaCount;
  }
}
