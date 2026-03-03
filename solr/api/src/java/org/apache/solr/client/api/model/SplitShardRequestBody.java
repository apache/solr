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

/**
 * Request body for splitting a shard via {@link org.apache.solr.client.api.endpoint.SplitShardApi}
 */
public class SplitShardRequestBody {
  @Schema(description = "The name of the shard to be split.")
  @JsonProperty
  public String shard;

  @Schema(description = "Comma-separated hash ranges to split the shard into (e.g. 'a-b,c-d').")
  @JsonProperty
  public String ranges;

  @Schema(description = "A route key to be used for splitting the index.")
  @JsonProperty
  public String splitKey;

  @Schema(description = "The number of sub-shards to create from the shard.")
  @JsonProperty
  public Integer numSubShards;

  @Schema(description = "A fuzzy factor for the split (e.g. '0.2').")
  @JsonProperty
  public String splitFuzz;

  @Schema(description = "When true, timing information is included in the response.")
  @JsonProperty
  public Boolean timing;

  @Schema(description = "When true, split ranges are determined based on prefix distribution.")
  @JsonProperty
  public Boolean splitByPrefix;

  @Schema(description = "When true, aliases are followed to find the actual collection name.")
  @JsonProperty
  public Boolean followAliases;

  @Schema(description = "The method to use for splitting the index (e.g. 'rewrite' or 'link').")
  @JsonProperty
  public String splitMethod;

  @Schema(description = "Core properties to set on the sub-shards.")
  @JsonProperty
  public Map<String, Object> coreProperties;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty
  public String async;

  @Deprecated(since = "9.10")
  @Schema(description = "Deprecated. When true, waits for the final state before returning.")
  @JsonProperty
  public Boolean waitForFinalState;
}
