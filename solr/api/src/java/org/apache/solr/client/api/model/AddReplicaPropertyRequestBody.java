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

public class AddReplicaPropertyRequestBody {
  public AddReplicaPropertyRequestBody() {}

  public AddReplicaPropertyRequestBody(String value) {
    this.value = value;
  }

  @Schema(description = "The value to assign to the property.", required = true)
  @JsonProperty("value")
  public String value;

  @Schema(
      description =
          "If `true`, then setting this property in one replica will remove the property from all other replicas in that shard. The default is `false`.\\nThere is one pre-defined property `preferredLeader` for which `shardUnique` is forced to `true` and an error returned if `shardUnique` is explicitly set to `false`.",
      defaultValue = "false")
  @JsonProperty("shardUnique")
  public Boolean shardUnique;
}
