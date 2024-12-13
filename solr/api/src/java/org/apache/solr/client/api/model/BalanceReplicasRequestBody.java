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
import java.util.Set;

public class BalanceReplicasRequestBody {

  public BalanceReplicasRequestBody() {}

  public BalanceReplicasRequestBody(Set<String> nodes, Boolean waitForFinalState, String async) {
    this.nodes = nodes;
    this.waitForFinalState = waitForFinalState;
    this.async = async;
  }

  @Schema(
      description =
          "The set of nodes across which replicas will be balanced. Defaults to all live data nodes.")
  @JsonProperty(value = "nodes")
  public Set<String> nodes;

  @Schema(
      description =
          "If true, the request will complete only when all affected replicas become active. "
              + "If false, the API will return the status of the single action, which may be "
              + "before the new replica is online and active.")
  @JsonProperty("waitForFinalState")
  public Boolean waitForFinalState = false;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty("async")
  public String async;
}
