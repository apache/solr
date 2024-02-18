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

public class ReplaceNodeRequestBody {

  public ReplaceNodeRequestBody() {}

  public ReplaceNodeRequestBody(String targetNodeName, Boolean waitForFinalState, String async) {
    this.targetNodeName = targetNodeName;
    this.waitForFinalState = waitForFinalState;
    this.async = async;
  }

  @Schema(
      description =
          "The target node where replicas will be copied. If this parameter is not provided, Solr "
              + "will identify nodes automatically based on policies or number of cores in each node.")
  @JsonProperty("targetNodeName")
  public String targetNodeName;

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
