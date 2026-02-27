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

/** Response for the v2 "list cluster nodes" API.
 * This is a bit unusual that it's wrapping a non JAX-RS V2 API defined in org.apache.solr.handler.ClusterAPI.getNodes()
 * The calls are made using just the defaults.
 */
public class ListClusterNodesResponse extends SolrJerseyResponse {

  @Schema(description = "The live nodes in the cluster.")
  @JsonProperty("nodes")
  public Set<String> nodes;
}
