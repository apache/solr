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

package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.AddReplicaPropertyRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

@Path("/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}")
public interface AddReplicaPropertyApi {

  @PUT
  @Operation(
      summary = "Adds a property to the specified replica",
      tags = {"replica-properties"})
  public SolrJerseyResponse addReplicaProperty(
      @Parameter(
              description = "The name of the collection the replica belongs to.",
              required = true)
          @PathParam("collName")
          String collName,
      @Parameter(description = "The name of the shard the replica belongs to.", required = true)
          @PathParam("shardName")
          String shardName,
      @Parameter(description = "The replica, e.g., `core_node1`.", required = true)
          @PathParam("replicaName")
          String replicaName,
      @Parameter(description = "The name of the property to add.", required = true)
          @PathParam("propName")
          String propertyName,
      @RequestBody(
              description = "The value of the replica property to create or update",
              required = true)
          AddReplicaPropertyRequestBody requestBody)
      throws Exception;
}
