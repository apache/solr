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

import static org.apache.solr.client.api.util.Constants.BINARY_CONTENT_TYPE_V2;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/**
 * V2 API definition for removing a property from a collection replica
 *
 * <p>This API is analogous to the v1 /admin/collections?action=DELETEREPLICAPROP command.
 */
@Path("/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}")
public interface DeleteReplicaPropertyApi {

  @DELETE
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @Operation(
      summary = "Delete an existing replica property",
      tags = {"replica-properties"})
  SolrJerseyResponse deleteReplicaProperty(
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
      @Parameter(description = "The name of the property to delete.", required = true)
          @PathParam("propName")
          String propertyName)
      throws Exception;
}
