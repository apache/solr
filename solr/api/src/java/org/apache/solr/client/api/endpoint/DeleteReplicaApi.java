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

import static org.apache.solr.client.api.model.Constants.ASYNC;
import static org.apache.solr.client.api.model.Constants.COUNT_PROP;
import static org.apache.solr.client.api.model.Constants.DELETE_DATA_DIR;
import static org.apache.solr.client.api.model.Constants.DELETE_INDEX;
import static org.apache.solr.client.api.model.Constants.DELETE_INSTANCE_DIR;
import static org.apache.solr.client.api.model.Constants.FOLLOW_ALIASES;
import static org.apache.solr.client.api.model.Constants.ONLY_IF_DOWN;

import io.swagger.v3.oas.annotations.Operation;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ScaleCollectionRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;

/**
 * V2 API definition for deleting one or more existing replicas from one or more shards.
 *
 * <p>These APIs are analogous to the v1 /admin/collections?action=DELETEREPLICA command.
 */
@Path("/collections/{collectionName}")
public interface DeleteReplicaApi {

  @DELETE
  @Path("/shards/{shardName}/replicas/{replicaName}")
  @Operation(
      summary = "Delete an single replica by name",
      tags = {"replicas"})
  SubResponseAccumulatingJerseyResponse deleteReplicaByName(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      @PathParam("replicaName") String replicaName,
      // Optional params below
      @QueryParam(FOLLOW_ALIASES) Boolean followAliases,
      @QueryParam(DELETE_INSTANCE_DIR) Boolean deleteInstanceDir,
      @QueryParam(DELETE_DATA_DIR) Boolean deleteDataDir,
      @QueryParam(DELETE_INDEX) Boolean deleteIndex,
      @QueryParam(ONLY_IF_DOWN) Boolean onlyIfDown,
      @QueryParam(ASYNC) String async)
      throws Exception;

  @DELETE
  @Path("/shards/{shardName}/replicas")
  @Operation(
      summary = "Delete one or more replicas from the specified collection and shard",
      tags = {"replicas"})
  SubResponseAccumulatingJerseyResponse deleteReplicasByCount(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      @QueryParam(COUNT_PROP) Integer numToDelete,
      // Optional params below
      @QueryParam(FOLLOW_ALIASES) Boolean followAliases,
      @QueryParam(DELETE_INSTANCE_DIR) Boolean deleteInstanceDir,
      @QueryParam(DELETE_DATA_DIR) Boolean deleteDataDir,
      @QueryParam(DELETE_INDEX) Boolean deleteIndex,
      @QueryParam(ONLY_IF_DOWN) Boolean onlyIfDown,
      @QueryParam(ASYNC) String async)
      throws Exception;

  @PUT
  @Path("/scale")
  @Operation(
      summary = "Scale the replica count for all shards in the specified collection",
      tags = {"replicas"})
  SubResponseAccumulatingJerseyResponse deleteReplicasByCountAllShards(
      @PathParam("collectionName") String collectionName, ScaleCollectionRequestBody requestBody)
      throws Exception;
}
