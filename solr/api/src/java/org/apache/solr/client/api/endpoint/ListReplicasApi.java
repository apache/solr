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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.ListReplicasResponse;

/**
 * V2 API definition for listing the replicas of a collection shard.
 *
 * <p>This API (GET /api/collections/collName/shards/shardName/replicas) has no dedicated v1
 * equivalent; {@code /admin/collections?action=CLUSTERSTATUS} is the closest v1 form.
 */
@Path("/collections/{collectionName}/shards/{shardName}/replicas")
public interface ListReplicasApi {

  @GET
  @Operation(
      summary = "List the replicas of the specified collection and shard",
      tags = {"replicas"})
  ListReplicasResponse listReplicas(
      @Parameter(description = "The name of the collection.", required = true)
          @PathParam("collectionName")
          String collectionName,
      @Parameter(description = "The name of the shard.", required = true) @PathParam("shardName")
          String shardName);
}
