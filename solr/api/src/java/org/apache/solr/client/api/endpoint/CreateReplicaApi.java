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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.CreateReplicaRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;

/**
 * V2 API definition for adding a new replica to an existing shard.
 *
 * <p>This API (POST /v2/collections/cName/shards/sName/replicas {...}) is analogous to the v1
 * /admin/collections?action=ADDREPLICA command.
 */
@Path("/collections/{collectionName}/shards/{shardName}/replicas")
public interface CreateReplicaApi {

  @POST
  @Operation(
      summary = "Creates a new replica of an existing shard.",
      tags = {"replicas"})
  SubResponseAccumulatingJerseyResponse createReplica(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      CreateReplicaRequestBody requestBody)
      throws Exception;
}
