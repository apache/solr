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

import io.swagger.v3.oas.annotations.Operation;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;

/** V2 API definition for deleting a particular shard from its collection. */
@Path("/collections/{collectionName}/shards/{shardName}")
public interface DeleteShardApi {

  @DELETE
  @Operation(
      summary = "Delete an existing shard",
      tags = {"shards"})
  SubResponseAccumulatingJerseyResponse deleteShard(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      @QueryParam("deleteInstanceDir") Boolean deleteInstanceDir,
      @QueryParam("deleteDataDir") Boolean deleteDataDir,
      @QueryParam("deleteIndex") Boolean deleteIndex,
      @QueryParam("followAliases") Boolean followAliases,
      @QueryParam(ASYNC) String asyncId)
      throws Exception;
}
