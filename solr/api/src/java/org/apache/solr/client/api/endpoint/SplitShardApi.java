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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SplitShardRequestBody;

/**
 * V2 API definition for splitting an existing shard into multiple pieces.
 *
 * <p>The new API (POST /api/collections/{collectionName}/shards/split) is equivalent to the v1
 * /admin/collections?action=SPLITSHARD command.
 */
@Path("/collections/{collectionName}/shards/split")
public interface SplitShardApi {
  @POST
  @Operation(
      summary = "Splits a shard in an existing collection into two or more shards.",
      tags = {"shards"})
  SolrJerseyResponse splitShard(
      @Parameter(description = "The name of the collection containing the shard.", required = true)
          @PathParam("collectionName")
          String collectionName,
      @RequestBody(description = "Additional properties for the shard split operation.")
          SplitShardRequestBody requestBody)
      throws Exception;
}
