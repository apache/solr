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
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ListCollectionsResponse;

@Path("/collections")
public interface ListCollectionsApi {
  @GET
  @Operation(
      summary = "List all collections in this Solr cluster",
      tags = {"collections"})
  ListCollectionsResponse listCollections(
      @Parameter(
              description =
                  "When true, return the collections, shards, and replicas tree (in"
                      + " 'collectionsDetail') instead of the plain collection name list.")
          @QueryParam("detailed")
          Boolean detailed,
      @Parameter(
              description =
                  "Only used when 'detailed' is true. Collection or alias to return. Omit to"
                      + " return every collection. An alias returns the collections it points"
                      + " at.")
          @QueryParam("collection")
          String collection,
      @Parameter(
              description =
                  "Only used when 'detailed' is true. Shard or comma-separated shards to return."
                      + " Applied to each selected collection.")
          @QueryParam("shard")
          String shard,
      @Parameter(
              description =
                  "Only used when 'detailed' is true. Route key of a document. Limits the tree to"
                      + " the shard that would hold that document.")
          @QueryParam("_route_")
          String routeKey,
      @Parameter(
              description =
                  "Only used when 'detailed' is true. Include per-replica state when the"
                      + " collection uses it.")
          @QueryParam("prs")
          Boolean prs)
      throws Exception;
}
