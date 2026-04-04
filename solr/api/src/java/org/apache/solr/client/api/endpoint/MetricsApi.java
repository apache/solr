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

import static org.apache.solr.client.api.util.Constants.RAW_OUTPUT_PROPERTY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.StreamingOutput;

/** V2 API definitions to fetch metrics. */
@Path("/metrics")
public interface MetricsApi {

  @GET
  @Operation(
      summary = "Retrieve metrics gathered by Solr.",
      tags = {"metrics"},
      extensions = {
        @Extension(properties = {@ExtensionProperty(name = RAW_OUTPUT_PROPERTY, value = "true")})
      })
  StreamingOutput getMetrics(
      @HeaderParam("Accept") String acceptHeader,
      @Parameter(
              schema =
                  @Schema(
                      name = "node",
                      description = "Name of the node to which proxy the request.",
                      defaultValue = "all"))
          @QueryParam(value = "node")
          String node,
      @Parameter(schema = @Schema(name = "name", description = "The metric name to filter on."))
          @QueryParam(value = "name")
          String name,
      @Parameter(
              schema = @Schema(name = "category", description = "The category label to filter on."))
          @QueryParam(value = "category")
          String category,
      @Parameter(
              schema =
                  @Schema(
                      name = "core",
                      description =
                          "TThe core name to filter on. More than one core can be specified in a comma-separated list."))
          @QueryParam(value = "core")
          String core,
      @Parameter(
              schema =
                  @Schema(name = "collection", description = "The collection name to filter on. "))
          @QueryParam(value = "collection")
          String collection,
      @Parameter(schema = @Schema(name = "shard", description = "The shard name to filter on."))
          @QueryParam(value = "shard")
          String shard,
      @Parameter(
              schema =
                  @Schema(
                      name = "replica_type",
                      description = "The replica type to filter on.",
                      allowableValues = {"NRT", "TLOG", "PULL"}))
          @QueryParam(value = "replica_type")
          String replicaType);
}
