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
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.NodeSystemResponse;
import org.apache.solr.client.api.util.Constants;

/** V2 API definitions to fetch node system info, analogous to the v1 /admin/info/system. */
@Path(Constants.NODE_INFO_SYSTEM_PATH)
public interface NodeSystemInfoApi {

  @GET
  @Operation(
      summary = "Retrieve all node system info.",
      tags = {"system"})
  NodeSystemResponse getNodeSystemInfo(@QueryParam(value = "nodes") String nodes);

  @GET
  @Operation(
      summary = "Retrieve specific node system info.",
      tags = {"system"},
      parameters = {
        @Parameter(
            name = "requestedInfo",
            description = "Allowed values: 'gpu', 'jvm', 'lucene', 'security', 'system'")
      })
  @Path("/{requestedInfo}")
  NodeSystemResponse getSpecificNodeSystemInfo(
      @PathParam(value = "requestedInfo") String requestedInfo,
      @QueryParam(value = "nodes") String nodes);
}
