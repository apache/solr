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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.GetAliasByNameResponse;
import org.apache.solr.client.api.model.ListAliasesResponse;

/** V2 API definition for listing and inspecting collection aliases */
@Path("/aliases")
public interface ListAliasesApi {

  @GET
  @Operation(
      summary = "List the existing collection aliases.",
      tags = {"aliases"})
  ListAliasesResponse getAliases() throws Exception;

  @GET
  @Path("/{aliasName}")
  @Operation(
      summary = "Get details for a specific collection alias.",
      tags = {"aliases"})
  GetAliasByNameResponse getAliasByName(
      @Parameter(description = "Alias name.", required = true) @PathParam("aliasName")
          String aliasName)
      throws Exception;
}
