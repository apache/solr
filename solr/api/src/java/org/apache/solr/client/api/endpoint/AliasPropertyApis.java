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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import org.apache.solr.client.api.model.GetAliasPropertyResponse;
import org.apache.solr.client.api.model.GetAllAliasPropertiesResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.UpdateAliasPropertiesRequestBody;
import org.apache.solr.client.api.model.UpdateAliasPropertyRequestBody;

/** V2 API definitions for managing and inspecting properties for collection aliases */
@Path("/aliases/{aliasName}/properties")
public interface AliasPropertyApis {

  @GET
  @Operation(
      summary = "Get properties for a collection alias.",
      tags = {"alias-properties"})
  GetAllAliasPropertiesResponse getAllAliasProperties(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName)
      throws Exception;

  @GET
  @Path("/{propName}")
  @Operation(
      summary = "Get a specific property for a collection alias.",
      tags = {"alias-properties"})
  GetAliasPropertyResponse getAliasProperty(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @Parameter(description = "Property Name") @PathParam("propName") String propName)
      throws Exception;

  @PUT
  @Operation(
      summary = "Update properties for a collection alias.",
      tags = {"alias-properties"})
  SolrJerseyResponse updateAliasProperties(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @RequestBody(description = "Properties that need to be updated", required = true)
          UpdateAliasPropertiesRequestBody requestBody)
      throws Exception;

  @PUT
  @Path("/{propName}")
  @Operation(
      summary = "Update a specific property for a collection alias.",
      tags = {"alias-properties"})
  SolrJerseyResponse createOrUpdateAliasProperty(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @Parameter(description = "Property Name") @PathParam("propName") String propName,
      @RequestBody(description = "Property value that needs to be updated", required = true)
          UpdateAliasPropertyRequestBody requestBody)
      throws Exception;

  @DELETE
  @Path("/{propName}")
  @Operation(
      summary = "Delete a specific property for a collection alias.",
      tags = {"alias-properties"})
  SolrJerseyResponse deleteAliasProperty(
      @Parameter(description = "Alias Name") @PathParam("aliasName") String aliasName,
      @Parameter(description = "Property Name") @PathParam("propName") String propName)
      throws Exception;
}
