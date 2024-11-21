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
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import java.util.Map;
import org.apache.solr.client.api.model.ListClusterPropertiesResponse;
import org.apache.solr.client.api.model.SetClusterPropertyRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** Definitions for v2 JAX-RS cluster properties APIs. */
@Path("/cluster/properties")
public interface ClusterPropertyApis {
  @GET
  @Operation(
      summary = "List all cluster properties in this Solr cluster.",
      tags = {"cluster-properties"})
  ListClusterPropertiesResponse listClusterProperties();

  @GET
  @Path("/{propertyName}")
  @Operation(
      summary = "Get a cluster property in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse getClusterProperty(
      @Parameter(description = "The name of the property being retrieved.", required = true)
          @PathParam("propertyName")
          String propertyName);

  @PUT
  @Path("/{propertyName}")
  @Operation(
      summary = "Set a single new or existing cluster property in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse createOrUpdateClusterProperty(
      @Parameter(description = "The name of the property being set.", required = true)
          @PathParam("propertyName")
          String propertyName,
      @RequestBody(description = "Value to set for the property", required = true)
          SetClusterPropertyRequestBody requestBody)
      throws Exception;

  @PUT
  @Operation(
      summary = "Set nested cluster properties in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse createOrUpdateNestedClusterProperty(
      @RequestBody(description = "Property/ies to be set", required = true)
          Map<String, Object> propertyValuesByName)
      throws Exception;

  @DELETE
  @Path("/{propertyName}")
  @Operation(
      summary = "Delete a cluster property in this Solr cluster.",
      tags = {"cluster-properties"})
  SolrJerseyResponse deleteClusterProperty(
      @Parameter(description = "The name of the property being deleted.", required = true)
          @PathParam("propertyName")
          String propertyName);
}
