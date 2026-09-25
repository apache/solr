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
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import org.apache.solr.client.api.model.CreatePermissionResponse;
import org.apache.solr.client.api.model.ListPermissionsResponse;
import org.apache.solr.client.api.model.PermissionDefinition;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/**
 * Definitions for v2 JAX-RS APIs managing Rule-Based Authorization permissions.
 *
 * <p>Resource-oriented alternative to the {@code set-permission}/{@code update-permission}/{@code
 * delete-permission} commands accepted by the {@code /cluster/security/authorization} API. A
 * permission's {@code index} - its position in the evaluated-top-down list - moves from a body
 * field to a path parameter.
 */
@Path("/cluster/security/authorization/permissions")
public interface AuthorizationPermissionsApi {
  @GET
  @Operation(
      summary = "List the configured permissions, in evaluation order.",
      tags = {"authorization"})
  ListPermissionsResponse listPermissions();

  @POST
  @Operation(
      summary = "Create a new permission.",
      tags = {"authorization"})
  CreatePermissionResponse createPermission(
      @RequestBody(description = "The permission to create.", required = true)
          PermissionDefinition requestBody)
      throws Exception;

  @PUT
  @Path("/{index}")
  @Operation(
      summary = "Update an existing permission.",
      tags = {"authorization"})
  SolrJerseyResponse updatePermission(
      @Parameter(description = "The index of the permission to update.", required = true)
          @PathParam("index")
          int index,
      @RequestBody(description = "The fields to update.", required = true)
          PermissionDefinition requestBody)
      throws Exception;

  @DELETE
  @Path("/{index}")
  @Operation(
      summary = "Delete a permission.",
      tags = {"authorization"})
  SolrJerseyResponse deletePermission(
      @Parameter(description = "The index of the permission to delete.", required = true)
          @PathParam("index")
          int index)
      throws Exception;
}
