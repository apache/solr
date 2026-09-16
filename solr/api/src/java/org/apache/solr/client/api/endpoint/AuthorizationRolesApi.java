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
import org.apache.solr.client.api.model.GetUserRolesResponse;
import org.apache.solr.client.api.model.SetUserRolesRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/**
 * Definitions for v2 JAX-RS APIs mapping roles to users under Rule-Based Authorization.
 *
 * <p>Resource-oriented alternative to the {@code set-user-role} command accepted by the {@code
 * /cluster/security/authorization} API. A {@code DELETE} replaces that command's {@code null} value
 * idiom for revoking a user's roles.
 *
 * <p>The {@code scheme} path segment names the authentication scheme these role mappings belong to
 * (e.g. "basic"), as configured under {@code MultiAuthRuleBasedAuthorizationPlugin}'s "schemes"
 * list. It is ignored when that plugin isn't in use - a plain {@code RuleBasedAuthorizationPlugin}
 * setup has only one set of role mappings, and any value may be supplied (conventionally "basic").
 * Unlike roles, permissions are shared across every scheme, so {@link AuthorizationPermissionsApi}
 * has no such segment.
 */
@Path("/cluster/security/authorization/{scheme}/roles")
public interface AuthorizationRolesApi {
  @GET
  @Path("/{username}")
  @Operation(
      summary = "Get the roles assigned to a user.",
      tags = {"authorization"})
  GetUserRolesResponse getUserRoles(
      @Parameter(description = "The authentication scheme this user belongs to.", required = true)
          @PathParam("scheme")
          String scheme,
      @Parameter(description = "The username to look up.", required = true) @PathParam("username")
          String username);

  @PUT
  @Path("/{username}")
  @Operation(
      summary = "Assign roles to a user, replacing any roles it already has.",
      tags = {"authorization"})
  SolrJerseyResponse setUserRoles(
      @Parameter(description = "The authentication scheme this user belongs to.", required = true)
          @PathParam("scheme")
          String scheme,
      @Parameter(description = "The username to assign roles to.", required = true)
          @PathParam("username")
          String username,
      @RequestBody(description = "The roles to assign.", required = true)
          SetUserRolesRequestBody requestBody)
      throws Exception;

  @DELETE
  @Path("/{username}")
  @Operation(
      summary = "Revoke all roles from a user.",
      tags = {"authorization"})
  SolrJerseyResponse deleteUserRoles(
      @Parameter(description = "The authentication scheme this user belongs to.", required = true)
          @PathParam("scheme")
          String scheme,
      @Parameter(description = "The username to revoke roles from.", required = true)
          @PathParam("username")
          String username)
      throws Exception;
}
