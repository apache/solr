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
import org.apache.solr.client.api.model.ListUsersResponse;
import org.apache.solr.client.api.model.SetUserRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/**
 * Definitions for v2 JAX-RS APIs managing Basic Authentication users.
 *
 * <p>These APIs are a resource-oriented alternative to the "set-user"/"delete-user" commands
 * accepted by the {@code /cluster/security/authentication} API - both operate on the same
 * underlying plugin configuration.
 *
 * <p>The {@code scheme} path segment names the authentication scheme these users belong to (e.g.
 * "basic"), as configured under {@code MultiAuthPlugin}'s "schemes" list. It is ignored when {@code
 * MultiAuthPlugin} isn't in use - a plain {@code BasicAuthPlugin} setup has only one set of users,
 * and any value may be supplied (conventionally "basic").
 */
@Path("/cluster/security/authentication/{scheme}/users")
public interface AuthenticationUsersApi {
  @GET
  @Operation(
      summary = "List the usernames configured for Basic Authentication.",
      tags = {"authentication"})
  ListUsersResponse listUsers(
      @Parameter(description = "The authentication scheme these users belong to.", required = true)
          @PathParam("scheme")
          String scheme);

  @PUT
  @Path("/{username}")
  @Operation(
      summary = "Create a new user, or change an existing user's password.",
      tags = {"authentication"})
  SolrJerseyResponse createOrUpdateUser(
      @Parameter(description = "The authentication scheme this user belongs to.", required = true)
          @PathParam("scheme")
          String scheme,
      @Parameter(description = "The username to create or update.", required = true)
          @PathParam("username")
          String username,
      @RequestBody(description = "The new password for this user.", required = true)
          SetUserRequestBody requestBody)
      throws Exception;

  @DELETE
  @Path("/{username}")
  @Operation(
      summary = "Delete a Basic Authentication user.",
      tags = {"authentication"})
  SolrJerseyResponse deleteUser(
      @Parameter(description = "The authentication scheme this user belongs to.", required = true)
          @PathParam("scheme")
          String scheme,
      @Parameter(description = "The username to delete.", required = true) @PathParam("username")
          String username)
      throws Exception;
}
