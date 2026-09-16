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
package org.apache.solr.handler.admin.api;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.CreatePermissionResponse;
import org.apache.solr.client.api.model.GetUserRolesResponse;
import org.apache.solr.client.api.model.ListPermissionsResponse;
import org.apache.solr.client.api.model.ListUsersResponse;
import org.apache.solr.client.api.model.PermissionDetails;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.AuthenticationApi;
import org.apache.solr.client.solrj.request.AuthorizationApi;
import org.apache.solr.util.SecurityJson;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * HTTP tests, under Basic Authentication (with {@code blockUnknown: true}, so every request below
 * needs credentials), for the resource-oriented v2 APIs at {@code
 * /api/cluster/security/authentication/{scheme}/users} and {@code
 * /api/cluster/security/authorization/{scheme}/roles} and {@code .../permissions}, via the
 * generated {@link AuthenticationApi} and {@link AuthorizationApi} SolrJ client classes.
 *
 * <p>The plugin under test here is a plain (non-multi) {@code BasicAuthPlugin}, so the {@code
 * scheme} path segment is ignored server-side; "basic" is used here purely by convention. See
 * {@link org.apache.solr.security.MultiAuthPluginTest} for coverage of the scheme actually being
 * honored under {@code MultiAuthPlugin}.
 */
public class SecurityV2ApiStandaloneTest extends SolrTestCase {

  private static final String SCHEME = "basic";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolr() throws Exception {
    Path solrHome = createTempDir();
    Files.writeString(
        solrHome.resolve("security.json"), SecurityJson.SIMPLE, StandardCharsets.UTF_8);
    solrTestRule.startSolr(solrHome);
  }

  private static <T extends SolrRequest<?>> T authed(T request) {
    request.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    return request;
  }

  @Test
  public void testUsersLifecycle() throws Exception {
    ListUsersResponse users =
        authed(new AuthenticationApi.ListUsers(SCHEME)).process(solrTestRule.getAdminClient());
    assertThat(users.users, containsInAnyOrder(SecurityJson.USER));

    var createTom = authed(new AuthenticationApi.CreateOrUpdateUser(SCHEME, "tom"));
    createTom.setPassword("TomIsCool");
    createTom.process(solrTestRule.getAdminClient());

    users = authed(new AuthenticationApi.ListUsers(SCHEME)).process(solrTestRule.getAdminClient());
    assertThat(users.users, containsInAnyOrder(SecurityJson.USER, "tom"));

    // Unauthenticated mutation is rejected - deliberately NOT using authed() here
    final RemoteSolrException unauth =
        expectThrows(
            RemoteSolrException.class,
            () ->
                new AuthenticationApi.DeleteUser(SCHEME, "tom")
                    .process(solrTestRule.getAdminClient()));
    assertEquals(401, unauth.code());

    // Deleting an unknown user is a 404
    final RemoteSolrException notFound =
        expectThrows(
            RemoteSolrException.class,
            () ->
                authed(new AuthenticationApi.DeleteUser(SCHEME, "does-not-exist"))
                    .process(solrTestRule.getAdminClient()));
    assertEquals(404, notFound.code());

    authed(new AuthenticationApi.DeleteUser(SCHEME, "tom")).process(solrTestRule.getAdminClient());

    users = authed(new AuthenticationApi.ListUsers(SCHEME)).process(solrTestRule.getAdminClient());
    assertThat(users.users, containsInAnyOrder(SecurityJson.USER));

    // Deleting the last remaining user is a conflict, not silently allowed
    final RemoteSolrException conflict =
        expectThrows(
            RemoteSolrException.class,
            () ->
                authed(new AuthenticationApi.DeleteUser(SCHEME, SecurityJson.USER))
                    .process(solrTestRule.getAdminClient()));
    assertEquals(409, conflict.code());
  }

  @Test
  public void testRolesLifecycle() throws Exception {
    GetUserRolesResponse roles =
        authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry"))
            .process(solrTestRule.getAdminClient());
    assertTrue(roles.roles.isEmpty());

    var setRoles = authed(new AuthorizationApi.SetUserRoles(SCHEME, "harry"));
    setRoles.setRoles(List.of("dev"));
    setRoles.process(solrTestRule.getAdminClient());

    roles =
        authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry"))
            .process(solrTestRule.getAdminClient());
    assertThat(roles.roles, containsInAnyOrder("dev"));

    authed(new AuthorizationApi.DeleteUserRoles(SCHEME, "harry"))
        .process(solrTestRule.getAdminClient());

    roles =
        authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry"))
            .process(solrTestRule.getAdminClient());
    assertTrue(roles.roles.isEmpty());
  }

  @Test
  public void testPermissionsLifecycle() throws Exception {
    ListPermissionsResponse initial =
        authed(new AuthorizationApi.ListPermissions()).process(solrTestRule.getAdminClient());
    int initialCount = initial.permissions.size();

    var create = authed(new AuthorizationApi.CreatePermission());
    create.setName("read");
    create.setRole(List.of("guest"));
    CreatePermissionResponse createResponse = create.process(solrTestRule.getAdminClient());
    assertNotNull(createResponse.index);
    int newIndex = createResponse.index;

    ListPermissionsResponse afterCreate =
        authed(new AuthorizationApi.ListPermissions()).process(solrTestRule.getAdminClient());
    assertEquals(initialCount + 1, afterCreate.permissions.size());
    assertThat(afterCreate.permissions.stream().map(p -> p.index).toList(), hasItem(newIndex));

    var update = authed(new AuthorizationApi.UpdatePermission(newIndex));
    update.setRole(List.of("guest", "dev"));
    update.process(solrTestRule.getAdminClient());

    ListPermissionsResponse afterUpdate =
        authed(new AuthorizationApi.ListPermissions()).process(solrTestRule.getAdminClient());
    PermissionDetails updated =
        afterUpdate.permissions.stream()
            .filter(p -> Integer.valueOf(newIndex).equals(p.index))
            .findFirst()
            .orElseThrow();
    assertThat(updated.role, containsInAnyOrder("guest", "dev"));

    authed(new AuthorizationApi.DeletePermission(newIndex)).process(solrTestRule.getAdminClient());

    ListPermissionsResponse afterDelete =
        authed(new AuthorizationApi.ListPermissions()).process(solrTestRule.getAdminClient());
    assertEquals(initialCount, afterDelete.permissions.size());

    // Deleting an already-removed index is a 404
    final RemoteSolrException notFound =
        expectThrows(
            RemoteSolrException.class,
            () ->
                authed(new AuthorizationApi.DeletePermission(newIndex))
                    .process(solrTestRule.getAdminClient()));
    assertEquals(404, notFound.code());
  }
}
