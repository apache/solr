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

import java.util.List;
import org.apache.solr.client.api.model.CreatePermissionResponse;
import org.apache.solr.client.api.model.GetUserRolesResponse;
import org.apache.solr.client.api.model.ListPermissionsResponse;
import org.apache.solr.client.api.model.ListUserRolesResponse;
import org.apache.solr.client.solrj.request.AuthorizationApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * SolrCloud-mode coverage for {@link Permissions}/{@link Roles}. Both read via {@code
 * SecurityConfHandler#getSecurityConfig(true)} (fresh, bypassing {@code SecurityConfHandlerZk}'s
 * cached ZK snapshot) specifically so a GET immediately following one of their own writes is
 * guaranteed to observe it, without any client-side polling for propagation - see {@code
 * SecurityConfHandler#getSecurityConfig}'s javadoc for why a cached ({@code getFresh=false}) read
 * can otherwise lag a write briefly. Standalone mode ({@code SecurityConfHandlerLocal}) always
 * reads security.json fresh from disk regardless of this flag, so this behavior needs cloud
 * coverage specifically to mean anything.
 *
 * <p>This also incidentally guards against a real bug this suite caught during development: the
 * cached path's snapshot is rebuilt via {@code Utils.getDeepCopy(..., mutable=false)}, which wraps
 * nested lists (e.g. "permissions") in {@code Collections.unmodifiableCollection(...)} rather than
 * {@code Collections.unmodifiableList(...)} - an object that implements {@code Collection} but not
 * {@code List}, which a naive {@code (List<...>) ...} cast would throw a {@code ClassCastException}
 * on. {@link Permissions}/{@link Roles} guard against that defensively regardless of which read
 * path is in use (see their {@code instanceof Collection} checks).
 *
 * <p>This plugin is a plain (non-multi) {@code RuleBasedAuthorizationPlugin}, so the {@code scheme}
 * path segment is ignored server-side; "basic" is used here purely by convention. See {@link
 * MultiAuthUsersAndRolesApiCloudTest} for coverage of the scheme actually being honored under
 * {@code MultiAuthPlugin}/{@code MultiAuthRuleBasedAuthorizationPlugin}.
 */
public class SecurityV2ApiCloudTest extends SolrCloudTestCase {

  private static final String SCHEME = "basic";

  @Before
  public void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SecurityJson.SIMPLE)
        .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    cluster.shutdown();
  }

  private static <T extends org.apache.solr.client.solrj.SolrRequest<?>> T authed(T request) {
    request.setBasicAuthCredentials(SecurityJson.USER, SecurityJson.PASS);
    return request;
  }

  @Test
  public void testPermissionsReadFreshAfterWrite() throws Exception {
    var client = cluster.getSolrClient();

    var create = authed(new AuthorizationApi.CreatePermission());
    create.setName("read");
    create.setRole(List.of("admin"));
    CreatePermissionResponse createResponse = create.process(client);
    int index = createResponse.index;

    // Reads fresh - see the class javadoc - so this is expected to see the create above
    // immediately, with no propagation delay.
    ListPermissionsResponse afterCreate =
        authed(new AuthorizationApi.ListPermissions()).process(client);
    assertTrue(
        afterCreate.permissions.stream().anyMatch(p -> Integer.valueOf(index).equals(p.index)));

    var update = authed(new AuthorizationApi.UpdatePermission(index));
    update.setRole(List.of("admin", "dev"));
    update.process(client);

    authed(new AuthorizationApi.DeletePermission(index)).process(client);

    // Reads fresh - see the class javadoc - so this is expected to see the delete immediately,
    // with no propagation delay or polling required.
    ListPermissionsResponse afterDelete =
        authed(new AuthorizationApi.ListPermissions()).process(client);
    assertTrue(
        "permission " + index + " was not removed",
        afterDelete.permissions.stream().noneMatch(p -> Integer.valueOf(index).equals(p.index)));
  }

  @Test
  public void testUserRolesReadFreshAfterWrite() throws Exception {
    var client = cluster.getSolrClient();

    var setRoles = authed(new AuthorizationApi.SetUserRoles(SCHEME, "harry"));
    setRoles.setRoles(List.of("dev"));
    setRoles.process(client);

    // Reads fresh - see the class javadoc - so this is expected to see the write above
    // immediately, with no propagation delay.
    GetUserRolesResponse roles =
        authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry")).process(client);
    assertEquals(List.of("dev"), roles.roles);

    // The bulk listing reads the same fresh path as the single-user GET above.
    ListUserRolesResponse allRoles =
        authed(new AuthorizationApi.ListUserRoles(SCHEME)).process(client);
    assertEquals(List.of("dev"), allRoles.userRoles.get("harry"));

    authed(new AuthorizationApi.DeleteUserRoles(SCHEME, "harry")).process(client);

    roles = authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry")).process(client);
    assertTrue(roles.roles.isEmpty());
  }
}
