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
 * SolrCloud-mode coverage for {@link Permissions}/{@link Roles}, exercising {@code
 * SecurityConfHandlerZk#getSecurityConfig(false)} - the cached read path backed by {@code
 * ZkStateReader}'s security-node watcher.
 *
 * <p>Unlike the initial (fresh) load, once that watcher's callback has fired at least once, it
 * rebuilds its cached snapshot via {@code Utils.getDeepCopy(..., mutable=false)}, which wraps
 * nested lists (e.g. "permissions") in {@code Collections.unmodifiableCollection(...)} rather than
 * {@code Collections.unmodifiableList(...)} - an object that implements {@code Collection} but not
 * {@code List}. Standalone mode ({@code SecurityConfHandlerLocal}) always re-reads security.json
 * fresh from disk and never exhibits this, so this gap needs cloud coverage specifically. Creating
 * a permission below forces a real ZK write, which trips the watcher and populates the cached,
 * wrapped snapshot before the following read.
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
  public void testPermissionsSurviveCachedZkRead() throws Exception {
    var client = cluster.getSolrClient();

    var create = authed(new AuthorizationApi.CreatePermission());
    create.setName("read");
    create.setRole(List.of("admin"));
    CreatePermissionResponse createResponse = create.process(client);
    int index = createResponse.index;

    // This first list forces SecurityConfHandlerZk's cached (getFresh=false) read path, now that
    // the create above has tripped the ZK security-node watcher at least once.
    ListPermissionsResponse afterCreate =
        authed(new AuthorizationApi.ListPermissions()).process(client);
    assertTrue(
        afterCreate.permissions.stream().anyMatch(p -> Integer.valueOf(index).equals(p.index)));

    var update = authed(new AuthorizationApi.UpdatePermission(index));
    update.setRole(List.of("admin", "dev"));
    update.process(client);

    authed(new AuthorizationApi.DeletePermission(index)).process(client);

    // security.json updates propagate to this node's ZK watcher asynchronously, so poll briefly
    // rather than asserting on the very next read.
    boolean deleted = false;
    for (int i = 0; i < 20 && !deleted; i++) {
      ListPermissionsResponse afterDelete =
          authed(new AuthorizationApi.ListPermissions()).process(client);
      deleted =
          afterDelete.permissions.stream().noneMatch(p -> Integer.valueOf(index).equals(p.index));
      if (!deleted) {
        Thread.sleep(100);
      }
    }
    assertTrue("permission " + index + " was not removed", deleted);
  }

  @Test
  public void testUserRolesSurviveCachedZkRead() throws Exception {
    var client = cluster.getSolrClient();

    // Force at least one ZK write/watch-fire cycle before reading roles back.
    var setRoles = authed(new AuthorizationApi.SetUserRoles(SCHEME, "harry"));
    setRoles.setRoles(List.of("dev"));
    setRoles.process(client);

    GetUserRolesResponse roles =
        authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry")).process(client);
    assertEquals(List.of("dev"), roles.roles);

    // The bulk listing reads the same cached-ZK path as the single-user GET above.
    ListUserRolesResponse allRoles =
        authed(new AuthorizationApi.ListUserRoles(SCHEME)).process(client);
    assertEquals(List.of("dev"), allRoles.userRoles.get("harry"));

    authed(new AuthorizationApi.DeleteUserRoles(SCHEME, "harry")).process(client);

    roles = authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry")).process(client);
    assertTrue(roles.roles.isEmpty());
  }
}
