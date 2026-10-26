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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.AuthorizationApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests that security API reads observe completed writes without polling in SolrCloud. Standalone
 * mode always reads from disk, so it cannot exercise the ZooKeeper cache behavior.
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

  private static <T extends SolrRequest<?>> T authed(T request) {
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

    ListPermissionsResponse afterCreate =
        authed(new AuthorizationApi.ListPermissions()).process(client);
    assertTrue(
        afterCreate.permissions.stream().anyMatch(p -> Integer.valueOf(index).equals(p.index)));

    var update = authed(new AuthorizationApi.UpdatePermission(index));
    update.setRole(List.of("admin", "dev"));
    update.process(client);

    authed(new AuthorizationApi.DeletePermission(index)).process(client);

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

    GetUserRolesResponse roles =
        authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry")).process(client);
    assertEquals(List.of("dev"), roles.roles);

    ListUserRolesResponse allRoles =
        authed(new AuthorizationApi.ListUserRoles(SCHEME)).process(client);
    assertEquals(List.of("dev"), allRoles.userRoles.get("harry"));

    authed(new AuthorizationApi.DeleteUserRoles(SCHEME, "harry")).process(client);

    roles = authed(new AuthorizationApi.GetUserRoles(SCHEME, "harry")).process(client);
    assertTrue(roles.roles.isEmpty());
  }
}
