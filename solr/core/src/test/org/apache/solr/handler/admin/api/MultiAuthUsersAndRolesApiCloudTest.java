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

import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import java.util.List;
import java.util.Map;
import org.apache.solr.client.api.model.GetUserRolesResponse;
import org.apache.solr.client.api.model.ListUserRolesResponse;
import org.apache.solr.client.api.model.ListUsersResponse;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.AuthenticationApi;
import org.apache.solr.client.solrj.request.AuthorizationApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.MultiAuthPlugin;
import org.apache.solr.security.MultiAuthRuleBasedAuthorizationPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Proves the {@code scheme} path segment on {@link Users}/{@link Roles} actually reaches the right
 * sub-plugin under {@link MultiAuthPlugin}/{@link MultiAuthRuleBasedAuthorizationPlugin} - two
 * configured schemes ("basic" and "other", both real {@code BasicAuthPlugin}/{@code
 * RuleBasedAuthorizationPlugin} instances) must stay fully isolated from each other: writing to one
 * scheme's users/roles must not appear under the other.
 */
public class MultiAuthUsersAndRolesApiCloudTest extends SolrCloudTestCase {

  private static final String ADMIN_USER = "solr";
  private static final String ADMIN_PASS = "SolrRocks";
  private static final String SEED_USER = "seed";
  private static final String SEED_PASS = "SeedPass123";

  private static final String SECURITY_JSON =
      Utils.toJSONString(
          Map.of(
              "authentication",
              Map.of(
                  "class",
                  MultiAuthPlugin.class.getName(),
                  "schemes",
                  List.of(
                      Map.of(
                          "scheme",
                          "basic",
                          "class",
                          "solr.BasicAuthPlugin",
                          "blockUnknown",
                          true,
                          "credentials",
                          Map.of(ADMIN_USER, getSaltedHashedValue(ADMIN_PASS))),
                      Map.of(
                          "scheme",
                          "other",
                          "class",
                          "solr.BasicAuthPlugin",
                          "blockUnknown",
                          true,
                          "credentials",
                          Map.of(SEED_USER, getSaltedHashedValue(SEED_PASS))))),
              "authorization",
              Map.of(
                  "class",
                  MultiAuthRuleBasedAuthorizationPlugin.class.getName(),
                  "schemes",
                  List.of(
                      Map.of(
                          "scheme",
                          "basic",
                          "class",
                          "solr.RuleBasedAuthorizationPlugin",
                          "user-role",
                          Map.of(ADMIN_USER, List.of("admin"))),
                      Map.of(
                          "scheme",
                          "other",
                          "class",
                          "solr.RuleBasedAuthorizationPlugin",
                          "user-role",
                          Map.of())),
                  "permissions",
                  List.of(Map.of("name", "all", "role", "admin")))));

  @Before
  public void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
  }

  @After
  public void tearDownCluster() throws Exception {
    cluster.shutdown();
  }

  private static <T extends SolrRequest<?>> T asAdmin(T request) {
    request.setBasicAuthCredentials(ADMIN_USER, ADMIN_PASS);
    return request;
  }

  @Test
  public void testUsersAreIsolatedPerScheme() throws Exception {
    var client = cluster.getSolrClient();

    ListUsersResponse basicUsers =
        asAdmin(new AuthenticationApi.ListUsers("basic")).process(client);
    assertEquals(List.of(ADMIN_USER), basicUsers.users);

    ListUsersResponse otherUsers =
        asAdmin(new AuthenticationApi.ListUsers("other")).process(client);
    assertEquals(List.of(SEED_USER), otherUsers.users);

    // Create a user under the "other" scheme only.
    var create = asAdmin(new AuthenticationApi.CreateOrUpdateUser("other", "newuser"));
    create.setPassword("NewUserPass123");
    create.process(client);

    otherUsers = asAdmin(new AuthenticationApi.ListUsers("other")).process(client);
    assertEquals(List.of("newuser", SEED_USER), sorted(otherUsers.users));

    // "basic" scheme is untouched.
    basicUsers = asAdmin(new AuthenticationApi.ListUsers("basic")).process(client);
    assertEquals(List.of(ADMIN_USER), basicUsers.users);

    asAdmin(new AuthenticationApi.DeleteUser("other", "newuser")).process(client);
    otherUsers = asAdmin(new AuthenticationApi.ListUsers("other")).process(client);
    assertEquals(List.of(SEED_USER), otherUsers.users);
  }

  @Test
  public void testRolesAreIsolatedPerScheme() throws Exception {
    var client = cluster.getSolrClient();

    var setRoles = asAdmin(new AuthorizationApi.SetUserRoles("other", SEED_USER));
    setRoles.setRoles(List.of("dev"));
    setRoles.process(client);

    GetUserRolesResponse otherRoles =
        asAdmin(new AuthorizationApi.GetUserRoles("other", SEED_USER)).process(client);
    assertEquals(List.of("dev"), otherRoles.roles);

    // Same username looked up under "basic" is unaffected - "seed" isn't even a basic-scheme user.
    GetUserRolesResponse basicRoles =
        asAdmin(new AuthorizationApi.GetUserRoles("basic", SEED_USER)).process(client);
    assertTrue(basicRoles.roles.isEmpty());

    // The bulk listing is scheme-isolated the same way: "seed"/"dev" only shows up under "other".
    ListUserRolesResponse otherList =
        asAdmin(new AuthorizationApi.ListUserRoles("other")).process(client);
    assertEquals(List.of("dev"), otherList.userRoles.get(SEED_USER));

    ListUserRolesResponse basicList =
        asAdmin(new AuthorizationApi.ListUserRoles("basic")).process(client);
    assertFalse(basicList.userRoles.containsKey(SEED_USER));

    asAdmin(new AuthorizationApi.DeleteUserRoles("other", SEED_USER)).process(client);
    otherRoles = asAdmin(new AuthorizationApi.GetUserRoles("other", SEED_USER)).process(client);
    assertTrue(otherRoles.roles.isEmpty());
  }

  private static List<String> sorted(List<String> values) {
    return values.stream().sorted().toList();
  }
}
