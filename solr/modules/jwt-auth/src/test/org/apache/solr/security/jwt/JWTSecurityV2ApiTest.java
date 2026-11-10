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
package org.apache.solr.security.jwt;

import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;
import static org.apache.solr.security.jwt.JWTAuthPluginTest.JWT_TEST_PATH;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.MultiAuthPlugin;
import org.apache.solr.security.MultiAuthRuleBasedAuthorizationPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

/** The v2 users and roles APIs refuse a JWT scheme, whose users and roles live in the token. */
public class JWTSecurityV2ApiTest extends SolrCloudTestCase {
  private static final String USER = "solr";
  private static final String PASS = "SolrRocks";

  @BeforeClass
  @SuppressWarnings("unchecked")
  public static void setupCluster() throws Exception {
    Map<String, Object> jwtFixture =
        (Map<String, Object>)
            Utils.fromJSONString(
                Files.readString(
                    JWT_TEST_PATH()
                        .resolve("security")
                        .resolve("jwt_plugin_jwk_security_with_authorization.json"),
                    StandardCharsets.UTF_8));
    Map<String, Object> bearer =
        new HashMap<>((Map<String, Object>) jwtFixture.get("authentication"));
    bearer.put("scheme", "bearer");
    bearer.put("blockUnknown", false);

    String securityJson =
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
                            Map.of(USER, getSaltedHashedValue(PASS))),
                        bearer)),
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
                            Map.of(USER, List.of("admin"))),
                        Map.of(
                            "scheme",
                            "bearer",
                            "class",
                            "solr.ExternalRoleRuleBasedAuthorizationPlugin",
                            "rolesClaim",
                            "roles")),
                    "permissions",
                    List.of(Map.of("name", "all", "role", "admin")))));

    configureCluster(1).withSecurityJson(securityJson).configure();
  }

  private static <T extends SolrRequest<?>> T asAdmin(T request) {
    request.setBasicAuthCredentials(USER, PASS);
    return request;
  }

  private static int putStatus(String path, String json) {
    var put =
        asAdmin(new GenericV2SolrRequest(SolrRequest.METHOD.PUT, path))
            .withContent(json.getBytes(StandardCharsets.UTF_8), "application/json");
    return expectThrows(RemoteSolrException.class, () -> put.process(cluster.getSolrClient()))
        .code();
  }

  private static Object v1Config(String key) throws Exception {
    NamedList<Object> rsp =
        asAdmin(new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/" + key))
            .process(cluster.getSolrClient())
            .getResponse();
    return rsp.get(key);
  }

  @Test
  public void testJwtSchemeHasNoUsersOrRoles() throws Exception {
    Object authcBefore = v1Config("authentication");
    Object authzBefore = v1Config("authorization");

    assertEquals(
        404,
        putStatus(
            "/cluster/security/authentication/bearer/users/bob", "{\"password\": \"BobIsCool\"}"));
    assertEquals(
        404,
        putStatus(
            "/cluster/security/authorization/bearer/users/bob/roles", "{\"roles\": [\"admin\"]}"));

    assertEquals(authcBefore, v1Config("authentication"));
    assertEquals(authzBefore, v1Config("authorization"));
  }
}
