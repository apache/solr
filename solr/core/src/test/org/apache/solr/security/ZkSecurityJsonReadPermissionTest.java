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
package org.apache.solr.security;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that a user with only the zk-read permission cannot read security.json through any
 * variant of the v1 or v2 ZooKeeper endpoints, while security-read users still can.
 */
public class ZkSecurityJsonReadPermissionTest extends SolrCloudTestCase {
  private static final String PASSWORD = "pwd";

  /** A key that only appears in the content of security.json */
  private static final String MARKER = "credentials";

  @BeforeClass
  public static void setupCluster() throws Exception {
    String hash = Sha256AuthenticationProvider.getSaltedHashedValue(PASSWORD);
    Map<String, Object> security =
        Map.of(
            "authentication",
            Map.of(
                "blockUnknown",
                true,
                "class",
                "solr.BasicAuthPlugin",
                "credentials",
                Map.of("admin", hash, "zkreader", hash)),
            "authorization",
            Map.of(
                "class",
                "solr.RuleBasedAuthorizationPlugin",
                "permissions",
                List.of(
                    Map.of("name", "zk-read", "role", "zkreader"),
                    Map.of("name", "security-read", "role", "admin"),
                    Map.of("name", "all", "role", "admin")),
                "user-role",
                Map.of("admin", "admin", "zkreader", "zkreader")));
    configureCluster(1).withSecurityJson(Utils.toJSONString(security)).configure();
  }

  @Test
  public void testZkReaderCanReadOtherNodes() throws Exception {
    assertReadable("zkreader", v1("path=/live_nodes&detail=true"));
    assertReadable("zkreader", v2("/data/live_nodes"));
  }

  @Test
  public void testSecurityReaderCanReadSecurityJson() throws Exception {
    assertContainsMarker("admin", v1("path=/security.json&detail=true"));
    assertContainsMarker("admin", v2("/data/security.json"));
  }

  @Test
  public void testZkReaderCannotReadSecurityJsonViaV1() throws Exception {
    for (String query :
        List.of(
            "path=/security.json&detail=true",
            "path=/security.json&dump=true",
            "path=/security.json/&detail=true",
            "path=/security.json//&detail=true",
            "path=%20/security.json&detail=true",
            "path=/security.json%20&detail=true",
            "path=/&dump=true",
            "dump=true")) {
      assertDenied("zkreader", v1(query));
    }
  }

  @Test
  public void testZkReaderCannotReadSecurityJsonViaV2() throws Exception {
    for (String path :
        List.of(
            "/data/security.json",
            "/data/security.json/",
            "/data/security.json//",
            "/data/security%2Ejson",
            "/data/security%2ejson",
            "/data/%73ecurity.json",
            "/data/security.json%20")) {
      assertDenied("zkreader", v2(path));
    }
  }

  private static String v1(String query) {
    return cluster.getJettySolrRunner(0).getBaseUrl() + "/admin/zookeeper?wt=json&" + query;
  }

  private static String v2(String path) {
    return cluster.getJettySolrRunner(0).getBaseURLV2() + "/cluster/zookeeper" + path;
  }

  private static ContentResponse get(String user, String url) throws Exception {
    String credentials =
        Base64.getEncoder()
            .encodeToString((user + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
    return cluster
        .getJettySolrRunner(0)
        .getSolrClient()
        .getHttpClient()
        .newRequest(url)
        .headers(h -> h.put(HttpHeader.AUTHORIZATION, "Basic " + credentials))
        .send();
  }

  private static void assertReadable(String user, String url) throws Exception {
    ContentResponse response = get(user, url);
    assertEquals(url + " -> " + response.getContentAsString(), 200, response.getStatus());
  }

  private static void assertContainsMarker(String user, String url) throws Exception {
    ContentResponse response = get(user, url);
    assertEquals(url, 200, response.getStatus());
    assertTrue(url, response.getContentAsString().contains(MARKER));
  }

  private static void assertDenied(String user, String url) throws Exception {
    ContentResponse response = get(user, url);
    String body = response.getContentAsString();
    assertFalse(url + " leaked security.json: " + body, body.contains(MARKER));
    assertNotEquals(url + " -> " + body, 200, response.getStatus());
  }
}
