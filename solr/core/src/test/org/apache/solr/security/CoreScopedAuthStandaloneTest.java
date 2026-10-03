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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that authorization rules scoped to a core with the "collection" field apply in standalone
 * mode, where the core name stands in for the collection.
 */
public class CoreScopedAuthStandaloneTest extends SolrTestCase {

  private static final String CORE_1 = "collection1";
  private static final String CORE_2 = "collection2";

  private static final String READER_USER = "reader";
  private static final String READER_PASS = "ReaderPass123";
  private static final String OTHER_USER = "other";
  private static final String OTHER_PASS = "OtherPass123";

  private static JettySolrRunner jetty;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path homeDir = createTempDir("corescoped-auth").toAbsolutePath();
    Path confSrc = SolrTestCaseJ4.configset("_default");
    assertTrue("configset not found at " + confSrc, Files.isDirectory(confSrc));
    for (String core : List.of(CORE_1, CORE_2)) {
      Path coreDir = homeDir.resolve(core);
      copyDir(confSrc, coreDir.resolve("conf"));
      Files.writeString(coreDir.resolve("core.properties"), "name=" + core + "\n");
    }

    // each role may read only the core named in its permission
    String securityJson =
        """
        {
          "authentication": {
            "class": "solr.BasicAuthPlugin",
            "credentials": {"%s": "%s", "%s": "%s"}
          },
          "authorization": {
            "class": "solr.RuleBasedAuthorizationPlugin",
            "user-role": {"%s": "reader-role", "%s": "other-role"},
            "permissions": [
              {"name": "read", "role": "reader-role", "collection": "%s"},
              {"name": "read", "role": "other-role", "collection": "%s"}
            ]
          }
        }
        """
            .formatted(
                READER_USER,
                Sha256AuthenticationProvider.getSaltedHashedValue(READER_PASS),
                OTHER_USER,
                Sha256AuthenticationProvider.getSaltedHashedValue(OTHER_PASS),
                READER_USER,
                OTHER_USER,
                CORE_1,
                CORE_2);
    Files.writeString(homeDir.resolve("security.json"), securityJson, StandardCharsets.UTF_8);
    jetty = new JettySolrRunner(homeDir.toString(), 0);
    jetty.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (jetty != null) {
      jetty.stop();
      jetty = null;
    }
  }

  private static void copyDir(Path src, Path dest) throws Exception {
    try (var stream = Files.walk(src)) {
      for (Path p : (Iterable<Path>) stream::iterator) {
        Path target = dest.resolve(src.relativize(p).toString());
        if (Files.isDirectory(p)) {
          Files.createDirectories(target);
        } else {
          Files.copy(p, target);
        }
      }
    }
  }

  private static QueryResponse queryAs(String user, String pass, String core) throws Exception {
    QueryRequest req = new QueryRequest(new SolrQuery("*:*"));
    if (user != null) {
      req.setBasicAuthCredentials(user, pass);
    }
    return req.process(jetty.getSolrClient(), core);
  }

  private static void assertDenied(String user, String pass, String core, int expectedCode) {
    RemoteSolrException e =
        expectThrows(RemoteSolrException.class, () -> queryAs(user, pass, core));
    assertEquals(user + " on " + core, expectedCode, e.code());
  }

  @Test
  public void testScopedRuleAllowsItsRoleOnItsCore() throws Exception {
    assertEquals(0, queryAs(READER_USER, READER_PASS, CORE_1).getStatus());
    assertEquals(0, queryAs(OTHER_USER, OTHER_PASS, CORE_2).getStatus());
  }

  @Test
  public void testScopedRuleDeniesOtherRolesOnTheCore() {
    assertDenied(OTHER_USER, OTHER_PASS, CORE_1, 403);
    assertDenied(READER_USER, READER_PASS, CORE_2, 403);
  }

  @Test
  public void testRequestWithoutCredentialsIsRejected() {
    assertDenied(null, null, CORE_1, 401);
  }
}
