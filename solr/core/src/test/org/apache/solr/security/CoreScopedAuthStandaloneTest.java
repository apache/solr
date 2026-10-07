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
import java.util.Locale;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;
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
  private static final String CORE_3 = "collection3";

  private static final String READER_USER = "reader";
  private static final String READER_PASS = "ReaderPass123";
  private static final String OTHER_USER = "other";
  private static final String OTHER_PASS = "OtherPass123";
  private static final String STAR_USER = "star";
  private static final String STAR_PASS = "StarPass123";

  private static JettySolrRunner jetty;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path homeDir = createTempDir("corescoped-auth").toAbsolutePath();
    Path confSrc = SolrTestCaseJ4.configset("_default");
    assertTrue("configset not found at " + confSrc, Files.isDirectory(confSrc));
    for (String core : List.of(CORE_1, CORE_2, CORE_3)) {
      Path coreDir = homeDir.resolve(core);
      copyDir(confSrc, coreDir.resolve("conf"));
      Files.writeString(coreDir.resolve("core.properties"), "name=" + core + "\n");
    }

    // each scoped role may read only the core named in its permission; the star role holds a
    // wildcard permission alongside the scoped ones
    String securityJsonTemplate =
        """
        {
          "authentication": {
            "class": "solr.BasicAuthPlugin",
            "credentials": {"%s": "%s", "%s": "%s", "%s": "%s"}
          },
          "authorization": {
            "class": "solr.RuleBasedAuthorizationPlugin",
            "user-role": {"%s": "reader-role", "%s": "other-role", "%s": "star-role"},
            "permissions": [
              {"name": "read", "role": "reader-role", "collection": "%s"},
              {"name": "read", "role": "other-role", "collection": "%s"},
              {"name": "read", "role": "star-role", "collection": "*"}
            ]
          }
        }
        """;
    String securityJson =
        String.format(
            Locale.ROOT,
            securityJsonTemplate,
            READER_USER,
            Sha256AuthenticationProvider.getSaltedHashedValue(READER_PASS),
            OTHER_USER,
            Sha256AuthenticationProvider.getSaltedHashedValue(OTHER_PASS),
            STAR_USER,
            Sha256AuthenticationProvider.getSaltedHashedValue(STAR_PASS),
            READER_USER,
            OTHER_USER,
            STAR_USER,
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

  private static NamedList<Object> queryV2As(String user, String pass, String core)
      throws Exception {
    GenericV2SolrRequest req =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.GET, "/cores/" + core + "/select", new SolrQuery("*:*"));
    if (user != null) {
      req.setBasicAuthCredentials(user, pass);
    }
    return req.process(jetty.getSolrClient()).getResponse();
  }

  private static void assertV2Allowed(String user, String pass, String core) throws Exception {
    NamedList<Object> rsp = queryV2As(user, pass, core);
    NamedList<?> header = (NamedList<?>) rsp.get("responseHeader");
    assertNotNull("v2 response for " + core + " has no responseHeader", header);
    assertEquals(user + " on v2 " + core, 0, header.get("status"));
  }

  private static void assertV2Denied(String user, String pass, String core, int expectedCode) {
    RemoteSolrException e =
        expectThrows(RemoteSolrException.class, () -> queryV2As(user, pass, core));
    assertEquals(user + " on v2 " + core, expectedCode, e.code());
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

  @Test
  public void testV2RequestUsesCoreScopedRules() throws Exception {
    assertV2Allowed(READER_USER, READER_PASS, CORE_1);
    assertV2Denied(OTHER_USER, OTHER_PASS, CORE_1, 403);
    assertV2Denied(READER_USER, READER_PASS, CORE_2, 403);
  }

  @Test
  public void testWildcardRuleAppliesOnlyWhereNoScopedRuleGoverns() throws Exception {
    // a scoped rule governs its core on its own; the wildcard rule adds nothing there
    assertDenied(STAR_USER, STAR_PASS, CORE_1, 403);
    assertDenied(STAR_USER, STAR_PASS, CORE_2, 403);
    // on a core no scoped rule governs, the wildcard rule is the one that decides
    assertEquals(0, queryAs(STAR_USER, STAR_PASS, CORE_3).getStatus());
    assertDenied(READER_USER, READER_PASS, CORE_3, 403);
  }
}
