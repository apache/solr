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
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that core-scoped authorization rules work in standalone mode (SOLR-13097). In standalone
 * mode there are no collections, so the authorization context must carry the serving core's name
 * for rules scoped via the "collection" field to match.
 */
public class CoreScopedAuthStandaloneTest extends SolrTestCase {

  private static JettySolrRunner jetty;

  private static final String READER_USER = "reader";
  private static final String READER_PASS = "ReaderPass123";
  private static final String OTHER_USER = "other";
  private static final String OTHER_PASS = "OtherPass123";

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path homeDir = createTempDir("corescoped-auth").toAbsolutePath();
    // Create the collection1 core by copying the _default configset; Jetty auto-discovers it.
    Path confSrc = Path.of("../../../src/test-files/solr/configsets/_default/conf");
    assertTrue("configset not found at " + confSrc.toAbsolutePath(), Files.isDirectory(confSrc));
    Path coreDir = homeDir.resolve("collection1");
    copyDir(confSrc, coreDir.resolve("conf"));
    Files.writeString(coreDir.resolve("core.properties"), "name=collection1\n");
    // Write security.json before startup: basic auth plus a read permission scoped to the
    // "collection1" core, granted only to the "corereader" role.
    String securityJson =
        "{"
            + "'authentication':{"
            + "  'class':'solr.BasicAuthPlugin',"
            + "  'credentials':{"
            + "    '"
            + READER_USER
            + "':'"
            + Sha256AuthenticationProvider.getSaltedHashedValue(READER_PASS)
            + "',"
            + "    '"
            + OTHER_USER
            + "':'"
            + Sha256AuthenticationProvider.getSaltedHashedValue(OTHER_PASS)
            + "'"
            + "  }},"
            + "'authorization':{"
            + "  'class':'solr.RuleBasedAuthorizationPlugin',"
            + "  'user-role':{'"
            + READER_USER
            + "':'corereader','"
            + OTHER_USER
            + "':'otherrole'},"
            + "  'permissions':[{'name':'read','role':'corereader','collection':'collection1'}]"
            + "}}";
    Files.writeString(
        homeDir.resolve("security.json"), securityJson.replace('\'', '"'), StandardCharsets.UTF_8);
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

  private QueryResponse queryAs(String user, String pass) throws Exception {
    SolrClient client = jetty.getSolrClient();
    QueryRequest req = new QueryRequest(new SolrQuery("*:*"));
    req.setBasicAuthCredentials(user, pass);
    return req.process(client, "collection1");
  }

  @Test
  public void testCoreScopedRuleAllowsConfiguredRole() throws Exception {
    QueryResponse rsp = queryAs(READER_USER, READER_PASS);
    assertEquals(0, rsp.getStatus());
  }

  @Test
  public void testCoreScopedRuleDeniesOtherRole() throws Exception {
    try {
      queryAs(OTHER_USER, OTHER_PASS);
      fail("Expected authorization failure for a role without the core-scoped permission");
    } catch (SolrServerException | RemoteSolrException e) {
      // expected: 403 Forbidden
      assertTrue("Expected 403 but got: " + e.getMessage(), e.getMessage().contains("403"));
    }
  }
}
