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

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * In SolrCloud, a request is authorized against the collection of the core that serves it, not the
 * {@code collection} request parameter. Collections reached from there (distributed search) are
 * authorized by the receiving nodes.
 */
@SolrTestCaseJ4.SuppressSSL
public class RuleBasedAuthorizationCoreCollectionTest extends SolrCloudAuthTestCase {

  private static final String PUBLIC_COLL = "publicColl";
  private static final String PRIVATE_COLL = "privateColl";
  private static final String BOTH_ALIAS = "bothAlias";
  // Indexed into PRIVATE_COLL; asserting its absence from a response body is independent of status
  private static final String PRIVATE_DOC_MARKER = "marker_8f3a2c";

  private static final String PASSWORD = "SolrRocks";
  private static final String ADMIN = "solr";
  private static final String PUBLIC_READER = "publicReader";
  private static final String BOTH_READER = "bothReader";

  // Every user's password is 'SolrRocks', same hash as BasicAuthIntegrationTest.
  private static final String PWD_HASH =
      "orwp2Ghgj39lmnrZOTm7Qtre1VqHFDfwAEzr0ApbN3Y= Ju5osoAqOX8iafhWpPP01E5P+sg8tK8tHON7rCYZRRw=";
  private static final String SECURITY_JSON =
      ("{"
              + "  'authentication':{"
              + "    'blockUnknown':'true',"
              + "    'class':'solr.BasicAuthPlugin',"
              + "    'credentials':{"
              + "      'solr':'PWD', 'publicReader':'PWD', 'bothReader':'PWD'"
              + "    }"
              + "  },"
              + "  'authorization':{"
              + "    'class':'solr.RuleBasedAuthorizationPlugin',"
              + "    'user-role':{"
              + "      'solr':'admin',"
              + "      'publicReader':'publicRead',"
              + "      'bothReader':['publicRead','privateRead']"
              + "    },"
              + "    'permissions':["
              + "      {'name':'read', 'collection':'publicColl', 'role':['publicRead','admin']},"
              + "      {'name':'read', 'collection':'privateColl', 'role':['privateRead','admin']},"
              + "      {'name':'all', 'role':'admin'}"
              + "    ]"
              + "  }"
              + "}")
          .replace("PWD", PWD_HASH)
          .replace('\'', '"');

  @Before
  public void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
    for (String coll : new String[] {PUBLIC_COLL, PRIVATE_COLL}) {
      CollectionAdminRequest.createCollection(coll, "conf", 1, 1)
          .setBasicAuthCredentials(ADMIN, PASSWORD)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(coll, 1, 1);
    }
    CollectionAdminRequest.createAlias(BOTH_ALIAS, PUBLIC_COLL + "," + PRIVATE_COLL)
        .setBasicAuthCredentials(ADMIN, PASSWORD)
        .process(cluster.getSolrClient());

    addDoc(PUBLIC_COLL, "pub-1", "public");
    addDoc(PRIVATE_COLL, "priv-1", PRIVATE_DOC_MARKER);
  }

  @After
  public void tearDownCluster() throws Exception {
    shutdownCluster();
  }

  private void addDoc(String collection, String id, String title) throws Exception {
    SolrInputDocument doc = new SolrInputDocument();
    doc.addField("id", id);
    doc.addField("title_s", title);
    UpdateRequest update = new UpdateRequest();
    update.add(doc).setBasicAuthCredentials(ADMIN, PASSWORD);
    update.commit(cluster.getSolrClient(), collection);
  }

  @Test
  public void testSearchAcrossPermittedCollections() throws Exception {
    Response rsp =
        request(
            "/" + coreName(PUBLIC_COLL) + "/select?collection=" + BOTH_ALIAS + "&q=*:*",
            BOTH_READER);
    assertEquals(rsp.body, 200, rsp.status);
    assertTrue(rsp.body, rsp.body.contains(PRIVATE_DOC_MARKER));

    for (String collection : new String[] {BOTH_ALIAS, PUBLIC_COLL + "," + PRIVATE_COLL}) {
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("q", "*:*");
      params.set("collection", collection);
      QueryRequest query = new QueryRequest(params, SolrRequest.METHOD.GET);
      query.setBasicAuthCredentials(BOTH_READER, PASSWORD);
      assertEquals(
          collection,
          2,
          query.process(cluster.getSolrClient(), PUBLIC_COLL).getResults().getNumFound());
    }
  }

  @Test
  public void testCoordinatorAuthorizesTargetCollection() throws Exception {
    JettySolrRunner coordinator;
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "coordinator:on");
    try {
      coordinator = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }
    String baseUrl = coordinator.getBaseUrl().toString();

    Response pub = request(baseUrl, "/" + PUBLIC_COLL + "/select?q=*:*", PUBLIC_READER);
    assertEquals(pub.body, 200, pub.status);
    assertRefused(request(baseUrl, "/" + PRIVATE_COLL + "/select?q=*:*", PUBLIC_READER));
    Response priv = request(baseUrl, "/" + PRIVATE_COLL + "/select?q=*:*", BOTH_READER);
    assertEquals(priv.body, 200, priv.status);
    assertTrue(priv.body, priv.body.contains(PRIVATE_DOC_MARKER));
  }

  private static void assertRefused(Response rsp) {
    assertFalse(rsp.body, rsp.body.contains(PRIVATE_DOC_MARKER));
    assertTrue(
        "Expected 401/403, got " + rsp.status + ": " + rsp.body,
        rsp.status == HttpURLConnection.HTTP_UNAUTHORIZED
            || rsp.status == HttpURLConnection.HTTP_FORBIDDEN);
  }

  private String coreName(String collection) {
    return cluster.getJettySolrRunner(0).getCoreContainer().getAllCoreNames().stream()
        .filter(n -> n.startsWith(collection + "_"))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no core of " + collection + " on node 0"));
  }

  private record Response(int status, String body) {}

  private Response request(String pathAndQuery, String user) throws Exception {
    return request(cluster.getJettySolrRunner(0).getBaseUrl().toString(), pathAndQuery, user);
  }

  private static Response request(String baseUrl, String pathAndQuery, String user)
      throws Exception {
    URI uri = URI.create(baseUrl + pathAndQuery + "&wt=json");
    HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
    try {
      String credentials = user + ":" + PASSWORD;
      conn.setRequestProperty(
          "Authorization",
          "Basic "
              + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8)));
      int status = conn.getResponseCode();
      InputStream in = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
      String rspBody = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      return new Response(status, rspBody);
    } finally {
      conn.disconnect();
    }
  }
}
