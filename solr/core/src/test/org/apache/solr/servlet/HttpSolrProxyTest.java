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

package org.apache.solr.servlet;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.HttpURLConnection;
import java.net.URI;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests {@link HttpSolrProxy}, the internode proxy used when a request lands on a node that does
 * not host a replica of the target collection. Verifies that a request body with a known length is
 * forwarded successfully and that the origin's {@code Content-Length} is preserved on the response.
 */
@SolrTestCaseJ4.SuppressSSL
public class HttpSolrProxyTest extends SolrCloudTestCase {
  private static final String COLLECTION = "proxycoll";

  private static JettySolrRunner owningNode;
  private static JettySolrRunner proxyingNode;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();

    // Single shard + replica, so exactly one node hosts it; the other must proxy.
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

    Replica replica =
        cluster.getSolrClient().getClusterState().getCollection(COLLECTION).getReplicas().get(0);
    for (JettySolrRunner runner : cluster.getJettySolrRunners()) {
      if (runner.getNodeName().equals(replica.getNodeName())) {
        owningNode = runner;
      } else {
        proxyingNode = runner;
      }
    }
    assertNotNull(owningNode);
    assertNotNull(proxyingNode);
  }

  /**
   * A POST with a body proxied to a node that doesn't host the replica must succeed. Jetty 12.1
   * rejects (400) a forwarded Content-Length that contradicts the client's own framing, so the
   * proxy must convey the known length via the request body content provider instead.
   */
  @Test
  public void testProxiedPostWithBodySucceeds() throws Exception {
    String body = "[{\"id\":\"1\"}]";
    HttpURLConnection conn = open(proxyingNode, "/" + COLLECTION + "/update/json?commit=true");
    conn.setRequestMethod("POST");
    conn.setDoOutput(true); // HttpURLConnection then sends a Content-Length for our body
    conn.setRequestProperty("Content-Type", "application/json");
    conn.getOutputStream().write(body.getBytes(UTF_8));
    assertEquals(200, conn.getResponseCode());
    conn.getInputStream().readAllBytes();
    conn.disconnect();
  }

  /**
   * When the origin declares a Content-Length, the proxied response must preserve it (rather than
   * being forced to chunked) and it must exactly match the bytes delivered — otherwise Jetty 12.1
   * would abort the re-streamed response with "too much content written".
   */
  @Test
  public void testProxiedResponsePreservesContentLength() throws Exception {
    String path = "/" + COLLECTION + "/select?q=*:*&rows=0";

    HttpURLConnection direct = open(owningNode, path);
    assertEquals(200, direct.getResponseCode());
    boolean originDeclaresLength = direct.getHeaderField("Content-Length") != null;
    direct.getInputStream().readAllBytes();
    direct.disconnect();

    HttpURLConnection proxied = open(proxyingNode, path);
    assertEquals(200, proxied.getResponseCode());
    String proxiedLen = proxied.getHeaderField("Content-Length");
    int proxiedBodyLen = proxied.getInputStream().readAllBytes().length;
    proxied.disconnect();

    if (originDeclaresLength) {
      assertNotNull("Proxy dropped the origin's Content-Length (forced chunked)", proxiedLen);
    }
    if (proxiedLen != null) {
      assertEquals(
          "Proxied Content-Length must match the bytes delivered",
          proxiedBodyLen,
          Integer.parseInt(proxiedLen));
    }
  }

  private static HttpURLConnection open(JettySolrRunner node, String pathAndQuery)
      throws Exception {
    URI uri = URI.create(node.getBaseUrl().toString() + pathAndQuery);
    return (HttpURLConnection) uri.toURL().openConnection();
  }
}
