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
import static org.apache.solr.servlet.NodeTargetProxyFilter.TARGET_NODE_HEADER;

import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests {@link NodeTargetProxyFilter}. */
@SolrTestCaseJ4.SuppressSSL
public class NodeTargetProxyFilterTest extends SolrCloudTestCase {

  private static JettySolrRunner receivingNode;
  private static JettySolrRunner targetNode;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    receivingNode = cluster.getJettySolrRunner(0);
    targetNode = cluster.getJettySolrRunner(1);
  }

  @Test
  public void testHeaderProxiesToTargetNode() throws Exception {
    HttpURLConnection conn = open(receivingNode.getBaseUrl() + "/admin/info/system?wt=json");
    conn.setRequestProperty(TARGET_NODE_HEADER, targetNode.getNodeName());
    assertEquals(targetNode.getNodeName(), respondingNode(conn));
  }

  @Test
  public void testHeaderProxiesV2() throws Exception {
    HttpURLConnection conn = open(receivingNode.getBaseURLV2() + "/node/system");
    conn.setRequestProperty(TARGET_NODE_HEADER, targetNode.getNodeName());
    assertEquals(targetNode.getNodeName(), respondingNode(conn));
  }

  @Test
  public void testQueryParamProxiesGet() throws Exception {
    HttpURLConnection conn =
        open(
            receivingNode.getBaseUrl()
                + "/admin/info/system?wt=json&"
                + TARGET_NODE_HEADER
                + "="
                + URLEncoder.encode(targetNode.getNodeName(), UTF_8));
    assertEquals(targetNode.getNodeName(), respondingNode(conn));
  }

  @Test
  public void testQueryParamIgnoredForPost() throws Exception {
    HttpURLConnection conn =
        open(
            receivingNode.getBaseUrl()
                + "/admin/info/system?wt=json&"
                + TARGET_NODE_HEADER
                + "="
                + URLEncoder.encode(targetNode.getNodeName(), UTF_8));
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setFixedLengthStreamingMode(0);
    conn.getOutputStream().close();
    assertEquals(receivingNode.getNodeName(), respondingNode(conn));
  }

  @Test
  public void testTargetingSelfIsHandledLocally() throws Exception {
    HttpURLConnection conn = open(receivingNode.getBaseUrl() + "/admin/info/system?wt=json");
    conn.setRequestProperty(TARGET_NODE_HEADER, receivingNode.getNodeName());
    assertEquals(receivingNode.getNodeName(), respondingNode(conn));
  }

  @Test
  public void testUnknownNodeIs400() throws Exception {
    HttpURLConnection conn = open(receivingNode.getBaseUrl() + "/admin/info/system?wt=json");
    conn.setRequestProperty(TARGET_NODE_HEADER, "no.such.host:1234_solr");
    assertEquals(400, conn.getResponseCode());
    conn.disconnect();
  }

  @Test
  public void testResolvePeerShorthand() {
    String own = "myhost-3.mydomain.com:3000_solr";
    assertEquals(
        "otherhost.mydomain.com:3000_solr",
        NodeTargetProxyFilter.resolvePeerShorthand("otherhost", own));
    assertEquals(
        "otherhost.elsewhere.com:8983_solr",
        NodeTargetProxyFilter.resolvePeerShorthand("otherhost.elsewhere.com:8983_solr", own));
    assertEquals(
        "otherhost:3000_solr",
        NodeTargetProxyFilter.resolvePeerShorthand("otherhost", "myhost:3000_solr"));
    // complete node names with dotless hosts are preserved
    assertEquals(
        "solr2:8983_solr",
        NodeTargetProxyFilter.resolvePeerShorthand("solr2:8983_solr", "solr1:8983_solr"));
  }

  private static HttpURLConnection open(String url) throws Exception {
    return (HttpURLConnection) URI.create(url).toURL().openConnection();
  }

  /** Returns the "node" field from a system-info response. */
  private static String respondingNode(HttpURLConnection conn) throws Exception {
    assertEquals(200, conn.getResponseCode());
    String body = new String(conn.getInputStream().readAllBytes(), UTF_8);
    conn.disconnect();
    return (String) ((Map<?, ?>) Utils.fromJSONString(body)).get("node");
  }
}
