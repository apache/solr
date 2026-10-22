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
import java.util.TreeMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests {@link NodeTargetProxyFilter}. */
@SolrTestCaseJ4.SuppressSSL
public class NodeTargetProxyFilterTest extends SolrCloudTestCase {

  private static JettySolrRunner receivingNode;
  private static JettySolrRunner targetNode;

  /** A collection with its only replica on {@link #receivingNode}. */
  private static final String COLLECTION = "coll";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    receivingNode = cluster.getJettySolrRunner(0);
    targetNode = cluster.getJettySolrRunner(1);

    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .setCreateNodeSet(receivingNode.getNodeName())
        .process(cluster.getSolrClient());
    cluster
        .getSolrClient()
        .request(
            new ConfigRequest(
                "{'add-requesthandler': {'name': '/echo', 'class': '"
                    + EchoHandler.class.getName()
                    + "'}}"),
            COLLECTION);
  }

  /** Echoes the headers and params of the HTTP request as seen by the handler. */
  public static class EchoHandler extends RequestHandlerBase {
    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) {
      Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      var httpReq = req.getHttpSolrCall().getReq();
      httpReq
          .getHeaderNames()
          .asIterator()
          .forEachRemaining(name -> headers.put(name, httpReq.getHeader(name)));
      rsp.add("headers", headers);
      rsp.add("requestURI", httpReq.getRequestURI());
      rsp.add("params", new SimpleOrderedMap<>(req.getParams()));
    }

    @Override
    public SolrRequestHandler getSubHandler(String subPath) {
      return this;
    }

    @Override
    public String getDescription() {
      return "echoes request headers and params";
    }

    @Override
    public Name getPermissionName(AuthorizationContext ctx) {
      return Name.ALL;
    }
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

  /**
   * The only replica is on the receiving node, so the target node forwards the request back to it.
   * The targeting directive must be consumed, or the request would bounce back and forth.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDirectiveConsumedWhenTargetForwardsToReplica() throws Exception {
    boolean useHeader = random().nextBoolean();
    String url = receivingNode.getBaseUrl() + "/" + COLLECTION + "/echo?wt=json&foo=bar";
    if (!useHeader) {
      url += "&" + TARGET_NODE_HEADER + "=" + URLEncoder.encode(targetNode.getNodeName(), UTF_8);
    }
    HttpURLConnection conn = open(url);
    conn.setRequestProperty("X-Other", "kept");
    if (useHeader) {
      conn.setRequestProperty(TARGET_NODE_HEADER, targetNode.getNodeName());
    }
    assertEquals(200, conn.getResponseCode());
    Map<?, ?> rsp =
        (Map<?, ?>) Utils.fromJSONString(new String(conn.getInputStream().readAllBytes(), UTF_8));
    conn.disconnect();

    Map<String, Object> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    headers.putAll((Map<String, ?>) rsp.get("headers"));
    assertEquals("kept", headers.get("X-Other"));
    assertFalse(headers.containsKey(TARGET_NODE_HEADER));
    Map<?, ?> params = (Map<?, ?>) rsp.get("params");
    assertEquals("bar", params.get("foo"));
    assertFalse(params.containsKey(TARGET_NODE_HEADER));
  }

  /** The path is proxied as received, still encoded. */
  @Test
  public void testEncodedPathPreserved() throws Exception {
    // the replica is on receivingNode, so the proxied request is served directly (no further hop)
    String encodedPath = "/a%20b%23c";
    HttpURLConnection conn =
        open(targetNode.getBaseUrl() + "/" + COLLECTION + "/echo" + encodedPath + "?wt=json");
    conn.setRequestProperty(TARGET_NODE_HEADER, receivingNode.getNodeName());
    assertEquals(200, conn.getResponseCode());
    Map<?, ?> rsp =
        (Map<?, ?>) Utils.fromJSONString(new String(conn.getInputStream().readAllBytes(), UTF_8));
    conn.disconnect();
    assertEquals("/solr/" + COLLECTION + "/echo" + encodedPath, rsp.get("requestURI"));
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
