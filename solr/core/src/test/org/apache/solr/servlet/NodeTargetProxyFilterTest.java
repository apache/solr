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

import static org.apache.solr.servlet.NodeTargetProxyFilter.TARGET_NODE_HEADER;

import java.util.Map;
import java.util.TreeMap;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.V2Request;
import org.apache.solr.cloud.ConfigRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
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
    var req = systemInfoRequest(SolrRequest.METHOD.GET, params());
    req.addHeader(TARGET_NODE_HEADER, targetNode.getNodeName());
    assertEquals(targetNode.getNodeName(), respondingNode(receivingNode, req));
  }

  @Test
  public void testHeaderProxiesV2() throws Exception {
    var req = new V2Request.Builder("/node/system").GET().build();
    req.addHeader(TARGET_NODE_HEADER, targetNode.getNodeName());
    assertEquals(targetNode.getNodeName(), respondingNode(receivingNode, req));
  }

  @Test
  public void testQueryParamProxiesGet() throws Exception {
    var req =
        systemInfoRequest(
            SolrRequest.METHOD.GET, params().set(TARGET_NODE_HEADER, targetNode.getNodeName()));
    assertEquals(targetNode.getNodeName(), respondingNode(receivingNode, req));
  }

  @Test
  public void testQueryParamIgnoredForPost() throws Exception {
    // a body forces the params onto the URL's query string
    var req =
        systemInfoRequest(
                SolrRequest.METHOD.POST, params().set(TARGET_NODE_HEADER, targetNode.getNodeName()))
            .withContent(new byte[0], "text/plain");
    assertEquals(receivingNode.getNodeName(), respondingNode(receivingNode, req));
  }

  /**
   * The only replica is on the receiving node, so the target node forwards the request back to it.
   * The targeting directive must be consumed, or the request would bounce back and forth.
   */
  @SuppressWarnings("unchecked")
  @Test
  public void testDirectiveConsumedWhenTargetForwardsToReplica() throws Exception {
    var params = params();
    boolean useHeader = random().nextBoolean();
    if (!useHeader) {
      params.set(TARGET_NODE_HEADER, targetNode.getNodeName());
    }
    var req = new GenericSolrRequest(SolrRequest.METHOD.GET, "/" + COLLECTION + "/echo", params);
    req.addHeader("X-Other", "kept");
    if (useHeader) {
      req.addHeader(TARGET_NODE_HEADER, targetNode.getNodeName());
    }
    NamedList<Object> rsp = receivingNode.getSolrClient().request(req);

    Map<String, Object> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    headers.putAll((Map<String, ?>) rsp.get("headers"));
    assertEquals("kept", headers.get("X-Other"));
    assertFalse(headers.containsKey(TARGET_NODE_HEADER));
    NamedList<?> echoedParams = (NamedList<?>) rsp.get("params");
    assertEquals("bar", echoedParams.get("foo"));
    assertNull(echoedParams.get(TARGET_NODE_HEADER));
  }

  /** The path is proxied as received, still encoded. */
  @Test
  public void testEncodedPathPreserved() throws Exception {
    // the replica is on receivingNode, so the proxied request is served directly (no further hop)
    String encodedPath = "/a%20b%23c";
    var req =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET, "/" + COLLECTION + "/echo" + encodedPath, params());
    req.addHeader(TARGET_NODE_HEADER, receivingNode.getNodeName());
    NamedList<Object> rsp = targetNode.getSolrClient().request(req);
    assertEquals("/solr/" + COLLECTION + "/echo" + encodedPath, rsp.get("requestURI"));
  }

  /** Jetty accepts query characters that are invalid in a URI (e.g. a local-params '{'). */
  @Test
  public void testUnencodedQueryCharsProxied() throws Exception {
    var httpClient = targetNode.getSolrClient().getHttpClient();
    var rsp =
        httpClient
            .newRequest(targetNode.getBaseUrl().toString())
            .path("/solr/" + COLLECTION + "/echo?wt=json&foo={!x}|b^2")
            .headers(h -> h.add(TARGET_NODE_HEADER, receivingNode.getNodeName()))
            .send();
    assertEquals(200, rsp.getStatus());
    Map<?, ?> params =
        (Map<?, ?>) ((Map<?, ?>) Utils.fromJSONString(rsp.getContentAsString())).get("params");
    assertEquals("{!x}|b^2", params.get("foo"));
  }

  @Test
  public void testTargetingSelfIsHandledLocally() throws Exception {
    var req = systemInfoRequest(SolrRequest.METHOD.GET, params());
    req.addHeader(TARGET_NODE_HEADER, receivingNode.getNodeName());
    assertEquals(receivingNode.getNodeName(), respondingNode(receivingNode, req));
  }

  @Test
  public void testUnknownNodeIs400() {
    var req = systemInfoRequest(SolrRequest.METHOD.GET, params());
    req.addHeader(TARGET_NODE_HEADER, "no.such.host:1234_solr");
    var e = expectThrows(SolrException.class, () -> receivingNode.getSolrClient().request(req));
    assertEquals(400, e.code());
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

  private static GenericSolrRequest systemInfoRequest(
      SolrRequest.METHOD method, SolrParams params) {
    return new GenericSolrRequest(
        method, "/admin/info/system", SolrRequest.SolrRequestType.ADMIN, params);
  }

  private static ModifiableSolrParams params() {
    return new ModifiableSolrParams().set("foo", "bar"); // including an arbitrary pair
  }

  private static String respondingNode(JettySolrRunner node, SolrRequest<?> req) throws Exception {
    return (String) node.getSolrClient().request(req).get("node");
  }
}
