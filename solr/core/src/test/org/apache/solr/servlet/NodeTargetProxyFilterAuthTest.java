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

import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudAuthTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/** Tests {@link NodeTargetProxyFilter} with authentication enabled. */
public class NodeTargetProxyFilterAuthTest extends SolrCloudAuthTestCase {

  private static final String USER = "solr";
  private static final String PASSWORD = "SolrRocks";

  private static final String SECURITY_JSON =
      """
      {
        "authentication": {
          "blockUnknown": true,
          "class": "solr.BasicAuthPlugin",
          "credentials": {
            "solr": "JeRyxP8A3dVWhFgFbf/Eg2RXmuoU8BE5gbNQyxmGAJQ= 6zkmjMjkMKyJX6/f0VarEWQujju5BzxZXub6WOrEKCw=",
            "limited": "JeRyxP8A3dVWhFgFbf/Eg2RXmuoU8BE5gbNQyxmGAJQ= 6zkmjMjkMKyJX6/f0VarEWQujju5BzxZXub6WOrEKCw="
          }
        },
        "authorization": {
          "class": "solr.RuleBasedAuthorizationPlugin",
          "user-role": {"solr": "admin"},
          "permissions": [{"name": "all", "role": "admin"}]
        }
      }
      """;

  private static JettySolrRunner receivingNode;
  private static JettySolrRunner targetNode;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
    receivingNode = cluster.getJettySolrRunner(0);
    targetNode = cluster.getJettySolrRunner(1);
  }

  @Test
  public void testProxiesWhenAuthenticated() throws Exception {
    var req = systemInfoRequest(targetNode.getNodeName());
    req.setBasicAuthCredentials(USER, PASSWORD);
    assertEquals(targetNode.getNodeName(), receivingNode.getSolrClient().request(req).get("node"));
  }

  @Test
  public void testUnknownNodeIs400WhenAuthenticated() {
    var req = systemInfoRequest("no.such.host:1234_solr");
    req.setBasicAuthCredentials(USER, PASSWORD);
    var e = expectThrows(SolrException.class, () -> receivingNode.getSolrClient().request(req));
    assertEquals(400, e.code());
  }

  @Test
  public void testUnauthenticatedIs401() {
    var req = systemInfoRequest(targetNode.getNodeName());
    var e = expectThrows(SolrException.class, () -> receivingNode.getSolrClient().request(req));
    assertEquals(401, e.code());
  }

  /** The target node authorizes the original caller, not the proxying node. */
  @Test
  public void testProxiedRequestAuthorizedAsCaller() {
    var req = systemInfoRequest(targetNode.getNodeName());
    req.setBasicAuthCredentials("limited", PASSWORD);
    var e = expectThrows(SolrException.class, () -> receivingNode.getSolrClient().request(req));
    assertEquals(403, e.code());
  }

  private static GenericSolrRequest systemInfoRequest(String targetNodeName) {
    var req =
        new GenericSolrRequest(
            SolrRequest.METHOD.GET,
            "/admin/info/system",
            SolrRequest.SolrRequestType.ADMIN,
            SolrParams.of());
    req.addHeader(TARGET_NODE_HEADER, targetNodeName);
    return req;
  }
}
