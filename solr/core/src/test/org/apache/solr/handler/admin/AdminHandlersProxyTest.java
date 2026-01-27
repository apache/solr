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

package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AdminHandlersProxyTest extends SolrCloudTestCase {
  private CloudSolrClient solrClient;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("metricsEnabled", "true");
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    solrClient = cluster.getSolrClient();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void proxySystemInfoHandlerAllNodes() throws IOException, SolrServerException {
    MapSolrParams params = new MapSolrParams(Collections.singletonMap("nodes", "all"));

    SystemInfoRequest req = new SystemInfoRequest(params);
    SystemInfoResponse rsp = req.process(solrClient, null);
    NamedList<Object> nl = rsp.getResponse();
    assertEquals(3, nl.size());
    assertTrue(nl.getName(1).endsWith("_solr"));
    assertTrue(nl.getName(2).endsWith("_solr"));
    String node1name = nl.getName(1);
    String node2name = nl.getName(2);
    NamedList<Object> node1 = (NamedList<Object>) nl.get(node1name);
    // mmm??? nodeInfo is not translated to NamedList?
    assertEquals("solrcloud", ((Map<String, Object>) node1.get("nodeInfo")).get("mode"));
    NamedList<Object> node2 = (NamedList<Object>) nl.get(node2name);
    assertEquals(node2name, ((Map<String, Object>) node2.get("nodeInfo")).get("node"));
  }

  @Test(expected = SolrException.class)
  public void proxySystemInfoHandlerNonExistingNode() throws IOException, SolrServerException {
    MapSolrParams params =
        new MapSolrParams(Collections.singletonMap("nodes", "example.com:1234_solr"));
    SystemInfoRequest req = new SystemInfoRequest(params);
    SystemInfoResponse rsp = req.process(solrClient, null);
  }

  @Test
  public void proxySystemInfoHandlerOneNode() {
    Set<String> nodes = solrClient.getClusterStateProvider().getLiveNodes();
    assertEquals(2, nodes.size());
    nodes.forEach(
        node -> {
          MapSolrParams params = new MapSolrParams(Collections.singletonMap("nodes", node));
          SystemInfoRequest req = new SystemInfoRequest(params);
          try {
            SystemInfoResponse rsp = req.process(solrClient, null);
            NamedList<Object> nl = rsp.getResponse();
            assertEquals(2, nl.size());
            assertEquals("solrcloud", rsp.getMode());
            assertEquals(nl.getName(1), rsp.getNode());
          } catch (Exception e) {
            fail("Exception while proxying request to node " + node);
          }
        });
  }
}
