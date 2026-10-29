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
package org.apache.solr.handler.admin.api;

import java.util.Map;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.client.solrj.request.LoggingApi;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.client.solrj.request.SystemApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that node-local admin APIs can be proxied to another live node via the {@code nodes}
 * query parameter.
 */
public class NodeAdminProxySolrCloudTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
  }

  private static String nodeName(int index) {
    return cluster
        .getJettySolrRunners()
        .get(index)
        .getCoreContainer()
        .getZkController()
        .getNodeName();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> remotePayload(
      Map<String, Object> remoteNodeData, String node) {
    assertNotNull(remoteNodeData);
    assertTrue(remoteNodeData.containsKey(node));
    Object payload = remoteNodeData.get(node);
    assertNotNull(payload);
    assertTrue(payload instanceof Map);
    return (Map<String, Object>) payload;
  }

  @Test
  public void testThreadDumpProxy() throws Exception {
    final String remote = nodeName(1);
    final var req = new SystemApi.GetThreadDump();
    req.setNodes(remote);

    final var rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.responseHeader.status);
    Map<String, Object> payload = remotePayload(rsp.remoteNodeData, remote);
    assertTrue(payload.containsKey("system"));
  }

  @Test
  public void testNodePropertiesProxy() throws Exception {
    final String remote = nodeName(1);
    final var req = new NodeApi.GetNodeProperties();
    req.setNodes(remote);

    final var rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.responseHeader.status);
    Map<String, Object> payload = remotePayload(rsp.remoteNodeData, remote);
    assertTrue(payload.containsKey("system.properties"));
  }

  @Test
  public void testLoggingLevelsProxy() throws Exception {
    final String remote = nodeName(1);
    final var req = new LoggingApi.ListAllLoggersAndLevels();
    req.setNodes(remote);

    final var rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.responseHeader.status);
    Map<String, Object> payload = remotePayload(rsp.remoteNodeData, remote);
    assertTrue(payload.containsKey("levels"));
    assertTrue(payload.containsKey("loggers"));
  }

  @Test
  public void testLoggingMessagesProxy() throws Exception {
    final String remote = nodeName(1);
    final var req = new LoggingApi.FetchLocalLogMessages();
    req.setSince(0L);
    req.setNodes(remote);

    final var rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.responseHeader.status);
    Map<String, Object> payload = remotePayload(rsp.remoteNodeData, remote);
    assertTrue(payload.containsKey("history"));
  }

  @Test
  public void testCoreStatusProxy() throws Exception {
    final String remote = nodeName(1);
    final var req = new CoresApi.GetAllCoreStatus();
    req.setIndexInfo(false);
    req.setNodes(remote);

    final var rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.responseHeader.status);
    Map<String, Object> payload = remotePayload(rsp.remoteNodeData, remote);
    assertTrue(payload.containsKey("status"));
  }
}
