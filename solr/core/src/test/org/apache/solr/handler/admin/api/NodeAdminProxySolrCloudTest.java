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

import static org.apache.solr.servlet.NodeTargetProxyFilter.TARGET_NODE_HEADER;

import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.client.solrj.request.LoggingApi;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.client.solrj.request.SystemApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that node-local admin APIs can be read from another live node via {@code
 * X-Solr-targetNode} (native response shape) and via the {@code nodes} query parameter (keyed
 * fallback).
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

  private SolrClient receivingClient() {
    return cluster.getJettySolrRunner(0).getSolrClient();
  }

  @Test
  public void testThreadDumpProxy() throws Exception {
    final String remote = nodeName(1);
    final var req = new SystemApi.GetThreadDump();
    req.setNodes(remote);

    final var rsp = req.process(cluster.getSolrClient());
    assertEquals(0, rsp.responseHeader.status);
    assertNotNull(rsp.failedNodes);
    assertTrue(rsp.failedNodes.isEmpty());
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
    assertNotNull(rsp.failedNodes);
    assertTrue(rsp.failedNodes.isEmpty());
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
    assertNotNull(rsp.failedNodes);
    assertTrue(rsp.failedNodes.isEmpty());
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
    assertNotNull(rsp.failedNodes);
    assertTrue(rsp.failedNodes.isEmpty());
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
    assertNotNull(rsp.failedNodes);
    assertTrue(rsp.failedNodes.isEmpty());
    Map<String, Object> payload = remotePayload(rsp.remoteNodeData, remote);
    assertTrue(payload.containsKey("status"));
  }

  @Test
  public void testUnknownNodeIs400() {
    final var req = new SystemApi.GetThreadDump();
    req.setNodes("no.such.host:1234_solr");
    var e = expectThrows(SolrException.class, () -> req.process(receivingClient()));
    assertEquals(400, e.code());
  }

  @Test
  public void testHeaderReturnsNativeThreadDump() throws Exception {
    final String remote = nodeName(1);
    final var req = new SystemApi.GetThreadDump();
    req.addHeader(TARGET_NODE_HEADER, remote);

    final var rsp = req.process(receivingClient());
    assertEquals(0, rsp.responseHeader.status);
    assertNotNull(rsp.system);
    assertTrue(rsp.remoteNodeData == null || rsp.remoteNodeData.isEmpty());
  }

  @Test
  public void testHeaderReturnsNativeCoreStatus() throws Exception {
    final String remote = nodeName(1);
    final var req = new CoresApi.GetAllCoreStatus();
    req.setIndexInfo(false);
    req.addHeader(TARGET_NODE_HEADER, remote);

    final var rsp = req.process(receivingClient());
    assertEquals(0, rsp.responseHeader.status);
    assertNotNull(rsp.status);
    assertTrue(rsp.remoteNodeData == null || rsp.remoteNodeData.isEmpty());
  }

  @Test
  public void testHeaderReturnsNativeSystemInfo() throws Exception {
    final String remote = nodeName(1);
    final var req = new SystemApi.GetNodeSystemInfo();
    req.addHeader(TARGET_NODE_HEADER, remote);

    final var rsp = req.process(receivingClient());
    assertEquals(0, rsp.responseHeader.status);
    assertEquals(remote, rsp.node);
    assertTrue(rsp.remoteNodeData == null || rsp.remoteNodeData.isEmpty());
  }

  @Test
  public void testHeaderReturnsNativeNodeProperties() throws Exception {
    final String remote = nodeName(1);
    final var req = new NodeApi.GetNodeProperties();
    req.addHeader(TARGET_NODE_HEADER, remote);

    final var rsp = req.process(receivingClient());
    assertEquals(0, rsp.responseHeader.status);
    assertNotNull(rsp.systemProperties);
    assertTrue(rsp.remoteNodeData == null || rsp.remoteNodeData.isEmpty());
  }

  @Test
  public void testHeaderReturnsNativeLoggingLevels() throws Exception {
    final String remote = nodeName(1);
    final var req = new LoggingApi.ListAllLoggersAndLevels();
    req.addHeader(TARGET_NODE_HEADER, remote);

    final var rsp = req.process(receivingClient());
    assertEquals(0, rsp.responseHeader.status);
    assertNotNull(rsp.levels);
    assertNotNull(rsp.loggers);
    assertTrue(rsp.remoteNodeData == null || rsp.remoteNodeData.isEmpty());
  }

  @Test
  public void testHeaderReturnsNativeLoggingMessages() throws Exception {
    final String remote = nodeName(1);
    final var req = new LoggingApi.FetchLocalLogMessages();
    req.setSince(0L);
    req.addHeader(TARGET_NODE_HEADER, remote);

    final var rsp = req.process(receivingClient());
    assertEquals(0, rsp.responseHeader.status);
    assertNotNull(rsp.docs);
    assertTrue(rsp.remoteNodeData == null || rsp.remoteNodeData.isEmpty());
  }
}
