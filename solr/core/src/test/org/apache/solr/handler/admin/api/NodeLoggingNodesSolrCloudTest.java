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
import org.apache.solr.client.api.model.ListLevelsResponse;
import org.apache.solr.client.api.model.LogLevelChange;
import org.apache.solr.client.api.model.LogLevelInfo;
import org.apache.solr.client.api.model.LoggingResponse;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.LoggingApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the {@code nodes} broadcast on the V2 logging endpoints ({@code PUT /node/logging/levels}
 * and {@code GET /node/logging/levels}) against a two-node cluster.
 *
 * <p>Requests go through the generated SolrJ client, so this also exercises its codegen. Log levels
 * themselves cannot be observed per node here: every node of the test cluster shares one JVM, and
 * log levels are JVM-wide. What is observable per node is the response: a broadcast request is
 * expected to carry one result entry per requested node (in {@link LoggingResponse#remoteNodeData},
 * keyed by node name), plus the names of any requested nodes that did not respond (in {@link
 * LoggingResponse#failedNodes}). The JVM-wide level after a broadcast is observable, and one test
 * uses it to show the level change was applied.
 */
public class NodeLoggingNodesSolrCloudTest extends SolrCloudTestCase {

  private static final String TEST_LOGGER =
      "org.apache.solr.handler.admin.api.NodeLoggingNodesSolrCloudTest";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("cloud-minimal", configset("cloud-minimal")).configure();
  }

  @Test
  public void testBroadcastToAllNodesReportsEveryNode() throws Exception {
    final String receivingNode = jettyName(0);
    final String otherNode = jettyName(1);

    try {
      final LoggingResponse rsp = putLogLevel(0, "all", TEST_LOGGER, "WARN");

      assertNotNull("Broadcast response should report failed nodes", rsp.failedNodes);
      assertTrue(
          "No node should be reported failed: " + rsp.failedNodes, rsp.failedNodes.isEmpty());
      for (String nodeName : new String[] {receivingNode, otherNode}) {
        final Object perNode = rsp.remoteNodeData.get(nodeName);
        assertNotNull(
            "Expected a per-node result for " + nodeName + " in " + rsp.remoteNodeData, perNode);
        assertTrue(perNode instanceof Map);
        // The per-node entry is that node's own logging response. Its presence proves the
        // request reached the node and a response came back.
        assertNotNull(((Map<?, ?>) perNode).get("watcher"));
      }
      // Log levels are JVM-wide in this cluster, so the receiving node's own listing shows
      // whether the level change in the broadcast payload was actually applied, not just
      // delivered.
      assertEquals(
          "The broadcast level change should be in effect", "WARN", loggerLevel(getLogLevels(0)));
    } finally {
      // Leave the logger as the test found it (no level set) for the rest of the JVM.
      putLogLevel(0, null, TEST_LOGGER, "unset");
    }
  }

  @Test
  public void testBroadcastToSingleNamedNode() throws Exception {
    final String receivingNode = jettyName(0);
    final String otherNode = jettyName(1);

    final LoggingResponse rsp = putLogLevel(0, otherNode, TEST_LOGGER, "WARN");

    assertNotNull(rsp.failedNodes);
    assertTrue(rsp.failedNodes.isEmpty());
    assertNotNull("Expected a per-node result for " + otherNode, rsp.remoteNodeData.get(otherNode));
    assertNull(
        "The receiving node was not a target and must not appear",
        rsp.remoteNodeData.get(receivingNode));
  }

  @Test
  public void testNoNodesKeepsLocalResponseShape() throws Exception {
    final LoggingResponse rsp = putLogLevel(0, null, TEST_LOGGER, "WARN");

    assertNotNull(rsp.watcherName);
    assertNull(rsp.failedNodes);
    assertTrue("No per-node entries expected: " + rsp.remoteNodeData, rsp.remoteNodeData.isEmpty());
  }

  @Test
  public void testUnknownNodeNameFailsFast() throws Exception {
    final var req = new LoggingApi.ModifyLocalLogLevel();
    req.addLogLevelChange(new LogLevelChange(TEST_LOGGER, "WARN"));
    req.setNodes("no-such-host.invalid:9999_solr");

    try (HttpJettySolrClient client = client(0)) {
      final RemoteSolrException e =
          expectThrows(RemoteSolrException.class, () -> req.process(client));
      assertEquals(400, e.code());
      assertTrue(
          "Error should name the problem: " + e.getMessage(),
          e.getMessage().contains("not part of the cluster"));
    }
  }

  @Test
  public void testBroadcastLevelsToAllNodesReportsEveryNode() throws Exception {
    final String receivingNode = jettyName(0);
    final String otherNode = jettyName(1);

    final JsonNode rsp = getLogLevels(cluster.getJettySolrRunners().get(0), "all");

    final JsonNode failedNodes = rsp.get("failedNodes");
    assertNotNull("Broadcast response should report failed nodes", failedNodes);
    assertTrue("No node should be reported failed: " + failedNodes, failedNodes.isEmpty());
    // The receiving node does not also serve the listing locally, so the top level carries no
    // levels or loggers of its own; the listings live under the per-node entries.
    assertNull("Expected no top-level 'levels' in a broadcast response: " + rsp, rsp.get("levels"));
    assertNull(
        "Expected no top-level 'loggers' in a broadcast response: " + rsp, rsp.get("loggers"));
    for (String nodeName : new String[] {receivingNode, otherNode}) {
      final JsonNode perNode = rsp.get(nodeName);
      assertNotNull("Expected a per-node result for " + nodeName + " in " + rsp, perNode);
      // The per-node entry is that node's own levels listing.
      assertNotNull(perNode.get("watcher"));
      assertNotNull(perNode.get("levels"));
      final JsonNode loggers = perNode.get("loggers");
      assertNotNull(loggers);
      assertTrue("Expected a non-empty logger list for " + nodeName, loggers.size() > 0);
    }
  }

  @Test
  public void testBroadcastLevelsToSingleNamedNode() throws Exception {
    final String receivingNode = jettyName(0);
    final String otherNode = jettyName(1);

    final JsonNode rsp = getLogLevels(cluster.getJettySolrRunners().get(0), otherNode);

    assertNotNull(rsp.get("failedNodes"));
    assertTrue(rsp.get("failedNodes").isEmpty());
    final JsonNode perNode = rsp.get(otherNode);
    assertNotNull("Expected a per-node result for " + otherNode, perNode);
    assertNotNull(perNode.get("loggers"));
    assertNull("The receiving node was not a target and must not appear", rsp.get(receivingNode));
  }

  @Test
  public void testNoNodesLevelsKeepsLocalResponseShape() throws Exception {
    final JsonNode rsp = getLogLevels(cluster.getJettySolrRunners().get(0), null);

    assertNotNull(rsp.get("watcher"));
    assertNotNull(rsp.get("levels"));
    final JsonNode loggers = rsp.get("loggers");
    assertNotNull(loggers);
    assertTrue("Expected a non-empty logger list", loggers.size() > 0);
    assertNull(rsp.get("failedNodes"));
    rsp.fieldNames()
        .forEachRemaining(
            field ->
                assertFalse("No per-node entries expected: " + field, field.endsWith("_solr")));
  }

  @Test
  public void testUnknownNodeNameFailsFastForLevelsGet() {
    final GenericV2SolrRequest req =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.GET,
            "/node/logging/levels",
            SolrRequest.SolrRequestType.ADMIN,
            new ModifiableSolrParams().set("nodes", "no-such-host.invalid:9999_solr"));
    final RemoteSolrException e =
        expectThrows(RemoteSolrException.class, () -> sendWithDefaultParser(jetty(0), req));
    assertEquals(400, e.code());
    assertTrue(
        "Error should name the problem: " + e.getMessage(),
        e.getMessage().contains("not part of the cluster"));
  }

  private static String jettyName(int idx) {
    return cluster.getJettySolrRunners().get(idx).getNodeName();
  }

  private static JettySolrRunner jetty(int idx) {
    return cluster.getJettySolrRunners().get(idx);
  }

  private static HttpJettySolrClient client(int idx) {
    return new HttpJettySolrClient.Builder(jetty(idx).getBaseUrl().toString()).build();
  }

  private static LoggingResponse putLogLevel(
      int targetIdx, String nodes, String logger, String level) throws Exception {
    final var req = new LoggingApi.ModifyLocalLogLevel();
    req.addLogLevelChange(new LogLevelChange(logger, level));
    req.setNodes(nodes);
    try (HttpJettySolrClient client = client(targetIdx)) {
      return req.process(client);
    }
  }

  private static JsonNode getLogLevels(JettySolrRunner target, String nodes) throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    if (nodes != null) {
      params.set("nodes", nodes);
    }
    final GenericV2SolrRequest req =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.GET,
            "/node/logging/levels",
            SolrRequest.SolrRequestType.ADMIN,
            params);
    req.setResponseParser(new InputStreamResponseParser("json"));
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(target.getBaseUrl().toString()).build()) {
      final String body =
          InputStreamResponseParser.consumeResponseToString(req.process(client).getResponse());
      return MAPPER.readTree(body);
    }
  }

  private static SimpleSolrResponse sendWithDefaultParser(
      JettySolrRunner target, GenericV2SolrRequest req) throws Exception {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(target.getBaseUrl().toString()).build()) {
      return req.process(client);
    }
  }

  private static ListLevelsResponse getLogLevels(int targetIdx) throws Exception {
    final var req = new LoggingApi.ListAllLoggersAndLevels();
    try (HttpJettySolrClient client = client(targetIdx)) {
      return req.process(client);
    }
  }

  private static String loggerLevel(ListLevelsResponse listing) {
    for (LogLevelInfo logger : listing.loggers) {
      if (TEST_LOGGER.equals(logger.name)) {
        return logger.level;
      }
    }
    return null;
  }
}
