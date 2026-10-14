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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.GenericV2SolrRequest;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests the {@code nodes} broadcast on the V2 {@code PUT /node/logging/levels} endpoint against a
 * two-node cluster.
 *
 * <p>The requests are sent with a generic V2 request, and the assertions run against the raw JSON
 * response body. Log levels themselves cannot be observed per node here: every node of the test
 * cluster shares one JVM, and log levels are JVM-wide. What is observable per node is the response:
 * a broadcast request is expected to carry one result entry per requested node, keyed by node name,
 * plus the names of any requested nodes that did not respond. The JVM-wide level after a broadcast
 * is observable, and one test uses it to show the level change was applied.
 */
public class NodeLoggingNodesSolrCloudTest extends SolrCloudTestCase {

  private static final String TEST_LOGGER =
      "org.apache.solr.handler.admin.api.NodeLoggingNodesSolrCloudTest";
  private static final String LEVEL_CHANGES_JSON =
      "[{\"logger\": \"" + TEST_LOGGER + "\", \"level\": \"WARN\"}]";
  private static final String UNSET_LEVEL_JSON =
      "[{\"logger\": \"" + TEST_LOGGER + "\", \"level\": \"unset\"}]";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("cloud-minimal", configset("cloud-minimal")).configure();
  }

  @Test
  public void testBroadcastToAllNodesReportsEveryNode() throws Exception {
    final String receivingNode = jettyName(0);
    final String otherNode = jettyName(1);

    try {
      final JsonNode rsp = putLogLevels(cluster.getJettySolrRunners().get(0), "all");

      final JsonNode failedNodes = rsp.get("failedNodes");
      assertNotNull("Broadcast response should report failed nodes", failedNodes);
      assertTrue("No node should be reported failed: " + failedNodes, failedNodes.isEmpty());
      for (String nodeName : new String[] {receivingNode, otherNode}) {
        final JsonNode perNode = rsp.get(nodeName);
        assertNotNull("Expected a per-node result for " + nodeName + " in " + rsp, perNode);
        // The per-node entry is that node's own logging response. Its presence proves the
        // request reached the node and a response came back.
        assertNotNull(perNode.get("watcher"));
      }
      // Log levels are JVM-wide in this cluster, so the receiving node's own listing shows
      // whether the level change in the broadcast payload was actually applied, not just
      // delivered.
      assertEquals(
          "The broadcast level change should be in effect",
          "WARN",
          loggerLevel(getLogLevels(cluster.getJettySolrRunners().get(0))));
    } finally {
      // Leave the logger as the test found it (no level set) for the rest of the JVM.
      putLogLevels(cluster.getJettySolrRunners().get(0), null, UNSET_LEVEL_JSON);
    }
  }

  @Test
  public void testBroadcastToSingleNamedNode() throws Exception {
    final String receivingNode = jettyName(0);
    final String otherNode = jettyName(1);

    final JsonNode rsp = putLogLevels(cluster.getJettySolrRunners().get(0), otherNode);

    assertNotNull(rsp.get("failedNodes"));
    assertTrue(rsp.get("failedNodes").isEmpty());
    assertNotNull("Expected a per-node result for " + otherNode, rsp.get(otherNode));
    assertNull("The receiving node was not a target and must not appear", rsp.get(receivingNode));
  }

  @Test
  public void testNoNodesKeepsLocalResponseShape() throws Exception {
    final JsonNode rsp = putLogLevels(cluster.getJettySolrRunners().get(0), null);

    assertNotNull(rsp.get("watcher"));
    assertNull(rsp.get("failedNodes"));
    rsp.fieldNames()
        .forEachRemaining(
            field ->
                assertFalse("No per-node entries expected: " + field, field.endsWith("_solr")));
  }

  @Test
  public void testUnknownNodeNameFailsFast() {
    final GenericV2SolrRequest req =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.PUT,
            "/node/logging/levels",
            SolrRequest.SolrRequestType.ADMIN,
            new ModifiableSolrParams().set("nodes", "no-such-host.invalid:9999_solr"));
    req.withContent(LEVEL_CHANGES_JSON.getBytes(StandardCharsets.UTF_8), "application/json");
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

  private static JsonNode putLogLevels(JettySolrRunner target, String nodes) throws Exception {
    return putLogLevels(target, nodes, LEVEL_CHANGES_JSON);
  }

  private static JsonNode putLogLevels(JettySolrRunner target, String nodes, String json)
      throws Exception {
    final ModifiableSolrParams params = new ModifiableSolrParams();
    if (nodes != null) {
      params.set("nodes", nodes);
    }
    final GenericV2SolrRequest req =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.PUT,
            "/node/logging/levels",
            SolrRequest.SolrRequestType.ADMIN,
            params);
    req.withContent(json.getBytes(StandardCharsets.UTF_8), "application/json");
    req.setResponseParser(new InputStreamResponseParser("json"));
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(target.getBaseUrl().toString()).build()) {
      final String body =
          InputStreamResponseParser.consumeResponseToString(req.process(client).getResponse());
      return MAPPER.readTree(body);
    }
  }

  private static JsonNode getLogLevels(JettySolrRunner target) throws Exception {
    final GenericV2SolrRequest req =
        new GenericV2SolrRequest(
            SolrRequest.METHOD.GET,
            "/node/logging/levels",
            SolrRequest.SolrRequestType.ADMIN,
            new ModifiableSolrParams());
    req.setResponseParser(new InputStreamResponseParser("json"));
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(target.getBaseUrl().toString()).build()) {
      final String body =
          InputStreamResponseParser.consumeResponseToString(req.process(client).getResponse());
      return MAPPER.readTree(body);
    }
  }

  private static String loggerLevel(JsonNode listing) {
    for (JsonNode logger : listing.get("loggers")) {
      if (TEST_LOGGER.equals(logger.get("name").asText())) {
        final JsonNode level = logger.get("level");
        return level == null || level.isNull() ? null : level.asText();
      }
    }
    return null;
  }

  private static SimpleSolrResponse sendWithDefaultParser(
      JettySolrRunner target, GenericV2SolrRequest req) throws Exception {
    try (HttpJettySolrClient client =
        new HttpJettySolrClient.Builder(target.getBaseUrl().toString()).build()) {
      return req.process(client);
    }
  }
}
