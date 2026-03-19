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

import static org.apache.solr.client.api.model.NodeHealthResponse.NodeStatus.OK;
import static org.hamcrest.Matchers.containsString;

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeHealthTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();

    CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testHealthyNodeReturnsOkStatus() throws Exception {
    final var request = new NodeApi.Healthcheck();
    final var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertNull("Expected no error on a healthy node", response.error);
  }

  @Test
  public void testRequireHealthyCoresReturnOkWhenAllCoresHealthy() throws Exception {
    final var request = new NodeApi.Healthcheck();
    request.setRequireHealthyCores(true);
    final var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertEquals("All cores are healthy", response.message);
  }

  @Test
  public void testCloudMode_UnhealthyWhenZkClientClosed() throws Exception {
    // Use a fresh node so closing its ZK client does not break the primary cluster node
    JettySolrRunner newJetty = cluster.startJettySolrRunner();
    cluster.waitForNode(newJetty, 30);
    try (SolrClient nodeClient = newJetty.newClient()) {
      // Sanity check: the new node should start out healthy
      assertEquals(OK, new NodeApi.Healthcheck().process(nodeClient).status);

      // Break the ZK connection to put the node into an unhealthy state
      newJetty.getCoreContainer().getZkController().getZkClient().close();

      SolrException e =
          assertThrows(SolrException.class, () -> new NodeApi.Healthcheck().process(nodeClient));
      assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
      assertThat(
          "Expected 'Host Unavailable' in exception message",
          e.getMessage(),
          containsString(("Host Unavailable")));
    } finally {
      newJetty.stop();
    }
  }

  /**
   * Verifies that when the node's name is absent from ZooKeeper's live-nodes set (while the ZK
   * session itself is still connected), the v2 health-check API throws a {@code
   * SERVICE_UNAVAILABLE} exception with a message identifying the live-nodes check as the cause.
   *
   * <p>This specifically exercises the code path at NodeHealth#getClusterState() that checks {@code
   * clusterState.getLiveNodes().contains(nodeName)}.
   */
  @Test
  public void testNotInLiveNodes_ThrowsServiceUnavailable() throws Exception {
    JettySolrRunner newJetty = cluster.startJettySolrRunner();
    cluster.waitForNode(newJetty, 30);
    try (SolrClient nodeClient = newJetty.newClient()) {
      // Sanity check: the new node should start out healthy
      assertEquals(OK, new NodeApi.Healthcheck().process(nodeClient).status);

      String nodeName = newJetty.getCoreContainer().getZkController().getNodeName();

      // Remove the node from ZooKeeper's live_nodes without closing the ZK session.
      // This ensures the "ZK not connected" check passes and only the "not in live nodes"
      // check fires, isolating the code path under test.
      newJetty.getCoreContainer().getZkController().removeEphemeralLiveNode();

      // Wait for the node's own ZkStateReader to reflect the removal before querying it.
      newJetty
          .getCoreContainer()
          .getZkController()
          .getZkStateReader()
          .waitForLiveNodes(10, TimeUnit.SECONDS, missingLiveNode(nodeName));

      SolrException e =
          assertThrows(SolrException.class, () -> new NodeApi.Healthcheck().process(nodeClient));
      assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
      assertThat(
          "Expected 'Not in live nodes' in exception message",
          e.getMessage(),
          containsString("Not in live nodes"));
    } finally {
      newJetty.stop();
    }
  }
}
