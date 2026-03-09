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

import static org.apache.solr.client.api.model.NodeHealthResponse.NodeStatus.FAILURE;
import static org.apache.solr.client.api.model.NodeHealthResponse.NodeStatus.OK;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.NodeApi;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class NodeHealthAPITest extends SolrCloudTestCase {

  /**
   * A standalone (non-ZooKeeper) Jetty instance used by the legacy-mode tests. The
   * {@code @ClassRule} ensures it is shut down after all tests in this class complete.
   */
  @ClassRule public static SolrJettyTestRule standaloneJetty = new SolrJettyTestRule();

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    standaloneJetty.startSolr(createTempDir());

    CollectionAdminRequest.createCollection(DEFAULT_TEST_COLLECTION_NAME, "conf", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testCloudMode_HealthyNodeReturnsOkStatus() throws Exception {
    final var request = new NodeApi.Healthcheck();
    final var response = request.process(cluster.getSolrClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertNull("Expected no error on a healthy node", response.error);
  }

  @Test
  public void testCloudMode_RequireHealthyCoresReturnOkWhenAllCoresHealthy() throws Exception {
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
    try (SolrClient nodeClient = newJetty.newClient()) {
      // Sanity check: the new node should start out healthy
      assertEquals(OK, new NodeApi.Healthcheck().process(nodeClient).status);

      // Break the ZK connection to put the node into an unhealthy state
      newJetty.getCoreContainer().getZkController().getZkClient().close();

      SolrException e =
          assertThrows(SolrException.class, () -> new NodeApi.Healthcheck().process(nodeClient));
      assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
      assertTrue(
          "Expected 'Host Unavailable' in exception message",
          e.getMessage().contains("Host Unavailable"));
    } finally {
      newJetty.stop();
    }
  }

  @Test
  public void testLegacyMode_WithoutMaxGenerationLagReturnsOk() throws Exception {

    final var request = new NodeApi.Healthcheck();
    final var response = request.process(standaloneJetty.getAdminClient());

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertTrue(
        "Expected message about maxGenerationLag not being specified",
        response.message.contains("maxGenerationLag isn't specified"));
  }

  @Test
  public void testLegacyMode_WithNegativeMaxGenerationLagReturnsFailure() {
    // maxGenerationLag is a v1-only parameter: NodeHealthAPI.healthcheck() (v2) hardcodes it to
    // null and never forwards it from request params. NodeApi.Healthcheck therefore cannot be used
    // to exercise this code path, so we call the JAX-RS implementation directly.
    // FIXME: IInteresting!  Do we have a gap?
    final var response =
        new NodeHealthAPI(standaloneJetty.getCoreContainer()).checkNodeHealth(null, -1);

    assertNotNull(response);
    assertEquals(FAILURE, response.status);
    assertTrue(
        "Expected message about invalid maxGenerationLag",
        response.message.contains("Invalid value of maxGenerationLag"));
  }
}
