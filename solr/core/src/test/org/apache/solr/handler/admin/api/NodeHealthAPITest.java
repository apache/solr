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

import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.OK;

import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Integration tests for {@link NodeHealthAPI} that use real Solr instances instead of Mockito
 * mocks.
 *
 * <p>Cloud-mode tests use a real {@link org.apache.solr.cloud.MiniSolrCloudCluster} and get a
 * {@link CoreContainer} directly from a {@link JettySolrRunner}. Legacy (standalone) mode tests
 * create an embedded {@link CoreContainer} via {@link NodeConfig} with no ZooKeeper.
 */
public class NodeHealthAPITest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  // ---- Cloud (ZooKeeper) mode tests ----

  @Test
  public void testCloudMode_HealthyNodeReturnsOkStatus() {
    CoreContainer coreContainer = cluster.getJettySolrRunner(0).getCoreContainer();

    final var response = new NodeHealthAPI(coreContainer).checkNodeHealth(null);

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertNull("Expected no error on a healthy node", response.error);
  }

  @Test
  public void testCloudMode_RequireHealthyCoresReturnOkWhenAllCoresHealthy() {
    CoreContainer coreContainer = cluster.getJettySolrRunner(0).getCoreContainer();

    // requireHealthyCores=true should succeed on a node with no unhealthy cores
    final var response = new NodeHealthAPI(coreContainer).checkNodeHealth(true);

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertEquals("All cores are healthy", response.message);
  }

  @Test
  public void testCloudMode_UnhealthyWhenZkClientClosed() throws Exception {
    // Use a fresh node so closing its ZK client does not break the primary cluster node
    JettySolrRunner newJetty = cluster.startJettySolrRunner();
    try {
      CoreContainer coreContainer = newJetty.getCoreContainer();

      // Sanity check: the new node should start out healthy
      assertEquals(OK, new NodeHealthAPI(coreContainer).checkNodeHealth(null).status);

      // Break the ZK connection to put the node into an unhealthy state
      coreContainer.getZkController().getZkClient().close();

      SolrException e =
          assertThrows(
              SolrException.class, () -> new NodeHealthAPI(coreContainer).checkNodeHealth(null));
      assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
      assertTrue(
          "Expected 'Host Unavailable' in exception message",
          e.getMessage().contains("Host Unavailable"));
    } finally {
      newJetty.stop();
    }
  }

  // ---- Legacy (standalone, non-ZooKeeper) mode tests ----

  @Test
  public void testLegacyMode_WithoutMaxGenerationLagReturnsOk() {
    NodeConfig nodeConfig =
        new NodeConfig.NodeConfigBuilder("testNode", createTempDir("solr-home"))
            .setUpdateShardHandlerConfig(UpdateShardHandlerConfig.TEST_DEFAULT)
            .setCoreRootDirectory(createTempDir("cores").toString())
            .build();

    CoreContainer container = new CoreContainer(nodeConfig);
    try {
      container.load();
      assertFalse("Standalone CoreContainer must not be ZK-aware", container.isZooKeeperAware());

      // In legacy mode with no maxGenerationLag, the health check always returns OK
      final var response = new NodeHealthAPI(container).checkNodeHealth(null, null);

      assertNotNull(response);
      assertEquals(OK, response.status);
      assertTrue(
          "Expected message about maxGenerationLag not being specified",
          response.message.contains("maxGenerationLag isn't specified"));
    } finally {
      container.shutdown();
    }
  }

  @Test
  public void testLegacyMode_WithNegativeMaxGenerationLagReturnsFailure() {
    NodeConfig nodeConfig =
        new NodeConfig.NodeConfigBuilder("testNode", createTempDir("solr-home"))
            .setUpdateShardHandlerConfig(UpdateShardHandlerConfig.TEST_DEFAULT)
            .setCoreRootDirectory(createTempDir("cores").toString())
            .build();

    CoreContainer container = new CoreContainer(nodeConfig);
    try {
      container.load();
      assertFalse("Standalone CoreContainer must not be ZK-aware", container.isZooKeeperAware());

      // A negative maxGenerationLag is invalid and should result in FAILURE status
      final var response = new NodeHealthAPI(container).checkNodeHealth(null, -1);

      assertNotNull(response);
      assertEquals(FAILURE, response.status);
      assertTrue(
          "Expected message about invalid maxGenerationLag",
          response.message.contains("Invalid value of maxGenerationLag"));
    } finally {
      container.shutdown();
    }
  }
}
