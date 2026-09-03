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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ClusterStateMockUtil;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for the cloud-mode logic of {@link NodeHealth}, using mocks instead of a real cluster.
 *
 * @see NodeHealthSolrCloudTest
 * @see NodeHealthStandaloneTest
 */
public class NodeHealthTest extends SolrTestCase {

  private static final String NODE_NAME = "baseUrl1:8983_";

  private CoreContainer mockCoreContainer;
  private ZkController mockZkController;
  private ZkStateReader mockZkStateReader;
  private SolrZkClient mockZkClient;
  private NodeHealth nodeHealth;

  @BeforeClass
  public static void ensureWorkingMockito() {
    SolrTestCaseJ4.assumeWorkingMockito();
  }

  @Before
  public void setupMocks() {
    mockCoreContainer = mock(CoreContainer.class);
    mockZkController = mock(ZkController.class);
    mockZkStateReader = mock(ZkStateReader.class);
    mockZkClient = mock(SolrZkClient.class);

    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    when(mockCoreContainer.getZkController()).thenReturn(mockZkController);
    when(mockZkController.getZkStateReader()).thenReturn(mockZkStateReader);
    when(mockZkController.getNodeName()).thenReturn(NODE_NAME);
    when(mockZkStateReader.getZkClient()).thenReturn(mockZkClient);
    when(mockZkClient.isConnected()).thenReturn(true);
    when(mockZkClient.isClosed()).thenReturn(false);
    when(mockZkStateReader.getClusterState())
        .thenReturn(new ClusterState(Set.of(NODE_NAME), Map.of()));

    nodeHealth = new NodeHealth(mockCoreContainer);
  }

  @Test
  public void testUninitializedCoreContainerReturns500() {
    SolrException e =
        expectThrows(SolrException.class, () -> new NodeHealth(null).healthcheck(null, null));
    assertEquals(ErrorCode.SERVER_ERROR.code, e.code());

    when(mockCoreContainer.isShutDown()).thenReturn(true);
    e = expectThrows(SolrException.class, () -> nodeHealth.healthcheck(null, null));
    assertEquals(ErrorCode.SERVER_ERROR.code, e.code());
  }

  @Test
  public void testHealthyNodeReturnsOkStatus() {
    final var response = nodeHealth.healthcheck(null, null);

    assertEquals(OK, response.status);
    assertNull("Expected no error on a healthy node", response.error);
  }

  @Test
  public void testZkClientClosedReturns503() {
    when(mockZkClient.isClosed()).thenReturn(true);

    SolrException e = expectThrows(SolrException.class, () -> nodeHealth.healthcheck(null, null));
    assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
    assertThat(e.getMessage(), containsString("Not connected to zk"));
  }

  @Test
  public void testZkClientDisconnectedReturns503() {
    when(mockZkClient.isConnected()).thenReturn(false);

    SolrException e = expectThrows(SolrException.class, () -> nodeHealth.healthcheck(null, null));
    assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
    assertThat(e.getMessage(), containsString("Not connected to zk"));
  }

  @Test
  public void testNotInLiveNodesReturns503() {
    when(mockZkStateReader.getClusterState())
        .thenReturn(new ClusterState(Set.of("someOtherNode:8983_"), Map.of()));

    SolrException e = expectThrows(SolrException.class, () -> nodeHealth.healthcheck(null, null));
    assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
    assertThat(e.getMessage(), containsString("Not in live nodes"));
  }

  @Test
  public void testCoreLoadingNotCompleteReturns503() {
    when(mockCoreContainer.isStatusLoadComplete()).thenReturn(false);

    SolrException e = expectThrows(SolrException.class, () -> nodeHealth.healthcheck(true, null));
    assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
    assertThat(e.getMessage(), containsString("Core Loading not complete"));
  }

  @Test
  public void testRequireHealthyCoresReturnsOkWhenAllCoresHealthy() {
    when(mockCoreContainer.isStatusLoadComplete()).thenReturn(true);
    CoreDescriptor activeCore = mockCoreDescriptor(Replica.State.ACTIVE);
    when(mockCoreContainer.getCoreDescriptors()).thenReturn(List.of(activeCore));

    final var response = nodeHealth.healthcheck(true, null);

    assertEquals(OK, response.status);
    assertEquals("All cores are healthy", response.message);
  }

  @Test
  public void testUnhealthyCoresReturns503() {
    when(mockCoreContainer.isStatusLoadComplete()).thenReturn(true);
    CoreDescriptor recoveringCore = mockCoreDescriptor(Replica.State.RECOVERING);
    when(mockCoreContainer.getCoreDescriptors()).thenReturn(List.of(recoveringCore));
    when(mockCoreContainer.getNumAllCores()).thenReturn(1);

    SolrException e = expectThrows(SolrException.class, () -> nodeHealth.healthcheck(true, null));
    assertEquals(ErrorCode.SERVICE_UNAVAILABLE.code, e.code());
    assertThat(
        e.getMessage(),
        containsString("1 out of 1 replicas are currently initializing or recovering"));
  }

  /**
   * Creates a core descriptor for a core of collection1/slice1 in the given state, and points the
   * mocked cluster state at a matching collection.
   */
  private CoreDescriptor mockCoreDescriptor(Replica.State state) {
    CoreDescriptor coreDescriptor =
        new CoreDescriptor(
            "slice1_replica1",
            createTempDir(),
            Map.of(
                CoreDescriptor.CORE_SHARD, "slice1",
                CoreDescriptor.CORE_COLLECTION, "collection1",
                CoreDescriptor.CORE_NODE_NAME, "slice1_replica1"),
            new Properties(),
            mockZkController);
    CloudDescriptor cloudDescriptor = coreDescriptor.getCloudDescriptor();
    cloudDescriptor.setHasRegistered(true);
    cloudDescriptor.setLastPublished(state);

    // collection1 with slice1 holding one active replica, on our (live) node
    try (ZkStateReader stateReader = ClusterStateMockUtil.buildClusterState("csr", NODE_NAME)) {
      when(mockZkStateReader.getClusterState()).thenReturn(stateReader.getClusterState());
    }

    return coreDescriptor;
  }
}
