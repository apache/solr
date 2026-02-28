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

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.apache.solr.common.params.CommonParams.FAILURE;
import static org.apache.solr.common.params.CommonParams.OK;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.core.CoreContainer;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for the {@link NodeHealthAPI} JAX-RS implementation. */
public class NodeHealthAPITest extends SolrTestCase {

  private CoreContainer mockCoreContainer;
  private ZkController mockZkController;
  private ZkStateReader mockZkStateReader;
  private SolrZkClient mockZkClient;
  private ClusterState mockClusterState;

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  @Before
  public void setUpMocks() {
    mockCoreContainer = mock(CoreContainer.class);
    mockZkController = mock(ZkController.class);
    mockZkStateReader = mock(ZkStateReader.class);
    mockZkClient = mock(SolrZkClient.class);
    mockClusterState = mock(ClusterState.class);

    when(mockCoreContainer.isShutDown()).thenReturn(false);
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    when(mockCoreContainer.getZkController()).thenReturn(mockZkController);
    when(mockZkController.getZkStateReader()).thenReturn(mockZkStateReader);
    when(mockZkController.getNodeName()).thenReturn("localhost:8983_solr");
    when(mockZkStateReader.getClusterState()).thenReturn(mockClusterState);
    when(mockZkStateReader.getZkClient()).thenReturn(mockZkClient);
    when(mockZkClient.isClosed()).thenReturn(false);
    when(mockZkClient.isConnected()).thenReturn(true);
    when(mockClusterState.getLiveNodes()).thenReturn(Set.of("localhost:8983_solr"));
  }

  @Test
  public void testHealthyNodeReturnsOkStatus() {
    final var response = new NodeHealthAPI(mockCoreContainer).checkNodeHealth(null);

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertNull("Expected no error", response.error);
  }

  @Test
  public void testUnhealthyNodeWhenZkClientClosed() {
    when(mockZkClient.isClosed()).thenReturn(true);

    final var exception =
        assertThrows(
            SolrException.class, () -> new NodeHealthAPI(mockCoreContainer).checkNodeHealth(null));

    assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, exception.code());
    assertTrue(exception.getMessage().contains("Host Unavailable"));
  }

  @Test
  public void testUnhealthyNodeWhenNotInLiveNodes() {
    when(mockClusterState.getLiveNodes()).thenReturn(Set.of("othernode:8983_solr"));

    final var exception =
        assertThrows(
            SolrException.class, () -> new NodeHealthAPI(mockCoreContainer).checkNodeHealth(null));

    assertEquals(SolrException.ErrorCode.SERVICE_UNAVAILABLE.code, exception.code());
    assertTrue(exception.getMessage().contains("Host Unavailable"));
  }

  @Test
  public void testLegacyModeWithoutMaxGenerationLagReturnsOk() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);

    final var response = new NodeHealthAPI(mockCoreContainer).checkNodeHealth(null, null);

    assertNotNull(response);
    assertEquals(OK, response.status);
    assertNotNull(response.message);
    assertTrue(response.message.contains("maxGenerationLag isn't specified"));
  }

  @Test
  public void testLegacyModeWithInvalidMaxGenerationLagReturnsFailure() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);
    when(mockCoreContainer.getCores()).thenReturn(List.of());

    final var response = new NodeHealthAPI(mockCoreContainer).checkNodeHealth(null, -1);

    assertNotNull(response);
    assertEquals(FAILURE, response.status);
    assertNotNull(response.message);
    assertTrue(response.message.contains("Invalid value of maxGenerationLag"));
  }
}
