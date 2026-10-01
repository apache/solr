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
package org.apache.solr.cluster.placement.impl;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ReplicaCount;
import org.junit.BeforeClass;
import org.junit.Test;

/** Unit tests for {@link PlacementPluginAssignStrategy}. */
public class PlacementPluginAssignStrategyTest extends SolrTestCase {

  @BeforeClass
  public static void ensureWorkingMockito() {
    assumeWorkingMockito();
  }

  /**
   * SOLR-18391: when the collection is missing from cluster state (race between {@code
   * CreateCollectionCmd.waitForState} and the subsequent cluster-state read), {@code assign} must
   * fail with {@link Assign.AssignmentException} so the collection create is cleaned up, instead of
   * throwing an NPE and leaving a zombie collection with no replicas.
   */
  @Test
  public void testAssignFailsCleanlyWhenCollectionMissingFromClusterState() throws Exception {
    SolrCloudManager cloudManager = mock(SolrCloudManager.class);
    ClusterStateProvider stateProvider = mock(ClusterStateProvider.class);
    when(stateProvider.getLiveNodes()).thenReturn(Set.of("node1:8983_solr"));
    when(cloudManager.getClusterStateProvider()).thenReturn(stateProvider);
    // No node-role data in ZK: every live node keeps its data role.
    DistribStateManager distribStateManager = mock(DistribStateManager.class);
    when(distribStateManager.listData(anyString())).thenThrow(new NoSuchElementException());
    when(cloudManager.getDistribStateManager()).thenReturn(distribStateManager);
    // Cluster state has live nodes but no collections: the collection vanished (or never
    // appeared) in the race window.
    when(cloudManager.getClusterState())
        .thenReturn(new ClusterState(Set.of("node1:8983_solr"), Map.of()));

    PlacementPluginAssignStrategy strategy =
        new PlacementPluginAssignStrategy(mock(PlacementPlugin.class));
    Assign.AssignRequest request =
        new Assign.AssignRequest(
            "missing-collection", List.of("shard1"), null, ReplicaCount.empty());

    Assign.AssignmentException e =
        expectThrows(
            Assign.AssignmentException.class,
            () -> strategy.assign(cloudManager, List.of(request)));
    assertTrue(
        "expected the collection name in the failure message, got: " + e.getMessage(),
        e.getMessage().contains("missing-collection"));
  }
}
