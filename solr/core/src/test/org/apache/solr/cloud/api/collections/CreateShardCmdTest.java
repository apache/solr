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
package org.apache.solr.cloud.api.collections;

import static org.apache.solr.SolrTestCaseJ4.assumeWorkingMockito;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.plugins.SimplePlacementFactory;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for {@link CreateShardCmd} failure handling. The cluster collaborators are mocked so
 * the replica wait can be made to time out deterministically: the replica is present in the cluster
 * state (so the wait's first phase passes) but no state watcher ever reports it active, so the
 * wait's latch times out.
 */
public class CreateShardCmdTest extends SolrTestCase {

  private static final String COLLECTION = "testcoll";
  private static final String SHARD = "shard2";
  private static final String NODE1 = "node1:8983_solr";
  private static final String NODE2 = "node2:8983_solr";

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testReplicaWaitFailureDeletesShardInsteadOfActivating() throws Exception {
    // Cluster states in the ZooKeeper JSON shape, parsed by the production factory.
    // Before the create: the collection has one unrelated slice, no shard2.
    DocCollection before =
        ClusterState.collectionFromObjects(
            COLLECTION,
            new HashMap<>(
                Map.of(
                    "configName",
                    "conf1",
                    "shards",
                    Map.of(
                        "shard1",
                        Map.of(
                            "state",
                            "active",
                            "range",
                            "80000000-ffffffff",
                            "replicas",
                            Map.of())))),
            1,
            Instant.now(),
            null);
    // Right after the shard state update lands: shard2 exists, still no replicas.
    DocCollection shardCreated =
        ClusterState.collectionFromObjects(
            COLLECTION,
            new HashMap<>(
                Map.of(
                    "configName",
                    "conf1",
                    "shards",
                    Map.of(
                        "shard1",
                        Map.of(
                            "state", "active", "range", "80000000-7fffffff", "replicas", Map.of()),
                        SHARD,
                        Map.of(
                            "state",
                            "construction",
                            "range",
                            "80000000-ffffffff",
                            "replicas",
                            Map.of())))),
            2,
            Instant.now(),
            null);
    // After AddReplicaCmd: shard2 has one replica, registered but never active.
    DocCollection replicaAdded =
        ClusterState.collectionFromObjects(
            COLLECTION,
            new HashMap<>(
                Map.of(
                    "configName",
                    "conf1",
                    "shards",
                    Map.of(
                        "shard1",
                        Map.of(
                            "state", "active", "range", "80000000-7fffffff", "replicas", Map.of()),
                        SHARD,
                        Map.of(
                            "state",
                            "construction",
                            "range",
                            "80000000-ffffffff",
                            "replicas",
                            Map.of(
                                "core_node1",
                                Map.of(
                                    "core",
                                    "testcoll_shard2_replica_n1",
                                    "node_name",
                                    NODE1,
                                    "base_url",
                                    "http://node1:8983/solr",
                                    "state",
                                    "down",
                                    "leader",
                                    "true",
                                    "type",
                                    "NRT")))))),
            3,
            Instant.now(),
            null);
    Set<String> liveNodes = Set.of(NODE1, NODE2);
    ClusterState beforeState = new ClusterState(liveNodes, Map.of(COLLECTION, before));
    ClusterState createdState = new ClusterState(liveNodes, Map.of(COLLECTION, shardCreated));
    ClusterState replicaState = new ClusterState(liveNodes, Map.of(COLLECTION, replicaAdded));

    ZkStateReader zkStateReader = mock(ZkStateReader.class);
    // The first read is waitForNewShard picking up the new slice; later reads see the replica.
    when(zkStateReader.getClusterState()).thenReturn(createdState, replicaState);
    // Every wait is answered against the replica-bearing state without ever timing out at this
    // level; the replica wait in CreateShardCmd still times out because its active-replica
    // watcher (a separate registration) is never notified.
    doAnswer(
            invocation -> {
              Predicate<DocCollection> predicate = invocation.getArgument(3);
              predicate.test(replicaAdded);
              return replicaAdded;
            })
        .when(zkStateReader)
        .waitForState(anyString(), anyLong(), any(TimeUnit.class), any(Predicate.class));

    ClusterStateProvider stateProvider = mock(ClusterStateProvider.class);
    when(stateProvider.getClusterState()).thenReturn(replicaState);
    when(stateProvider.getLiveNodes()).thenReturn(liveNodes);
    SolrCloudManager cloudManager = mock(SolrCloudManager.class);
    when(cloudManager.getClusterStateProvider()).thenReturn(stateProvider);
    when(cloudManager.getClusterState()).thenReturn(replicaState);
    when(cloudManager.getTimeSource()).thenReturn(new TimeSource.NanoTimeSource());
    when(cloudManager.getDistribStateManager()).thenReturn(mock(DistribStateManager.class));

    // The shard handler accepts the CREATE request and reports success for it, once.
    ShardHandler shardHandler = mock(ShardHandler.class);
    ShardHandlerFactory shardHandlerFactory = mock(ShardHandlerFactory.class);
    when(shardHandler.getShardHandlerFactory()).thenReturn(shardHandlerFactory);
    when(shardHandlerFactory.getShardHandler()).thenReturn(shardHandler);
    AtomicReference<ShardRequest> submitted = new AtomicReference<>();
    AtomicBoolean responded = new AtomicBoolean();
    doAnswer(
            invocation -> {
              submitted.set(invocation.getArgument(0));
              return null;
            })
        .when(shardHandler)
        .submit(any(ShardRequest.class), any(), any(ModifiableSolrParams.class));
    Answer<ShardResponse> takeAnswer =
        invocation -> {
          if (submitted.get() == null || !responded.compareAndSet(false, true)) {
            return null;
          }
          ShardResponse response = new ShardResponse();
          response.setShardRequest(submitted.get());
          QueryResponse queryResponse = new QueryResponse();
          queryResponse.setResponse(
              new NamedList<>(Map.of("responseHeader", new NamedList<>(Map.of("status", 0)))));
          response.setSolrResponse(queryResponse);
          return response;
        };
    when(shardHandler.takeCompletedOrError()).thenAnswer(takeAnswer);
    when(shardHandler.takeCompletedIncludingErrors()).thenAnswer(takeAnswer);

    DistributedClusterStateUpdater stateUpdater = mock(DistributedClusterStateUpdater.class);
    when(stateUpdater.isDistributedStateUpdate()).thenReturn(false);

    CoreContainer coreContainer = mock(CoreContainer.class);
    PlacementPluginFactory placementFactory = mock(PlacementPluginFactory.class);
    when(placementFactory.createPluginInstance())
        .thenReturn(new SimplePlacementFactory().createPluginInstance());
    when(coreContainer.getPlacementPluginFactory()).thenReturn(placementFactory);
    ZkController zkController = mock(ZkController.class);
    when(coreContainer.getZkController()).thenReturn(zkController);
    when(zkController.getNodeName()).thenReturn(NODE1);

    CollectionCommandContext ccc = mock(CollectionCommandContext.class);
    when(ccc.isDistributedCollectionAPI()).thenReturn(false);
    when(ccc.newShardHandler()).thenReturn(shardHandler);
    when(ccc.getSolrCloudManager()).thenReturn(cloudManager);
    when(ccc.getZkStateReader()).thenReturn(zkStateReader);
    when(ccc.getDistributedClusterStateUpdater()).thenReturn(stateUpdater);
    when(ccc.getCoreContainer()).thenReturn(coreContainer);
    when(ccc.getAdminPath()).thenReturn("/admin/collections");
    when(ccc.getCloseableToLatchOn()).thenReturn(mock(SolrCloseable.class));
    // Record every cluster state update the command offers.
    List<Map<String, Object>> offeredUpdates = new ArrayList<>();
    doAnswer(
            invocation -> {
              Object update = invocation.getArgument(0);
              if (update instanceof ZkNodeProps zkNodeProps) {
                offeredUpdates.add(new HashMap<>(zkNodeProps.getProperties()));
              } else {
                offeredUpdates.add(
                    new HashMap<>((Map<String, Object>) Utils.fromJSON(Utils.toJSON(update))));
              }
              return null;
            })
        .when(ccc)
        .offerStateUpdate(any(MapWriter.class));

    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionAction.CREATESHARD).withClusterState(beforeState);
    ZkNodeProps message =
        new ZkNodeProps(
            Map.of(
                "collection",
                COLLECTION,
                "shard",
                SHARD,
                "timeout",
                1,
                "replicationFactor",
                1,
                "createNodeSet",
                NODE1));

    Exception thrown = null;
    try {
      new CreateShardCmd(ccc).call(adminCmdContext, message, new NamedList<>());
    } catch (Exception e) {
      thrown = e;
    }
    assertNotNull("expected the create to fail in the replica wait", thrown);
    if (!String.valueOf(thrown.getMessage()).contains("replicas of shard")) {
      throw new AssertionError("expected the replica wait timeout, got: " + thrown, thrown);
    }

    List<String> operations = new ArrayList<>();
    for (Map<String, Object> update : offeredUpdates) {
      operations.add(String.valueOf(update.get("operation")));
    }
    assertTrue(
        "expected a deleteshard state update, offered: " + offeredUpdates,
        operations.contains("deleteshard"));
    for (Map<String, Object> update : offeredUpdates) {
      if ("updateshardstate".equalsIgnoreCase(String.valueOf(update.get("operation")))) {
        fail("the failed shard must not be activated, offered: " + update);
      }
    }
  }
}
