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
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
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
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.component.ShardResponse;
import org.apache.solr.handler.component.ShardResponseTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

/**
 * Unit tests for {@link CreateShardCmd} failure handling and activation. The cluster collaborators
 * are mocked and the simulated cluster state advances as the command offers state updates, so each
 * phase of the create can be made to fail deterministically.
 */
public class CreateShardCmdTest extends SolrTestCase {

  private static final String COLLECTION = "testcoll";
  private static final String SHARD = "shard2";
  private static final String NODE1 = "node1:8983_solr";
  private static final String NODE2 = "node2:8983_solr";
  private static final String LEADER_CORE = "testcoll_shard2_replica_n1";

  @Before
  public void setUpMocks() {
    assumeWorkingMockito();
  }

  /**
   * Builds a simulated collection state in the ZooKeeper JSON shape, parsed by the production
   * factory. shard2 is absent when {@code shard2State} is null; it has one replica, marked as
   * leader, when {@code replicaState} is non-null (the replica's state).
   */
  private static DocCollection collectionState(
      int version, String shard2State, String replicaState) {
    Map<String, Object> shard1 = new HashMap<>();
    shard1.put("state", "active");
    shard1.put("range", shard2State == null ? "80000000-ffffffff" : "80000000-7fffffff");
    shard1.put("replicas", Map.of());
    Map<String, Object> shards = new HashMap<>();
    shards.put("shard1", shard1);
    if (shard2State != null) {
      Map<String, Object> replicas = new HashMap<>();
      if (replicaState != null) {
        Map<String, Object> replica = new HashMap<>();
        replica.put("core", LEADER_CORE);
        replica.put("node_name", NODE1);
        replica.put("base_url", "http://node1:8983/solr");
        replica.put("state", replicaState);
        replica.put("leader", "true");
        replica.put("type", "NRT");
        replicas.put("core_node1", replica);
      }
      Map<String, Object> shard2 = new HashMap<>();
      shard2.put("state", shard2State);
      shard2.put("range", "80000000-ffffffff");
      shard2.put("replicas", replicas);
      shards.put(SHARD, shard2);
    }
    Map<String, Object> collection = new HashMap<>();
    collection.put("configName", "conf1");
    collection.put("shards", shards);
    return ClusterState.collectionFromObjects(COLLECTION, collection, version, Instant.now(), null);
  }

  /**
   * A shard handler that records every submitted request and answers each with a success response,
   * except the requests {@code failureFor} fails by returning an exception for their params. Every
   * submitted request's params are also appended to {@code allSubmitted}.
   */
  private static ShardHandler shardHandlerResponding(
      List<ModifiableSolrParams> allSubmitted,
      Function<ModifiableSolrParams, Throwable> failureFor) {
    ShardHandler shardHandler = mock(ShardHandler.class);
    ShardHandlerFactory shardHandlerFactory = mock(ShardHandlerFactory.class);
    when(shardHandler.getShardHandlerFactory()).thenReturn(shardHandlerFactory);
    when(shardHandlerFactory.getShardHandler()).thenReturn(shardHandler);
    Queue<ShardRequest> pendingRequests = new ConcurrentLinkedQueue<>();
    Queue<ModifiableSolrParams> pendingParams = new ConcurrentLinkedQueue<>();
    doAnswer(
            invocation -> {
              pendingRequests.add(invocation.getArgument(0));
              ModifiableSolrParams params = invocation.getArgument(2);
              pendingParams.add(params);
              allSubmitted.add(params);
              return null;
            })
        .when(shardHandler)
        .submit(any(ShardRequest.class), any(), any(ModifiableSolrParams.class));
    Answer<ShardResponse> takeAnswer =
        invocation -> {
          ShardRequest sreq = pendingRequests.poll();
          ModifiableSolrParams params = pendingParams.poll();
          if (sreq == null) {
            return null;
          }
          Throwable failure = params == null ? null : failureFor.apply(params);
          if (failure != null) {
            return ShardResponseTestSupport.failedResponse(sreq, failure);
          }
          ShardResponse response = new ShardResponse();
          response.setShardRequest(sreq);
          QueryResponse queryResponse = new QueryResponse();
          queryResponse.setResponse(
              new NamedList<>(Map.of("responseHeader", new NamedList<>(Map.of("status", 0)))));
          response.setSolrResponse(queryResponse);
          return response;
        };
    when(shardHandler.takeCompletedOrError()).thenAnswer(takeAnswer);
    when(shardHandler.takeCompletedIncludingErrors()).thenAnswer(takeAnswer);
    return shardHandler;
  }

  private static boolean submittedAction(
      List<ModifiableSolrParams> submitted, CoreAdminParams.CoreAdminAction action) {
    return submitted.stream()
        .anyMatch(params -> action.toString().equals(params.get(CoreAdminParams.ACTION)));
  }

  /**
   * The replica is registered and {@code AddReplicaCmd}'s own final-state wait sees it active, but
   * the watcher {@code CreateShardCmd} registers for its activation wait never reports it active,
   * so that wait times out. The create must fail without deleting or activating the shard: by then
   * its replica can already hold acknowledged writes, so the shard is left in CONSTRUCTION state
   * for the operator.
   */
  @Test
  public void testReplicaWaitFailureLeavesShardInConstruction() throws Exception {
    DocCollection before = collectionState(1, null, null);
    DocCollection shardCreated = collectionState(2, "construction", null);
    DocCollection replicaAdded = collectionState(3, "construction", "down");
    DocCollection replicaActive = collectionState(4, "construction", "active");

    MockEnvironment env =
        new MockEnvironment(
            shardCreated,
            replicaAdded,
            Map.of(
                "createshard", shardCreated,
                "addreplica", replicaAdded,
                "deletecore", shardCreated,
                "deleteshard", before));
    // Report the replica active to the first watcher registered (the one AddReplicaCmd waits
    // on); the watcher CreateShardCmd registers afterwards never fires.
    AtomicBoolean firstRegistration = new AtomicBoolean(true);
    env.watcherHook.set(
        watcher -> {
          if (firstRegistration.compareAndSet(true, false)) {
            watcher.onStateChanged(env.liveNodes, replicaActive);
          }
        });
    env.useShardHandler(shardHandlerResponding(new ArrayList<>(), params -> null));

    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionAction.CREATESHARD)
            .withClusterState(new ClusterState(env.liveNodes, Map.of(COLLECTION, before)));
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
                NODE1,
                "waitForFinalState",
                true));

    Exception thrown = null;
    try {
      new CreateShardCmd(env.ccc).call(adminCmdContext, message, new NamedList<>());
    } catch (Exception e) {
      thrown = e;
    }
    assertNotNull("expected the create to fail in the replica wait", thrown);
    if (!String.valueOf(thrown.getMessage()).contains("replicas of shard")) {
      throw new AssertionError("expected the replica wait timeout, got: " + thrown, thrown);
    }

    List<String> operations = env.offeredOperations();
    assertFalse(
        "the shard must not be deleted over an activation-phase failure, offered: "
            + env.offeredUpdates,
        operations.contains("deleteshard"));
    assertFalse(
        "the replica must not be deleted over an activation-phase failure, offered: "
            + env.offeredUpdates,
        operations.contains("deletecore"));
    assertFalse(
        "the failed shard must not be activated, offered: " + env.offeredUpdates,
        operations.contains("updateshardstate"));
    Slice slice = env.currentDoc.get().getSlice(SHARD);
    assertNotNull("expected the shard to be left in place", slice);
    assertEquals(Slice.State.CONSTRUCTION, slice.getState());
  }

  /**
   * Drives the first cleanup block, the one around adding the replicas. The replica is registered
   * in the cluster state first and creating its core fails afterwards, so {@code AddReplicaCmd}
   * throws with the replica already in place. The create must then delete the half-created shard,
   * including that replica, and the caller must see the original add failure rather than a cleanup
   * result.
   */
  @Test
  public void testAddReplicaFailureDeletesShardAndReplica() throws Exception {
    DocCollection before = collectionState(1, null, null);
    DocCollection shardCreated = collectionState(2, "construction", null);
    DocCollection replicaAdded = collectionState(3, "construction", "down");

    MockEnvironment env =
        new MockEnvironment(
            shardCreated,
            replicaAdded,
            Map.of(
                "createshard", shardCreated,
                "addreplica", replicaAdded,
                "deletecore", shardCreated,
                "deleteshard", before));
    env.useShardHandler(
        shardHandlerResponding(
            new ArrayList<>(),
            params ->
                CoreAdminParams.CoreAdminAction.CREATE
                        .toString()
                        .equals(params.get(CoreAdminParams.ACTION))
                    ? new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR, "simulated core creation failure")
                    : null));

    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionAction.CREATESHARD)
            .withClusterState(new ClusterState(env.liveNodes, Map.of(COLLECTION, before)));
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
      new CreateShardCmd(env.ccc).call(adminCmdContext, message, new NamedList<>());
    } catch (Exception e) {
      thrown = e;
    }
    assertNotNull("expected the create to fail when creating the replica's core fails", thrown);
    if (!String.valueOf(thrown.getMessage()).contains("ADDREPLICA failed to create replica")) {
      throw new AssertionError("expected the add replica failure, got: " + thrown, thrown);
    }

    List<String> operations = env.offeredOperations();
    assertTrue(
        "expected the cleanup to delete the replica that was registered before the failure,"
            + " offered: "
            + env.offeredUpdates,
        operations.contains("deletecore"));
    for (Map<String, Object> update : env.offeredUpdates) {
      if ("deletecore".equals(String.valueOf(update.get("operation")))) {
        assertEquals(
            "the cleanup must delete the replica that AddReplicaCmd registered",
            LEADER_CORE,
            String.valueOf(update.get("core")));
      }
      if ("updateshardstate".equalsIgnoreCase(String.valueOf(update.get("operation")))) {
        fail("the failed shard must not be activated, offered: " + update);
      }
    }
    assertTrue(
        "expected a deleteshard state update, offered: " + env.offeredUpdates,
        operations.contains("deleteshard"));
    assertNull(
        "expected no slice left behind after the cleanup", env.currentDoc.get().getSlice(SHARD));
  }

  /**
   * The leader refuses REQUESTAPPLYUPDATES because its core is not buffering updates, which is the
   * normal case for a shard nobody wrote to while it was under construction. That refusal alone
   * must not fail the create or delete the shard; the shard is still activated.
   */
  @Test
  public void testNotBufferingRefusalDoesNotFailCreate() throws Exception {
    DocCollection before = collectionState(1, null, null);
    DocCollection shardCreated = collectionState(2, "construction", null);
    DocCollection replicaActive = collectionState(3, "construction", "active");
    DocCollection shardActive = collectionState(4, "active", "active");

    MockEnvironment env =
        new MockEnvironment(
            shardCreated,
            replicaActive,
            Map.of(
                "createshard", shardCreated,
                "addreplica", replicaActive,
                "updateshardstate", shardActive));
    env.watcherHook.set(watcher -> watcher.onStateChanged(env.liveNodes, env.currentDoc.get()));
    List<ModifiableSolrParams> submitted = new ArrayList<>();
    env.useShardHandler(
        shardHandlerResponding(
            submitted,
            params ->
                CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES
                        .toString()
                        .equals(params.get(CoreAdminParams.ACTION))
                    ? new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR,
                        "Core " + LEADER_CORE + " not in buffering state")
                    : null));

    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionAction.CREATESHARD)
            .withClusterState(new ClusterState(env.liveNodes, Map.of(COLLECTION, before)));
    ZkNodeProps message =
        new ZkNodeProps(
            Map.of(
                "collection",
                COLLECTION,
                "shard",
                SHARD,
                "timeout",
                5,
                "replicationFactor",
                1,
                "createNodeSet",
                NODE1,
                "waitForFinalState",
                true));

    Exception thrown = null;
    try {
      new CreateShardCmd(env.ccc).call(adminCmdContext, message, new NamedList<>());
    } catch (Exception e) {
      thrown = e;
    }
    assertNull("the create must not fail when the leader was not buffering updates", thrown);
    assertTrue(
        "expected the leader to be asked to apply buffered updates",
        submittedAction(submitted, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES));

    List<String> operations = env.offeredOperations();
    assertFalse(
        "the shard must not be deleted over the not-buffering refusal, offered: "
            + env.offeredUpdates,
        operations.contains("deleteshard"));
    boolean activated =
        env.offeredUpdates.stream()
            .anyMatch(
                update ->
                    "updateshardstate".equalsIgnoreCase(String.valueOf(update.get("operation")))
                        && "active".equalsIgnoreCase(String.valueOf(update.get(SHARD))));
    assertTrue(
        "expected the shard to be activated despite the refusal, offered: " + env.offeredUpdates,
        activated);
  }

  /**
   * A REQUESTAPPLYUPDATES failure that is not the not-buffering refusal means buffered updates were
   * not replayed. The create must fail rather than activate the shard over them, and the shard must
   * be left in CONSTRUCTION state, not deleted with the writes it holds.
   */
  @Test
  public void testReplayFailureFailsCreateAndLeavesShard() throws Exception {
    DocCollection before = collectionState(1, null, null);
    DocCollection shardCreated = collectionState(2, "construction", null);
    DocCollection replicaActive = collectionState(3, "construction", "active");
    DocCollection shardActive = collectionState(4, "active", "active");

    MockEnvironment env =
        new MockEnvironment(
            shardCreated,
            replicaActive,
            Map.of(
                "createshard", shardCreated,
                "addreplica", replicaActive,
                "updateshardstate", shardActive));
    env.watcherHook.set(watcher -> watcher.onStateChanged(env.liveNodes, env.currentDoc.get()));
    List<ModifiableSolrParams> submitted = new ArrayList<>();
    env.useShardHandler(
        shardHandlerResponding(
            submitted,
            params ->
                CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES
                        .toString()
                        .equals(params.get(CoreAdminParams.ACTION))
                    ? new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Replay failed")
                    : null));

    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionAction.CREATESHARD)
            .withClusterState(new ClusterState(env.liveNodes, Map.of(COLLECTION, before)));
    ZkNodeProps message =
        new ZkNodeProps(
            Map.of(
                "collection",
                COLLECTION,
                "shard",
                SHARD,
                "timeout",
                5,
                "replicationFactor",
                1,
                "createNodeSet",
                NODE1,
                "waitForFinalState",
                true));

    Exception thrown = null;
    try {
      new CreateShardCmd(env.ccc).call(adminCmdContext, message, new NamedList<>());
    } catch (Exception e) {
      thrown = e;
    }
    assertNotNull("expected the create to fail when replaying buffered updates fails", thrown);
    if (!String.valueOf(thrown.getMessage()).contains("failed to apply buffered updates")) {
      throw new AssertionError("expected the replay failure, got: " + thrown, thrown);
    }
    assertTrue(
        "expected the leader to be asked to apply buffered updates",
        submittedAction(submitted, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES));

    List<String> operations = env.offeredOperations();
    assertFalse(
        "the shard must not be activated over unreplayed updates, offered: " + env.offeredUpdates,
        operations.contains("updateshardstate"));
    assertFalse(
        "the shard must not be deleted over a replay failure, offered: " + env.offeredUpdates,
        operations.contains("deleteshard"));
    Slice slice = env.currentDoc.get().getSlice(SHARD);
    assertNotNull("expected the shard to be left in place", slice);
    assertEquals(Slice.State.CONSTRUCTION, slice.getState());
  }

  /**
   * With createNodeSet=EMPTY (the v2 API's createReplicas=false) the shard is created without
   * replicas: the add step is skipped entirely (AddReplicaCmd would reject "EMPTY" as an unknown
   * node name), there is nothing to wait for and no leader to apply buffered updates on, and the
   * shard is activated right away.
   */
  @Test
  public void testCreateShardWithoutReplicasActivatesShard() throws Exception {
    DocCollection before = collectionState(1, null, null);
    DocCollection shardCreated = collectionState(2, "construction", null);
    DocCollection shardActive = collectionState(3, "active", null);

    MockEnvironment env =
        new MockEnvironment(
            shardCreated,
            shardCreated,
            Map.of(
                "createshard", shardCreated,
                "updateshardstate", shardActive));
    List<ModifiableSolrParams> submitted = new ArrayList<>();
    env.useShardHandler(shardHandlerResponding(submitted, params -> null));

    AdminCmdContext adminCmdContext =
        new AdminCmdContext(CollectionAction.CREATESHARD)
            .withClusterState(new ClusterState(env.liveNodes, Map.of(COLLECTION, before)));
    ZkNodeProps message =
        new ZkNodeProps(
            Map.of(
                "collection",
                COLLECTION,
                "shard",
                SHARD,
                "timeout",
                5,
                "replicationFactor",
                1,
                "createNodeSet",
                CollectionHandlingUtils.CREATE_NODE_SET_EMPTY));

    Exception thrown = null;
    try {
      new CreateShardCmd(env.ccc).call(adminCmdContext, message, new NamedList<>());
    } catch (Exception e) {
      thrown = e;
    }
    assertNull("creating a shard without replicas must not fail", thrown);
    assertTrue(
        "no core request may be submitted when no replicas are created, submitted: " + submitted,
        submitted.isEmpty());

    List<String> operations = env.offeredOperations();
    assertFalse(
        "the shard must not be deleted, offered: " + env.offeredUpdates,
        operations.contains("deleteshard"));
    boolean activated =
        env.offeredUpdates.stream()
            .anyMatch(
                update ->
                    "updateshardstate".equalsIgnoreCase(String.valueOf(update.get("operation")))
                        && "active".equalsIgnoreCase(String.valueOf(update.get(SHARD))));
    assertTrue("expected the shard to be activated, offered: " + env.offeredUpdates, activated);
    Slice slice = env.currentDoc.get().getSlice(SHARD);
    assertNotNull("expected the shard to exist", slice);
    assertEquals(Slice.State.ACTIVE, slice.getState());
    assertTrue("expected the shard to have no replicas", slice.getReplicas().isEmpty());
  }

  /**
   * The mocked overseer-side environment the command runs against. The simulated cluster state
   * starts at a given state and advances through a transition map (operation name to the state that
   * offering an update with that operation produces); {@code getClusterState} reports the current
   * simulated state, and {@code waitForState} answers its predicate against it, timing out like the
   * real implementation when the predicate does not hold. Every offered update is recorded in
   * {@link #offeredUpdates}. Cluster state watchers registered with the reader are handed to {@link
   * #watcherHook}, which does nothing by default.
   */
  private static final class MockEnvironment {
    final Set<String> liveNodes = Set.of(NODE1, NODE2);
    final AtomicReference<DocCollection> currentDoc;
    final List<Map<String, Object>> offeredUpdates = new ArrayList<>();
    final AtomicReference<Consumer<CollectionStateWatcher>> watcherHook =
        new AtomicReference<>(watcher -> {});
    final CollectionCommandContext ccc = mock(CollectionCommandContext.class);

    @SuppressWarnings({"unchecked", "rawtypes"})
    MockEnvironment(
        DocCollection initial, DocCollection placementState, Map<String, DocCollection> transitions)
        throws Exception {
      currentDoc = new AtomicReference<>(initial);
      ZkStateReader zkStateReader = mock(ZkStateReader.class);
      when(zkStateReader.getClusterState())
          .thenAnswer(
              invocation -> new ClusterState(liveNodes, Map.of(COLLECTION, currentDoc.get())));
      doAnswer(
              invocation -> {
                Predicate<DocCollection> predicate = invocation.getArgument(3);
                DocCollection current = currentDoc.get();
                if (predicate.test(current)) {
                  return current;
                }
                throw new TimeoutException("simulated state never satisfied the wait predicate");
              })
          .when(zkStateReader)
          .waitForState(anyString(), anyLong(), any(TimeUnit.class), any(Predicate.class));
      doAnswer(
              invocation -> {
                watcherHook.get().accept(invocation.getArgument(1));
                return null;
              })
          .when(zkStateReader)
          .registerCollectionStateWatcher(anyString(), any(CollectionStateWatcher.class));
      // DeleteShardCmd cleans up shard metadata in ZooKeeper after deleting a shard.
      when(zkStateReader.getZkClient()).thenReturn(mock(SolrZkClient.class));

      ClusterStateProvider stateProvider = mock(ClusterStateProvider.class);
      ClusterState placementClusterState =
          new ClusterState(liveNodes, Map.of(COLLECTION, placementState));
      when(stateProvider.getClusterState()).thenReturn(placementClusterState);
      when(stateProvider.getLiveNodes()).thenReturn(liveNodes);
      SolrCloudManager cloudManager = mock(SolrCloudManager.class);
      when(cloudManager.getClusterStateProvider()).thenReturn(stateProvider);
      when(cloudManager.getClusterState()).thenReturn(placementClusterState);
      when(cloudManager.getTimeSource()).thenReturn(new TimeSource.NanoTimeSource());
      when(cloudManager.getDistribStateManager()).thenReturn(mock(DistribStateManager.class));

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

      when(ccc.isDistributedCollectionAPI()).thenReturn(false);
      when(ccc.getSolrCloudManager()).thenReturn(cloudManager);
      when(ccc.getZkStateReader()).thenReturn(zkStateReader);
      when(ccc.getDistributedClusterStateUpdater()).thenReturn(stateUpdater);
      when(ccc.getCoreContainer()).thenReturn(coreContainer);
      when(ccc.getAdminPath()).thenReturn("/admin/collections");
      when(ccc.getCloseableToLatchOn()).thenReturn(mock(SolrCloseable.class));
      // DeleteReplicaCmd unloads the core on the command context's executor; run it inline so
      // no thread outlives the test.
      when(ccc.getExecutorService()).thenReturn(new SameThreadExecutorService());
      // Record every cluster state update the command offers, and advance the simulated
      // cluster state (see the ZkStateReader stub above) as each update lands.
      doAnswer(
              invocation -> {
                Object update = invocation.getArgument(0);
                Map<String, Object> recorded;
                if (update instanceof ZkNodeProps zkNodeProps) {
                  recorded = new HashMap<>(zkNodeProps.getProperties());
                } else {
                  @SuppressWarnings("unchecked")
                  Map<String, Object> asMap =
                      (Map<String, Object>) Utils.fromJSON(Utils.toJSON(update));
                  recorded = new HashMap<>(asMap);
                }
                offeredUpdates.add(recorded);
                DocCollection next = transitions.get(String.valueOf(recorded.get("operation")));
                if (next != null) {
                  currentDoc.set(next);
                }
                return null;
              })
          .when(ccc)
          .offerStateUpdate(any(MapWriter.class));
    }

    void useShardHandler(ShardHandler shardHandler) {
      when(ccc.newShardHandler()).thenReturn(shardHandler);
    }

    List<String> offeredOperations() {
      List<String> operations = new ArrayList<>();
      for (Map<String, Object> update : offeredUpdates) {
        operations.add(String.valueOf(update.get("operation")));
      }
      return operations;
    }
  }

  /**
   * An {@link AbstractExecutorService} that runs every task on the submitting thread, so tests
   * using it leave no pool thread behind.
   */
  private static final class SameThreadExecutorService extends AbstractExecutorService {
    private boolean shutdown;

    @Override
    public void shutdown() {
      shutdown = true;
    }

    @Override
    public List<Runnable> shutdownNow() {
      shutdown = true;
      return List.of();
    }

    @Override
    public boolean isShutdown() {
      return shutdown;
    }

    @Override
    public boolean isTerminated() {
      return shutdown;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
      return true;
    }

    @Override
    public void execute(Runnable command) {
      if (shutdown) {
        throw new RejectedExecutionException("executor is shut down");
      }
      command.run();
    }
  }
}
