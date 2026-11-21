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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionParams.CollectionAction.CREATESHARD;
import static org.apache.solr.common.params.CommonAdminParams.TIMEOUT;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaCount;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateShardCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // The message a core answers REQUESTAPPLYUPDATES with when it is not buffering updates; see
  // org.apache.solr.handler.admin.RequestApplyUpdatesOp.
  private static final String NOT_BUFFERING_MESSAGE = "not in buffering state";

  private final CollectionCommandContext ccc;

  public CreateShardCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(AdminCmdContext adminCmdContext, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    String extCollectionName = message.getStr(COLLECTION_PROP);
    String sliceName = message.getStr(SHARD_ID_PROP);
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);

    log.info("Create shard invoked: {}", message);
    if (extCollectionName == null || sliceName == null)
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "'collection' and 'shard' are required parameters");

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName =
          ccc.getSolrCloudManager().getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }
    ClusterState clusterState = adminCmdContext.getClusterState();
    DocCollection collection = clusterState.getCollection(collectionName);
    boolean sliceAlreadyExists = collection.getSlice(sliceName) != null;

    ReplicaCount numReplicas = ReplicaCount.fromMessage(message, collection, 1);
    if (!numReplicas.hasLeaderReplica()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Unexpected number of replicas ("
              + numReplicas
              + "), there must be at least one leader-eligible replica");
    }

    ZkNodeProps m = cloneZkPropsWithOperation(message, CREATESHARD);

    if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      // The message has been crafted by CollectionsHandler.CollectionOperation.CREATESHARD_OP and
      // defines the QUEUE_OPERATION to be CollectionParams.CollectionAction.CREATESHARD. Likely a
      // bug here (distributed or Overseer based) as we use the collection alias name and not the
      // real name?
      ccc.getDistributedClusterStateUpdater()
          .doSingleStateUpdate(
              DistributedClusterStateUpdater.MutatingCommand.CollectionCreateShard,
              m,
              ccc.getSolrCloudManager(),
              ccc.getZkStateReader());
    } else {
      // message contains extCollectionName that might be an alias. Unclear (to me) how this works
      // in that case.
      ccc.offerStateUpdate(m);
    }

    // wait for a while until we see the shard and update the local view of the cluster state
    clusterState =
        CollectionHandlingUtils.waitForNewShard(collectionName, sliceName, ccc.getZkStateReader());

    String async = adminCmdContext.getAsyncId();
    Map<String, Object> addReplicasProps =
        Utils.makeMap(
            COLLECTION_PROP,
            (Object) collectionName,
            SHARD_ID_PROP,
            sliceName,
            CollectionHandlingUtils.CREATE_NODE_SET,
            message.getStr(CollectionHandlingUtils.CREATE_NODE_SET),
            CommonAdminParams.WAIT_FOR_FINAL_STATE,
            Boolean.toString(waitForFinalState));
    numReplicas.writeProps(addReplicasProps);

    CollectionHandlingUtils.addPropertyParams(message, addReplicasProps);
    final NamedList<Object> addResult = new NamedList<>();
    int timeout = message.getInt(TIMEOUT, 10 * 60); // 10 minutes
    // AddReplicaCmd applies its own timeout when it waits for the new replicas (only when
    // waitForFinalState is set); give it the caller's timeout rather than its own default.
    if (message.getInt(TIMEOUT, null) != null) {
      addReplicasProps.put(TIMEOUT, timeout);
    }
    // A caller can ask for the shard without its replicas (createNodeSet=EMPTY, the v2 API's
    // createReplicas=false). AddReplicaCmd has no empty node set to assign from and would
    // reject "EMPTY" as an unknown node name, so there is no add step at all in that case.
    boolean createReplicas =
        !CollectionHandlingUtils.CREATE_NODE_SET_EMPTY.equals(
            message.getStr(CollectionHandlingUtils.CREATE_NODE_SET));
    if (createReplicas) {
      try {
        new AddReplicaCmd(ccc)
            .addReplica(
                adminCmdContext
                    .subRequestContext(CollectionParams.CollectionAction.ADDREPLICA, async)
                    .withClusterState(clusterState),
                new ZkNodeProps(addReplicasProps),
                addResult,
                () -> {
                  @SuppressWarnings("unchecked")
                  NamedList<Object> addResultFailure = (NamedList<Object>) addResult.get("failure");
                  if (addResultFailure != null) {
                    @SuppressWarnings("unchecked")
                    SimpleOrderedMap<Object> failure =
                        (SimpleOrderedMap<Object>) results.get("failure");
                    if (failure == null) {
                      failure = new SimpleOrderedMap<>();
                      results.add("failure", failure);
                    }
                    failure.addAll(addResultFailure);
                  } else {
                    @SuppressWarnings("unchecked")
                    SimpleOrderedMap<Object> success =
                        (SimpleOrderedMap<Object>) results.get("success");
                    if (success == null) {
                      success = new SimpleOrderedMap<>();
                      results.add("success", success);
                    }
                    @SuppressWarnings("unchecked")
                    NamedList<Object> addResultSuccess =
                        (NamedList<Object>) addResult.get("success");
                    success.addAll(addResultSuccess);
                  }
                });
      } catch (Exception e) {
        if (!sliceAlreadyExists) {
          // Don't leave a half-created shard stuck in CONSTRUCTION; remove it so the create can be
          // retried cleanly. DeleteShardCmd needs a fresh cluster state here: the snapshot from
          // waitForNewShard predates AddReplicaCmd, so it shows a slice with no replicas and the
          // delete would leave the added replicas' cores behind.
          try {
            new DeleteShardCmd(ccc)
                .call(
                    adminCmdContext
                        .subRequestContext(CollectionParams.CollectionAction.DELETESHARD, async)
                        .withClusterState(ccc.getZkStateReader().getClusterState()),
                    new ZkNodeProps(COLLECTION_PROP, collectionName, SHARD_ID_PROP, sliceName),
                    results);
          } catch (Exception cleanupEx) {
            log.warn("Failed to delete shard {} after failed create", sliceName, cleanupEx);
          }
        }
        throw e;
      }
    }

    if (!sliceAlreadyExists) {
      // The new slice is in CONSTRUCTION state, so queries skip it until it is activated. When
      // the caller creates the shard without its replicas (createNodeSet=EMPTY), there is
      // nothing to wait for and no leader to apply buffered updates on, so the slice can be
      // activated right away. Otherwise any buffered updates are applied before activation: a
      // client can already write to the shard through the implicit router, and those writes
      // only become visible once the leader applies them. When the caller asked to wait for
      // the final state, the replicas must also be active first; without that flag the create
      // does not block on them. A failure here propagates without deleting the shard: its
      // replicas can already hold acknowledged writes, so the shard is left in CONSTRUCTION
      // state for the operator instead of dropping them.
      if (createReplicas) {
        if (waitForFinalState) {
          waitForShardReplicasActive(collectionName, sliceName, numReplicas.total(), timeout);
        }
        applyBufferedUpdatesOnLeader(adminCmdContext, collectionName, sliceName);
      }
      activateShard(collectionName, sliceName, timeout);
    }

    log.info("Finished create command on all shards for collection: {}", collectionName);
  }

  /**
   * Cores of a CONSTRUCTION shard buffer their updates, which a split lifts for its sub-shards but
   * nothing else would for a created shard, so ask the leader to apply them. A failure to apply
   * them is fatal to the create, except for the leader refusing the request because its core is not
   * buffering updates: activating the shard over updates that failed to replay would hide them.
   */
  private void applyBufferedUpdatesOnLeader(
      AdminCmdContext adminCmdContext, String collectionName, String sliceName) {
    Slice slice =
        ccc.getZkStateReader().getClusterState().getCollection(collectionName).getSlice(sliceName);
    Replica leader = slice == null ? null : slice.getLeader();
    if (leader == null) {
      log.warn("No leader for new shard {} of collection {}", sliceName, collectionName);
      return;
    }
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(
        CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.REQUESTAPPLYUPDATES.toString());
    params.set(CoreAdminParams.NAME, leader.getCoreName());

    ShardHandler shardHandler = ccc.newShardHandler();
    CollectionHandlingUtils.ShardRequestTracker tracker =
        CollectionHandlingUtils.asyncRequestTracker(adminCmdContext, ccc);
    tracker.sendShardRequest(leader, params, shardHandler);

    NamedList<Object> applyResults = new NamedList<>();
    tracker.processResponses(applyResults, shardHandler, false, null);
    Object failure = applyResults.get("failure");
    if (failure == null) {
      return;
    }
    if (isNotBufferingRefusal(failure)) {
      // The core refuses the request when it is not buffering updates, which is the normal case
      // for a shard nobody wrote to while it was under construction.
      if (log.isInfoEnabled()) {
        log.info(
            "Leader {} of new shard {} was not buffering updates; nothing to apply",
            leader.getName(),
            sliceName);
      }
      return;
    }
    throw new SolrException(
        SolrException.ErrorCode.SERVER_ERROR,
        "Leader "
            + leader.getName()
            + " of new shard "
            + sliceName
            + " failed to apply buffered updates: "
            + failure);
  }

  /**
   * Whether every failure in the "failure" entry of a shard request's results is the leader
   * refusing REQUESTAPPLYUPDATES because its core is not buffering updates (the message the core
   * answers with in that case). Any other failure means buffered updates were not replayed.
   */
  private static boolean isNotBufferingRefusal(Object failure) {
    if (!(failure instanceof NamedList<?> failures) || failures.size() == 0) {
      return false;
    }
    for (int i = 0; i < failures.size(); i++) {
      Object value = failures.getVal(i);
      String text =
          value instanceof Throwable throwable ? throwable.getMessage() : String.valueOf(value);
      if (text == null || !text.contains(NOT_BUFFERING_MESSAGE)) {
        return false;
      }
    }
    return true;
  }

  /** Flip a newly created shard from CONSTRUCTION to ACTIVE so it becomes visible to queries. */
  private void activateShard(String collectionName, String sliceName, int timeout)
      throws KeeperException, InterruptedException {
    Map<String, Object> activateProps = new HashMap<>();
    activateProps.put(Overseer.QUEUE_OPERATION, OverseerAction.UPDATESHARDSTATE.toLower());
    activateProps.put(COLLECTION_PROP, collectionName);
    activateProps.put(sliceName, Slice.State.ACTIVE.toString());
    ZkNodeProps activateMsg = new ZkNodeProps(activateProps);
    if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      ccc.getDistributedClusterStateUpdater()
          .doSingleStateUpdate(
              DistributedClusterStateUpdater.MutatingCommand.SliceUpdateShardState,
              activateMsg,
              ccc.getSolrCloudManager(),
              ccc.getZkStateReader());
    } else {
      ccc.offerStateUpdate(activateMsg);
    }
    // Do not return before the flip is visible in the local cluster state: once the create
    // returns, callers act on the shard being queryable.
    try {
      ccc.getZkStateReader()
          .waitForState(
              collectionName,
              timeout,
              TimeUnit.SECONDS,
              collection -> {
                Slice slice = collection == null ? null : collection.getSlice(sliceName);
                return slice != null && slice.getState() == Slice.State.ACTIVE;
              });
    } catch (TimeoutException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Timeout waiting "
              + timeout
              + " seconds for shard "
              + sliceName
              + " of collection "
              + collectionName
              + " to become active",
          e);
    }
  }

  /** Wait until a shard has the expected number of replicas and all of them are ACTIVE. */
  private void waitForShardReplicasActive(
      String collectionName, String sliceName, int expectedReplicas, int timeout)
      throws InterruptedException {
    ZkStateReader zkStateReader = ccc.getZkStateReader();
    // The two phases below share the one timeout: the wait as a whole gets the caller's
    // budget, not that budget per phase.
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeout);
    try {
      zkStateReader.waitForState(
          collectionName,
          timeout,
          TimeUnit.SECONDS,
          collection -> {
            Slice newSlice = collection == null ? null : collection.getSlice(sliceName);
            return newSlice != null && newSlice.getReplicas().size() >= expectedReplicas;
          });
    } catch (TimeoutException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Timeout waiting " + timeout + " seconds for the replicas of shard " + sliceName,
          e);
    }
    Slice slice = zkStateReader.getClusterState().getCollection(collectionName).getSlice(sliceName);
    if (slice == null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Newly created shard " + sliceName + " not visible in cluster state");
    }
    List<String> coreNames =
        slice.getReplicas().stream().map(Replica::getCoreName).collect(Collectors.toList());
    if (coreNames.isEmpty()) {
      log.warn(
          "No replicas found for new shard {} of collection {}; activating without waiting",
          sliceName,
          collectionName);
      return;
    }
    SolrCloseableLatch latch =
        new SolrCloseableLatch(coreNames.size(), ccc.getCloseableToLatchOn());
    ActiveReplicaWatcher watcher = new ActiveReplicaWatcher(collectionName, null, coreNames, latch);
    try {
      zkStateReader.registerCollectionStateWatcher(collectionName, watcher);
      long remainingNanos = deadlineNanos - System.nanoTime();
      if (remainingNanos <= 0 || !latch.await(remainingNanos, TimeUnit.NANOSECONDS)) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Timeout waiting "
                + timeout
                + " seconds for replicas of shard "
                + sliceName
                + " to become active.");
      }
    } finally {
      zkStateReader.removeCollectionStateWatcher(collectionName, watcher);
    }
  }
}
