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
                  NamedList<Object> addResultSuccess = (NamedList<Object>) addResult.get("success");
                  success.addAll(addResultSuccess);
                }
              });
    } catch (Exception e) {
      if (!sliceAlreadyExists) {
        // Don't leave a half-created shard stuck in CONSTRUCTION; remove it so the create can be
        // retried cleanly.
        try {
          new DeleteShardCmd(ccc)
              .call(
                  adminCmdContext
                      .subRequestContext(CollectionParams.CollectionAction.DELETESHARD, async)
                      .withClusterState(clusterState),
                  new ZkNodeProps(COLLECTION_PROP, collectionName, SHARD_ID_PROP, sliceName),
                  results);
        } catch (Exception cleanupEx) {
          log.warn("Failed to delete shard {} after failed create", sliceName, cleanupEx);
        }
      }
      throw e;
    }

    if (!sliceAlreadyExists) {
      // The new slice is in CONSTRUCTION state, so queries skip it until it is activated. It is
      // activated even if waiting for its replicas fails, so that it is not left unusable.
      try {
        waitForShardReplicasActive(collectionName, sliceName, numReplicas.total(), timeout);
        applyBufferedUpdatesOnLeader(adminCmdContext, collectionName, sliceName);
      } finally {
        activateShard(collectionName, sliceName);
      }
    }

    log.info("Finished create command on all shards for collection: {}", collectionName);
  }

  /**
   * Cores of a CONSTRUCTION shard buffer their updates, which a split lifts for its sub-shards but
   * nothing else would for a created shard, so ask the leader to apply them.
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

    // The core refuses the request when it is not buffering, which is fine, so only log a failure.
    NamedList<Object> applyResults = new NamedList<>();
    tracker.processResponses(applyResults, shardHandler, false, null);
    if (applyResults.get("failure") != null) {
      log.warn(
          "Leader {} of new shard {} did not apply buffered updates: {}",
          leader.getName(),
          sliceName,
          applyResults.get("failure"));
    }
  }

  /** Flip a newly created shard from CONSTRUCTION to ACTIVE so it becomes visible to queries. */
  private void activateShard(String collectionName, String sliceName)
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
  }

  /** Wait until a shard has the expected number of replicas and all of them are ACTIVE. */
  private void waitForShardReplicasActive(
      String collectionName, String sliceName, int expectedReplicas, int timeout)
      throws InterruptedException {
    ZkStateReader zkStateReader = ccc.getZkStateReader();
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
      if (!latch.await(timeout, TimeUnit.SECONDS)) {
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
