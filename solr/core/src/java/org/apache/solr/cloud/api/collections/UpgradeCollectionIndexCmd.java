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

import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.solr.cloud.DistributedClusterStateUpdater;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ShardRequestTracker;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.solr.util.TimeOut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upgrades a collection's index by rewriting old-format Lucene segments into the current format.
 *
 * <p>The operation sets the collection to readOnly, then for each shard:
 *
 * <ol>
 *   <li>Upgrades the shard leader's index locally (no distributed forwarding).
 *   <li>Upgrades each NRT non-leader replica's index locally in parallel.
 *   <li>TLOG and PULL replicas converge by replicating committed leader states via their normal
 *       background replication mechanism. Intermediate commits during upgrade are allowed, so these
 *       replicas may fetch more than once before reaching the terminal state.
 *   <li>Polls every live replica with {@code checkOnly=true} until all report no old-format
 *       segments remaining.
 * </ol>
 *
 * <p>After all shards are processed and validated, readOnly is cleared. If validation fails, the
 * collection remains read-only so writes do not resume on a partially upgraded collection.
 *
 * @see org.apache.solr.handler.admin.api.UpgradeCoreIndex
 */
public class UpgradeCollectionIndexCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Parameter name for the configurable convergence timeout (in seconds). If not specified,
   * defaults to {@link #DEFAULT_REPLICA_CONVERGENCE_TIMEOUT_SECS}.
   */
  public static final String REPLICA_CONVERGENCE_TIMEOUT_SECS_PARAM =
      "replicaConvergenceTimeoutSecs";

  /** Default timeout for waiting for TLOG/PULL replicas to converge after the upgrade. */
  static final int DEFAULT_REPLICA_CONVERGENCE_TIMEOUT_SECS = 1800; // 30 minutes

  private static final long REPLICA_CONVERGENCE_POLL_MS = TimeUnit.SECONDS.toMillis(30);

  private final CollectionCommandContext ccc;

  public UpgradeCollectionIndexCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(AdminCmdContext adminCmdContext, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    String collectionName = message.getStr(NAME);
    if (collectionName == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection name is required for UPGRADECOLLECTIONINDEX");
    }

    ClusterState clusterState = adminCmdContext.getClusterState();
    DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    if (collection == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Collection '" + collectionName + "' not found");
    }

    log.info("Starting UPGRADECOLLECTIONINDEX for collection [{}]", collectionName);

    final int convergenceTimeoutSecs =
        message.getInt(
            REPLICA_CONVERGENCE_TIMEOUT_SECS_PARAM, DEFAULT_REPLICA_CONVERGENCE_TIMEOUT_SECS);

    boolean readOnlyWasSet = false;
    boolean upgradeCompleted = false;
    try {
      // 1. Set collection readOnly to block external writes
      setCollectionReadOnly(collectionName, true);
      readOnlyWasSet = true;

      // 2. Process each shard
      Map<String, Object> shardResults = new LinkedHashMap<>();
      for (Slice slice : collection.getSlices()) {
        NamedList<Object> shardResult = new NamedList<>();
        upgradeShardIndex(
            adminCmdContext, collectionName, slice, shardResult, convergenceTimeoutSecs);
        shardResults.put(slice.getName(), shardResult);
      }
      results.add("shardResults", shardResults);
      upgradeCompleted = true;

      log.info("UPGRADECOLLECTIONINDEX completed successfully for collection [{}]", collectionName);
    } finally {
      // 3. Clear readOnly only after all shards are verified upgraded. On failure we keep the
      // collection read-only to avoid resuming writes on a partially upgraded collection.
      if (readOnlyWasSet && upgradeCompleted) {
        try {
          setCollectionReadOnly(collectionName, false);
        } catch (Exception e) {
          log.error(
              "Failed to clear readOnly on collection [{}] after UPGRADECOLLECTIONINDEX. "
                  + "Collection remains read-only and must be manually restored via "
                  + "MODIFYCOLLECTION action.",
              collectionName,
              e);
          results.add(
              "warning",
              "Failed to clear readOnly on collection. Use MODIFYCOLLECTION to restore writes.");
        }
      } else if (readOnlyWasSet) {
        log.warn(
            "UPGRADECOLLECTIONINDEX did not complete successfully for collection [{}]; keeping"
                + " collection readOnly for operator intervention.",
            collectionName);
        results.add(
            "warning",
            "Upgrade did not complete successfully. Collection remains read-only until it is"
                + " manually restored via MODIFYCOLLECTION.");
      }
    }
  }

  private void upgradeShardIndex(
      AdminCmdContext adminCmdContext,
      String collectionName,
      Slice slice,
      NamedList<Object> shardResult,
      int convergenceTimeoutSecs)
      throws Exception {

    String shardName = slice.getName();
    log.info("Upgrading shard [{}] of collection [{}]", shardName, collectionName);

    // Refresh cluster state to get the current leader
    ClusterState clusterState = ccc.getZkStateReader().getClusterState();

    Replica leader = ccc.getZkStateReader().getLeaderRetry(collectionName, shardName, 30000);

    ShardHandler shardHandler = ccc.newShardHandler();
    ShardRequestTracker tracker = CollectionHandlingUtils.asyncRequestTracker(adminCmdContext, ccc);

    // Step 1: Upgrade the leader (can be NRT or TLOG — both are leader-eligible and have
    // an active IndexWriter when serving as leader)
    log.info(
        "Upgrading leader [{}] (type={}) for shard [{}]",
        leader.getCoreName(),
        leader.getType(),
        shardName);
    ModifiableSolrParams leaderParams = buildUpgradeParams(leader.getCoreName());
    tracker.sendShardRequest(leader, leaderParams, shardHandler);
    tracker.processResponses(
        shardResult, shardHandler, true, "Leader upgrade failed for shard " + shardName);

    // Step 2: Upgrade NRT non-leader replicas in parallel.
    // NRT replicas own their index via IndexWriter, so they can upgrade locally.
    // If the leader is TLOG, all NRT replicas in the shard are non-leaders and are upgraded here.
    List<Replica> nrtNonLeaders = new ArrayList<>();
    for (Replica replica : slice.getReplicas(EnumSet.of(Replica.Type.NRT))) {
      if (!replica.getName().equals(leader.getName())
          && clusterState.liveNodesContain(replica.getNodeName())) {
        nrtNonLeaders.add(replica);
      }
    }

    if (!nrtNonLeaders.isEmpty()) {
      log.info(
          "Upgrading {} NRT non-leader replica(s) for shard [{}]", nrtNonLeaders.size(), shardName);

      ShardHandler nrtHandler = ccc.newShardHandler();
      ShardRequestTracker nrtTracker =
          CollectionHandlingUtils.asyncRequestTracker(adminCmdContext, ccc);
      for (Replica nrtReplica : nrtNonLeaders) {
        ModifiableSolrParams nrtParams = buildUpgradeParams(nrtReplica.getCoreName());
        nrtTracker.sendShardRequest(nrtReplica, nrtParams, nrtHandler);
      }
      nrtTracker.processResponses(
          shardResult, nrtHandler, true, "NRT replica upgrade failed for shard " + shardName);
    }

    // Step 3: TLOG and PULL non-leader replicas converge via their normal background
    // replication from committed leader states. Because auto-commit may persist intermediate
    // progress during the leader upgrade, these replicas can begin converging before the leader's
    // final explicit commit, so completion must be gated on replica-side validation rather than on
    // assuming a single terminal fetch. We do not upgrade them locally to avoid racing with their
    // background replication thread.
    List<String> replicatingReplicas = new ArrayList<>();
    for (Replica replica : slice.getReplicas(EnumSet.of(Replica.Type.TLOG, Replica.Type.PULL))) {
      if (!replica.getName().equals(leader.getName())) {
        replicatingReplicas.add(replica.getCoreName() + " (" + replica.getType() + ")");
      }
    }
    if (!replicatingReplicas.isEmpty()) {
      log.info(
          "TLOG/PULL replicas for shard [{}] will converge via replication from leader: {}",
          shardName,
          replicatingReplicas);
      shardResult.add("replicatingFromLeader", replicatingReplicas);
    }

    waitForShardReplicaConvergence(
        adminCmdContext, collectionName, slice, shardResult, convergenceTimeoutSecs);

    log.info("Shard [{}] upgrade complete", shardName);
  }

  private ModifiableSolrParams buildUpgradeParams(String coreName) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.UPGRADECOREINDEX.toString());
    params.set(CoreAdminParams.CORE, coreName);
    params.set("cloudMode", "true");
    return params;
  }

  private ModifiableSolrParams buildCheckOnlyParams(String coreName) {
    ModifiableSolrParams params = buildUpgradeParams(coreName);
    params.set("checkOnly", "true");
    return params;
  }

  private void waitForShardReplicaConvergence(
      AdminCmdContext adminCmdContext,
      String collectionName,
      Slice slice,
      NamedList<Object> shardResult,
      int convergenceTimeoutSecs)
      throws Exception {
    final String shardName = slice.getName();
    final TimeOut timeout =
        new TimeOut(
            convergenceTimeoutSecs, TimeUnit.SECONDS, ccc.getSolrCloudManager().getTimeSource());

    // Monotonically accumulates replica keys that have confirmed indexUpgraded=true.
    // A replica that confirms in iteration N is not re-checked in iteration N+1, so a
    // momentary probe failure (GC pause, brief network blip) on a subsequent poll cannot
    // un-confirm an already-verified replica.
    final Set<String> confirmedConverged = new HashSet<>();

    while (true) {
      ClusterState latestClusterState = ccc.getZkStateReader().getClusterState();
      Slice latestSlice = latestClusterState.getCollection(collectionName).getSlice(shardName);
      List<Replica> allReplicas = new ArrayList<>(latestSlice.getReplicas());

      // Probe only the live replicas that have not yet been confirmed. Non-live replicas
      // remain in pendingReplicas until they recover and report indexUpgraded=true.
      List<Replica> toCheck = new ArrayList<>();
      for (Replica replica : allReplicas) {
        String key = CollectionHandlingUtils.requestKey(replica);
        if (!confirmedConverged.contains(key)
            && latestClusterState.liveNodesContain(replica.getNodeName())) {
          toCheck.add(replica);
        }
      }

      if (!toCheck.isEmpty()) {
        NamedList<Object> checkResults = new NamedList<>();
        ShardHandler checkHandler = ccc.newShardHandler();
        ShardRequestTracker checkTracker =
            CollectionHandlingUtils.syncRequestTracker(adminCmdContext, ccc);
        for (Replica replica : toCheck) {
          checkTracker.sendShardRequest(
              replica, buildCheckOnlyParams(replica.getCoreName()), checkHandler);
        }
        // abortOnError=false: transient request failures are treated as "not yet converged"
        // rather than fatal errors that would permanently leave the collection read-only.
        checkTracker.processResponses(
            checkResults,
            checkHandler,
            false,
            "Replica validation failed for shard " + shardName + " during UPGRADECOLLECTIONINDEX");

        updateConfirmedConverged(checkResults, confirmedConverged);
      }

      // Pending = ALL replicas of the shard (live or not) not yet confirmed. A down replica
      // is still unverified and must eventually confirm before readOnly is cleared.
      List<String> pendingReplicas =
          allReplicas.stream()
              .map(CollectionHandlingUtils::requestKey)
              .filter(key -> !confirmedConverged.contains(key))
              .toList();

      if (pendingReplicas.isEmpty()) {
        shardResult.add(
            "validatedReplicas", allReplicas.stream().map(Replica::getCoreName).toList());
        return;
      }

      if (timeout.hasTimedOut()) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Timed out waiting for shard "
                + shardName
                + " replicas to converge to the upgraded index format after "
                + convergenceTimeoutSecs
                + "s: "
                + pendingReplicas);
      }

      log.info(
          "Shard [{}] waiting for replica convergence ({} pending, {}s remaining): {}",
          shardName,
          pendingReplicas.size(),
          timeout.timeLeft(TimeUnit.SECONDS),
          pendingReplicas);
      timeout.sleep(REPLICA_CONVERGENCE_POLL_MS);
    }
  }

  /**
   * Adds to {@code confirmedConverged} the keys of replicas that responded with {@code
   * indexUpgraded=true} in this poll round.
   */
  private void updateConfirmedConverged(
      NamedList<Object> checkResults, Set<String> confirmedConverged) {
    @SuppressWarnings("unchecked")
    SimpleOrderedMap<Object> successes = (SimpleOrderedMap<Object>) checkResults.get("success");
    if (successes == null) {
      return;
    }
    for (int i = 0; i < successes.size(); i++) {
      Object responseObj = successes.getVal(i);
      if ((responseObj instanceof NamedList<?> response)
          && Boolean.TRUE.equals(response.get("indexUpgraded"))) {
        confirmedConverged.add(successes.getName(i));
      }
    }
  }

  /**
   * Sets or clears the readOnly property on a collection via MODIFYCOLLECTION. Setting readOnly to
   * true blocks all external writes at the {@link
   * org.apache.solr.update.processor.DistributedZkUpdateProcessor} layer.
   */
  private void setCollectionReadOnly(String collectionName, boolean readOnly) throws Exception {
    String readOnlyValue = readOnly ? "true" : null; // null clears the property

    log.info("Setting readOnly={} on collection [{}]", readOnly, collectionName);

    ZkNodeProps props =
        new ZkNodeProps(
            Overseer.QUEUE_OPERATION,
            CollectionParams.CollectionAction.MODIFYCOLLECTION.toLower(),
            ZkStateReader.COLLECTION_PROP,
            collectionName,
            ZkStateReader.READ_ONLY,
            readOnlyValue);

    if (ccc.getDistributedClusterStateUpdater().isDistributedStateUpdate()) {
      ccc.getDistributedClusterStateUpdater()
          .doSingleStateUpdate(
              DistributedClusterStateUpdater.MutatingCommand.CollectionModifyCollection,
              props,
              ccc.getSolrCloudManager(),
              ccc.getZkStateReader());
    } else {
      ccc.offerStateUpdate(props);
    }
  }
}
