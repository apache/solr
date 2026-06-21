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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.Cmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler.ShardRequestTracker;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.component.ShardHandler;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;

public class DeleteReplicaCmd implements Cmd {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final OverseerCollectionMessageHandler ocmh;
  private final boolean onlyUpdateState;

  private boolean createdShardHandler;

  public DeleteReplicaCmd(OverseerCollectionMessageHandler ocmh) {
    this(ocmh, false);
  }

  public DeleteReplicaCmd(OverseerCollectionMessageHandler ocmh , boolean onlyUpdateState) {
    this.onlyUpdateState = onlyUpdateState;
    this.ocmh = ocmh;
  }

  @Override

  public AddReplicaCmd.Response call(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"})NamedList results) throws Exception {
    ShardHandler shardHandler = null;
    ShardRequestTracker shardRequestTracker = null;
    if (!onlyUpdateState) {
      String asyncId = message.getStr(ASYNC);
      shardHandler = ocmh.shardHandlerFactory.getShardHandler(ocmh.overseerLbClient);
      shardRequestTracker = ocmh.asyncRequestTracker(asyncId, message.getStr(Overseer.QUEUE_OPERATION));
      createdShardHandler = true;
    }
    String shardId = message.getStr(SHARD_ID_PROP);
    String extCollectionName = message.getStr(COLLECTION_PROP);

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);
    String collectionName;
    if (followAliases) {
      collectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }
    return deleteReplica(clusterState, message, shardHandler, shardRequestTracker, results, collectionName, shardId);
  }


  @SuppressWarnings("unchecked")
  AddReplicaCmd.Response deleteReplica(ClusterState clusterState, ZkNodeProps message, ShardHandler shardHandler,
      ShardRequestTracker shardRequestTracker, @SuppressWarnings({"rawtypes"})NamedList results, String collectionName, String shard)
          throws KeeperException, InterruptedException {

    log.info("deleteReplica() : {}", Utils.toJSONString(message));
    boolean waitForFinalState = message.getBool(WAIT_FOR_FINAL_STATE, false);
    //If a count is specified the strategy needs be different
    if (message.getStr(COUNT_PROP) != null) {

      AddReplicaCmd.Response resp = deleteReplicaBasedOnCount(clusterState, message, results, shardHandler, shardRequestTracker);
      clusterState = resp.clusterState;
      AddReplicaCmd.Response response = new AddReplicaCmd.Response();

      if (results.get("failure") == null && results.get("exception") == null) {
        String finalCollectionName = collectionName;
        String finalCollectionName2 = collectionName;
        response.asyncFinalRunner = new OverseerCollectionMessageHandler.Finalize() {
          @Override
          public AddReplicaCmd.Response call() {
            if (resp.asyncFinalRunner != null) {
              try {
                resp.asyncFinalRunner.call();
              } catch (Exception e) {
                log.error("Exception running delete replica finalizers", e);
              }
            }

            if (shardRequestTracker != null) {
              try {
                shardRequestTracker.processResponses(results, shardHandler, false, null);
              } catch (Exception e) {
                log.error("Exception waiting for delete replica response");
              }
            }
            ocmh.overseer.getZkStateWriter().writeStructureUpdates(finalCollectionName2);
            Set<String> replicas = (Set<String>) resp.results.get("replicas_deleted");
            for (String replica : replicas) {
              try {
                waitForCoreNodeGone(finalCollectionName, shard, replica, 30000);
              } catch (Exception e) {
                log.error("", e);
              }
            }
            AddReplicaCmd.Response response = new AddReplicaCmd.Response();
            return response;
          }
        };
      }
      response.clusterState = clusterState;
      return response;
    }

    OverseerCollectionMessageHandler.checkRequired(message, COLLECTION_PROP, SHARD_ID_PROP, REPLICA_PROP);
    String extCollectionName = message.getStr(COLLECTION_PROP);

    String replicaName = message.getStr(REPLICA_PROP);

    boolean followAliases = message.getBool(FOLLOW_ALIASES, false);

    if (followAliases) {
      collectionName = ocmh.cloudManager.getClusterStateProvider().resolveSimpleAlias(extCollectionName);
    } else {
      collectionName = extCollectionName;
    }

    DocCollection coll = clusterState.getCollectionOrNull(collectionName);
    if (coll == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid collection name for replica : " + replicaName + " in collection : " + collectionName);
    }

    Slice slice = coll.getSlice(shard);
    if (slice == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid shard name : " + shard + " in collection : " + collectionName);
    }

    // The dispatcher hands us the overseer's in-memory working cluster state. That copy can momentarily
    // lag the published state - e.g. right after a new overseer reloaded its in-memory view, or while a
    // freshly-registered replica's structure change is still in flight - so a replica that genuinely
    // exists may not be visible here yet. Before rejecting the request as 'Invalid replica', fall back to
    // the authoritative published state from the ZkStateReader; if it has the replica, use that view so
    // the delete proceeds (and the resulting structure change re-converges the working copy).
    if (slice.getReplica(replicaName) == null) {
      ZkStateReader reader = ocmh.overseer.getZkStateReader();
      DocCollection live = reader == null ? null : reader.getCollectionOrNull(collectionName);
      Slice liveSlice = live == null ? null : live.getSlice(shard);
      if (liveSlice != null && liveSlice.getReplica(replicaName) != null) {
        log.warn("Replica {} not present in overseer working cluster state for {}/{}; using published state",
            replicaName, collectionName, shard);
        coll = live;
        slice = liveSlice;
        clusterState = clusterState.copyWith(collectionName, live);
      }
    }

    AddReplicaCmd.Response resp = deleteCore(clusterState, slice, collectionName, replicaName, message, shard, results, shardRequestTracker, shardHandler);
    clusterState = resp.clusterState;

//    if (clusterState.getCollectionOrNull(collectionName).getReplica(replicaName) != null) {
//      throw new IllegalStateException("Failed to remove replica from state " + replicaName);
//    }

    AddReplicaCmd.Response response = new AddReplicaCmd.Response();

    if (!onlyUpdateState && createdShardHandler) {
      ClusterState finalClusterState = clusterState;
      String finalCollectionName1 = collectionName;
      response.asyncFinalRunner = new OverseerCollectionMessageHandler.Finalize() {
        @Override
        public AddReplicaCmd.Response call() {

          try {
            shardRequestTracker.processResponses(results, shardHandler, false, null);
          } catch (Exception e) {
            log.error("Exception waiting for delete replica response");
          }

          try {
            ocmh.overseer.getZkStateWriter().enqueueStructureChange(finalClusterState.getCollection(finalCollectionName1));
            ocmh.overseer.writePendingUpdates(finalCollectionName1);
          } catch (Exception e) {
            log.error("Failed enqueuing and writing out state", e);
          }

          if (waitForFinalState) {
            try {
              waitForCoreNodeGone(finalCollectionName1, shard, replicaName, 5000); // MRM TODO: timeout
            } catch (Exception e) {
              log.error("Failed waiting for replica to be removed", e);
            }
          }
          return new AddReplicaCmd.Response();
        }
      };
    }
    response.clusterState = clusterState;
    response.results = results;
    return response;
  }


  /**
   * Delete replicas based on count for a given collection. If a shard is passed, uses that
   * else deletes given num replicas across all shards for the given collection.
   * @return
   */
  @SuppressWarnings({"unchecked"})
  AddReplicaCmd.Response deleteReplicaBasedOnCount(ClusterState clusterState, ZkNodeProps message, @SuppressWarnings({"rawtypes"}) NamedList results, ShardHandler shardHandler,
      ShardRequestTracker shardRequestTracker)
          throws KeeperException, InterruptedException {
    OverseerCollectionMessageHandler.checkRequired(message, COLLECTION_PROP, COUNT_PROP);
    int count = Integer.parseInt(message.getStr(COUNT_PROP));
    String collectionName = message.getStr(COLLECTION_PROP);
    String shard = message.getStr(SHARD_ID_PROP);
    DocCollection coll = clusterState.getCollection(collectionName);
    Slice slice = null;
    //Validate if shard is passed.
    if (shard != null) {
      slice = coll.getSlice(shard);
      if (slice == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Invalid shard name : " + shard + " in collection : " + collectionName);
      }
    }
    Set<String> replicasToBeDeleted = null;
    Map<Slice,Set<String>> shardToReplicasMapping = new HashMap<Slice,Set<String>>();
    if (slice != null) {
      replicasToBeDeleted = pickReplicasTobeDeleted(slice, shard, collectionName, count);
      shardToReplicasMapping.put(slice, replicasToBeDeleted);
    } else {

      //If there are many replicas left, remove the rest based on count.
      Collection<Slice> allSlices = coll.getSlices();
      for (Slice individualSlice : allSlices) {
        replicasToBeDeleted = pickReplicasTobeDeleted(individualSlice, individualSlice.getName(), collectionName, count);
        shardToReplicasMapping.put(individualSlice, replicasToBeDeleted);
      }
    }
    List<AddReplicaCmd.Response> finalizers = new ArrayList<>();

    for (Map.Entry<Slice,Set<String>> entry : shardToReplicasMapping.entrySet()) {
      Slice shardSlice = entry.getKey();
      String shardId = shardSlice.getName();
      Set<String> replicas = entry.getValue();
      // callDeleteReplica on all replicas
      for (String replica : replicas) {
        if (log.isDebugEnabled()) log.debug("Deleting replica {}  for shard {} based on count {}", replica, shardId, count);

        AddReplicaCmd.Response resp = deleteCore(clusterState, shardSlice, collectionName, replica, message, shard, results, shardRequestTracker, shardHandler);
        clusterState = resp.clusterState;
        if (clusterState != null) {
          try {
            ocmh.overseer.getZkStateWriter().enqueueStructureChange(clusterState.getCollection(collectionName));
          } catch (Exception e) {
            log.error("failed sending update to zkstatewriter", e);
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
          }
        }

        finalizers.add(resp);
      }

      results.add("shard_id", shardId);
      results.add("replicas_deleted", replicas);
    }

    AddReplicaCmd.Response response = new AddReplicaCmd.Response();
    response.clusterState = clusterState;
    response.asyncFinalRunner = () -> {
      AddReplicaCmd.Response resp = new AddReplicaCmd.Response();
      resp.asyncFinalRunner = () -> {
        for (AddReplicaCmd.Response finalize : finalizers) {
          finalize.asyncFinalRunner.call();
        }
        return new AddReplicaCmd.Response();
      };
      return resp;
    };
    response.results = results;
    return response;
  }


  /**
   * Pick replicas to be deleted. Avoid picking the leader.
   */
  private Set<String> pickReplicasTobeDeleted(Slice s, String shard, String collectionName, int count) {
    Set<String> replicasToBeRemoved = ConcurrentHashMap.newKeySet(count);
    // Capture the freshest converged slice so we can validate against current state. The validation
    // itself must run OUTSIDE the predicate: waitForState invokes the predicate on a watcher thread
    // whose thrown exceptions are swallowed (they would surface only as a spurious TimeoutException).
    final AtomicReference<Slice> freshSlice = new AtomicReference<>();
    final AtomicReference<Set<String>> liveNodesRef = new AtomicReference<>();

    try {
      ocmh.zkStateReader.waitForState(collectionName,5,TimeUnit.SECONDS, (liveNodes, collectionState) -> {
        if (collectionState == null) {
          return false;
        }
        Slice slice = collectionState.getSlice(s.getName());
        if (slice == null) {
          return false;
        }
        freshSlice.set(slice);
        liveNodesRef.set(liveNodes);
        return true;
      });
    } catch (InterruptedException | TimeoutException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    }

    Slice slice = freshSlice.get();
    // Validate against the freshest cluster state: never delete down to (or below) the last replica.
    // This guard was disabled in the fork; without it a delete-by-count that would remove the last
    // replica silently no-ops (the leader is skipped) instead of erroring with a BAD_REQUEST.
    validateReplicaAvailability(slice, shard, collectionName, count);

    Replica leader = slice.getLeader(liveNodesRef.get());
    Collection<Replica> allReplicas = slice.getReplicas();
    int cnt = count;
    for (Replica replica: allReplicas) {
      if (cnt == 0) {
        break;
      }
      //Try avoiding to pick up the leader to minimize activity on the cluster.
      if (leader != null && leader.getName().equals(replica.getName())) {
        continue;
      }
      replicasToBeRemoved.add(replica.getName());
      cnt--;
    }

    if (log.isDebugEnabled()) log.debug("Found replicas to be removed {}", replicasToBeRemoved);
    return replicasToBeRemoved;
  }

  /**
   * Validate if there is less replicas than requested to remove. Also error out if there is
   * only one replica available
   */
  private static void validateReplicaAvailability(Slice slice, String shard, String collectionName, int count) {
    //If there is a specific shard passed, validate if there any or just 1 replica left
    if (slice != null) {
      Collection<Replica> allReplicasForShard = slice.getReplicas();
      if (allReplicasForShard == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No replicas found  in shard/collection: " +
                shard + "/"  + collectionName);
      }


      if (allReplicasForShard.size() == 1) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There is only one replica available in shard/collection: " +
                shard + "/" + collectionName + ". Cannot delete that.");
      }

      if (allReplicasForShard.size() <= count) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "There are lesser num replicas requested to be deleted than are available in shard/collection : " +
                shard + "/"  + collectionName  + " Requested: "  + count + " Available: " + allReplicasForShard.size() + ".");
      }
    }
  }

  AddReplicaCmd.Response deleteCore(ClusterState clusterState, Slice slice, String collectionName, String replicaName,
      ZkNodeProps message, String shard, @SuppressWarnings({"rawtypes"})NamedList results, ShardRequestTracker shardRequestTracker, ShardHandler shardHandler) {
    log.info("delete core {}", replicaName);
    Replica replica = slice.getReplica(replicaName);
    if (replica == null) {
      // Idempotent delete: the replica is not present in the cluster state we were given (and the caller
      // already fell back to the published state if the working copy merely lagged). Treat this as a
      // replica that has already been removed and return success rather than a 400. This is important
      // because a delete can be retried after a partial success: the first attempt removes the replica
      // from cluster state but then the core UNLOAD comes back 404 (the core had already failed to load /
      // gone under load), which makes the command 'fail' and be retried - and the retry would otherwise
      // throw 'Invalid replica' for the replica the first attempt just successfully removed.
      ArrayList<String> l = new ArrayList<>();
      for (Replica r : slice.getReplicas())
        l.add(r.getName());
      log.warn("Replica {} not found in shard/collection {}/{} (already removed?); treating delete as a no-op. Present replicas: {}",
          replicaName, shard, collectionName, l);
      AddReplicaCmd.Response alreadyGone = new AddReplicaCmd.Response();
      alreadyGone.clusterState = clusterState;
      return alreadyGone;
    }

    // If users are being safe and only want to remove a shard if it is down, they can specify onlyIfDown=true
    // on the command.
//    if (Boolean.parseBoolean(message.getStr(OverseerCollectionMessageHandler.ONLY_IF_DOWN)) && replica.getState() != Replica.State.DOWN) {
//      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
//              "Attempted to remove replica : " + collectionName + "/"  + shard + "/" + replicaName +
//              " with onlyIfDown='true', but state is '" + replica.getStr(ZkStateReader.STATE_PROP) + "'");
//    }
    AddReplicaCmd.Response response = new AddReplicaCmd.Response();
    ZkNodeProps rep = new ZkNodeProps();
    rep.getProperties().put("replica", replicaName);
    rep.getProperties().put("collection", replica.getCollection());
    rep.getProperties().put("node", replica.getNodeName());

    if (log.isDebugEnabled()) log.debug("Before slice remove replica {} {}", rep, clusterState);
    clusterState = SliceMutator.removeReplica(clusterState, rep);
    if (log.isDebugEnabled()) log.debug("After slice remove replica {} {}", rep, clusterState);
    boolean isLive = false;

    if (!onlyUpdateState) {

      String asyncId = message.getStr(ASYNC);

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.add(CoreAdminParams.ACTION, CoreAdminParams.CoreAdminAction.UNLOAD.toString());
      params.add(CoreAdminParams.CORE, replicaName);

      params.set(CoreAdminParams.DELETE_INDEX, message.getBool(CoreAdminParams.DELETE_INDEX, true));
      params.set(CoreAdminParams.DELETE_INSTANCE_DIR, message.getBool(CoreAdminParams.DELETE_INSTANCE_DIR, true));
      params.set(CoreAdminParams.DELETE_DATA_DIR, message.getBool(CoreAdminParams.DELETE_DATA_DIR, true));
      params.set(CoreAdminParams.DELETE_METRICS_HISTORY, message.getBool(CoreAdminParams.DELETE_METRICS_HISTORY, true));

      isLive = ocmh.zkStateReader.getLiveNodes().contains(replica.getNodeName());


      if (isLive) {

          shardRequestTracker.sendShardRequest(replica.getNodeName(), params, shardHandler);
      }


      //    try {
      //      ocmh.deleteCoreNode(collectionName, replicaName, replica, core);
      //    } catch (Exception e) {
      //      ParWork.propagateInterrupt(e);
      //      results.add("failure", "Could not complete delete " + e.getMessage());
      //    }



    }

    response.clusterState = clusterState;

    return response;
  }

  boolean waitForCoreNodeGone(String collectionName, String shard, String replicaName, int timeoutms) throws InterruptedException {
    try {
      ocmh.zkStateReader.waitForState(collectionName, timeoutms, TimeUnit.MILLISECONDS, (l, c) -> {
        if (c == null)
          return true;
        Slice slice = c.getSlice(shard);
        if(slice == null) {
          return true;
        }
        Replica r = slice.getReplica(replicaName);
        if(r == null || !ocmh.zkStateReader.isNodeLive(r.getNodeName())) {
          return true;
        }
        return false;
      });
    } catch (TimeoutException e) {
      return false;
    }

    return true;
  }
}
