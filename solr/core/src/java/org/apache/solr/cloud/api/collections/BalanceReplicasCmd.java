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

import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.cluster.Node;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

public class BalanceReplicasCmd implements CollApiCmds.CollectionApiCommand {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final CollectionCommandContext ccc;

  public BalanceReplicasCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    ZkStateReader zkStateReader = ccc.getZkStateReader();
    Set<String> nodes;
    Object nodesRaw = message.get(CollectionParams.NODES);
    if (nodesRaw == null) {
      nodes = Collections.emptySet();
    } else if (nodesRaw instanceof Set) {
      nodes = (Set<String>)nodesRaw;
    } else if (nodesRaw instanceof Collection) {
      nodes = new HashSet<>((Collection<String>)nodesRaw);
    } else if (nodesRaw instanceof String) {
      nodes = Set.of(((String) nodesRaw).split(","));
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'nodes' was not passed as a correct type (Set/List/String): "
              + nodesRaw.getClass().getName());
    }
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);
    String async = message.getStr(ASYNC);
    int timeout = message.getInt("timeout", 10 * 60); // 10 minutes
    boolean parallel = message.getBool("parallel", false);
    ClusterState clusterState = zkStateReader.getClusterState();

    if (nodes.size() == 1) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cannot balance across a single node: "
              + nodes.stream().findAny().get());
    }

    Assign.AssignStrategy assignStrategy = Assign.createAssignStrategy(ccc.getCoreContainer());
    Map<Replica, Node> replicaMovements = assignStrategy.balanceReplicas(ccc.getSolrCloudManager(), nodes, message.getInt(CollectionParams.MAX_BALANCE_SKEW, -1));

    // TODO: Move the below logic out and share with ReplaceNodeCmd

    // how many leaders are we moving? for these replicas we have to make sure that either:
    // * another existing replica can become a leader, or
    // * we wait until the newly created replica completes recovery (and can become the new leader)
    // If waitForFinalState=true we wait for all replicas
    int numLeaders = 0;
    for (ZkNodeProps props : replicaMovements.keySet()) {
      if (props.getBool(ZkStateReader.LEADER_PROP, false) || waitForFinalState) {
        numLeaders++;
      }
    }
    // map of collectionName_coreNodeName to watchers
    Map<String, CollectionStateWatcher> watchers = new HashMap<>();
    List<ZkNodeProps> createdReplicas = new ArrayList<>();

    AtomicBoolean anyOneFailed = new AtomicBoolean(false);
    SolrCloseableLatch countDownLatch =
        new SolrCloseableLatch(replicaMovements.size(), ccc.getCloseableToLatchOn());

    SolrCloseableLatch replicasToRecover =
        new SolrCloseableLatch(numLeaders, ccc.getCloseableToLatchOn());

    int replicaPositionIdx = 0;
    for (Map.Entry<Replica, Node> movement : replicaMovements.entrySet()) {
      Replica sourceReplica = movement.getKey();
      Node targetNode = movement.getValue();
      String sourceCollection = sourceReplica.getCollection();
      String sourceShard = sourceReplica.getShard();
      if (log.isInfoEnabled()) {
        log.info(
            "Going to create replica for collection={} shard={} on node={}",
            sourceCollection,
            sourceShard,
            targetNode.getName());
      }
      ZkNodeProps msg =
          sourceReplica
              .toFullProps()
              .plus("parallel", String.valueOf(parallel))
              .plus(CoreAdminParams.NODE, targetNode.getName());
      if (async != null) msg.getProperties().put(ASYNC, async);
      NamedList<Object> nl = new NamedList<>();
      final ZkNodeProps addedReplica =
          new AddReplicaCmd(ccc)
              .addReplica(
                  clusterState,
                  msg,
                  nl,
                  () -> {
                    countDownLatch.countDown();
                    if (nl.get("failure") != null) {
                      String errorString =
                          String.format(
                              Locale.ROOT,
                              "Failed to create replica for collection=%s shard=%s" + " on node=%s",
                              sourceCollection,
                              sourceShard,
                              targetNode.getName());
                      log.warn(errorString);
                      // one replica creation failed. Make the best attempt to
                      // delete all the replicas created so far in the target
                      // and exit
                      synchronized (results) {
                        results.add("failure", errorString);
                        anyOneFailed.set(true);
                      }
                    } else {
                      if (log.isDebugEnabled()) {
                        log.debug(
                            "Successfully created replica for collection={} shard={} on node={}",
                            sourceCollection,
                            sourceShard,
                            targetNode.getName());
                      }
                    }
                  })
              .get(0);

      if (addedReplica != null) {
        createdReplicas.add(addedReplica);
        if (sourceReplica.getBool(ZkStateReader.LEADER_PROP, false) || waitForFinalState) {
          String replicaName = sourceReplica.getStr(ZkStateReader.REPLICA_PROP);
          String key = sourceCollection + "_" + replicaName;
          CollectionStateWatcher watcher;
          if (waitForFinalState) {
            watcher =
                new ActiveReplicaWatcher(
                    sourceCollection,
                    null,
                    Collections.singletonList(addedReplica.getStr(ZkStateReader.CORE_NAME_PROP)),
                    replicasToRecover);
          } else {
            watcher =
                new LeaderRecoveryWatcher(
                    sourceCollection,
                    sourceShard,
                    replicaName,
                    addedReplica.getStr(ZkStateReader.CORE_NAME_PROP),
                    replicasToRecover);
          }
          watchers.put(key, watcher);
          log.debug("--- adding {}, {}", key, watcher);
          zkStateReader.registerCollectionStateWatcher(sourceCollection, watcher);
        } else {
          log.debug("--- not waiting for {}", addedReplica);
        }
      }
    }

    log.debug("Waiting for replicas to be added");
    if (!countDownLatch.await(timeout, TimeUnit.SECONDS)) {
      log.info("Timed out waiting for replicas to be added");
      anyOneFailed.set(true);
    } else {
      log.debug("Finished waiting for replicas to be added");
    }

    // now wait for leader replicas to recover
    log.debug("Waiting for {} leader replicas to recover", numLeaders);
    if (!replicasToRecover.await(timeout, TimeUnit.SECONDS)) {
      if (log.isInfoEnabled()) {
        log.info(
            "Timed out waiting for {} leader replicas to recover", replicasToRecover.getCount());
      }
      anyOneFailed.set(true);
    } else {
      log.debug("Finished waiting for leader replicas to recover");
    }
    // remove the watchers, we're done either way
    for (Map.Entry<String, CollectionStateWatcher> e : watchers.entrySet()) {
      zkStateReader.removeCollectionStateWatcher(e.getKey(), e.getValue());
    }
    if (anyOneFailed.get()) {
      log.info("Failed to create some replicas. Cleaning up all replicas on target node");
      SolrCloseableLatch cleanupLatch =
          new SolrCloseableLatch(createdReplicas.size(), ccc.getCloseableToLatchOn());
      for (ZkNodeProps createdReplica : createdReplicas) {
        NamedList<Object> deleteResult = new NamedList<>();
        try {
          new DeleteReplicaCmd(ccc)
              .deleteReplica(
                  zkStateReader.getClusterState(),
                  createdReplica.plus("parallel", "true"),
                  deleteResult,
                  () -> {
                    cleanupLatch.countDown();
                    if (deleteResult.get("failure") != null) {
                      synchronized (results) {
                        results.add(
                            "failure",
                            "Could not cleanup, because of : " + deleteResult.get("failure"));
                      }
                    }
                  });
        } catch (KeeperException e) {
          cleanupLatch.countDown();
          log.warn("Error deleting replica ", e);
        } catch (Exception e) {
          log.warn("Error deleting replica ", e);
          cleanupLatch.countDown();
          throw e;
        }
      }
      cleanupLatch.await(5, TimeUnit.MINUTES);
      return;
    }

    // we have reached this far means all replicas could be recreated
    // now cleanup the replicas in the source node
    DeleteNodeCmd.cleanupReplicas(results, state, replicaMovements.keySet(), ccc, async);
    results.add(
        "success",
        "BalanceReplicas action completed successfully across nodes  : [" + String.join(", ", nodes) + "]");
  }
}
