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

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.solr.cloud.ActiveReplicaWatcher;
import org.apache.solr.common.SolrCloseableLatch;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CollectionStateWatcher;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaMigrationUtils {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Code to migrate replicas to already chosen nodes. This will create new replicas, and delete the
   * old replicas after the creation is done.
   *
   * <p>If an error occurs during the creation of new replicas, all new replicas will be deleted.
   *
   * @param ccc The collection command context to use from the API that calls this method
   * @param movements a map from replica to the new node that the replica should live on
   * @param parallel whether the replica creations should be done in parallel
   * @param waitForFinalState wait for the final state of all newly created replicas before
   *     continuing
   * @param timeout the amount of time to wait for new replicas to be created
   * @param asyncId If provided, the command will be run under the given asyncId
   * @param results push results (successful and failure) onto this list
   * @return whether the command was successful
   */
  static boolean migrateReplicas(
      CollectionCommandContext ccc,
      Map<Replica, String> movements,
      boolean parallel,
      boolean waitForFinalState,
      int timeout,
      String asyncId,
      NamedList<Object> results)
      throws IOException, InterruptedException, KeeperException {
    // how many leaders are we moving? for these replicas we have to make sure that either:
    // * another existing replica can become a leader, or
    // * we wait until the newly created replica completes recovery (and can become the new leader)
    // If waitForFinalState=true we wait for all replicas
    int numLeaders = 0;
    for (Replica replica : movements.keySet()) {
      if (replica.isLeader() || waitForFinalState) {
        numLeaders++;
      }
    }
    // map of collectionName_coreNodeName to watchers
    Map<String, CollectionStateWatcher> watchers = new HashMap<>();
    List<ZkNodeProps> createdReplicas = new ArrayList<>();

    AtomicBoolean anyOneFailed = new AtomicBoolean(false);
    SolrCloseableLatch countDownLatch =
        new SolrCloseableLatch(movements.size(), ccc.getCloseableToLatchOn());

    SolrCloseableLatch replicasToRecover =
        new SolrCloseableLatch(numLeaders, ccc.getCloseableToLatchOn());

    ClusterState clusterState = ccc.getZkStateReader().getClusterState();

    for (Map.Entry<Replica, String> movement : movements.entrySet()) {
      Replica sourceReplica = movement.getKey();
      String targetNode = movement.getValue();
      String sourceCollection = sourceReplica.getCollection();
      if (log.isInfoEnabled()) {
        log.info(
            "Going to create replica for collection={} shard={} on node={}",
            sourceCollection,
            sourceReplica.getShard(),
            targetNode);
      }

      ZkNodeProps msg =
          sourceReplica
              .toFullProps()
              .plus("parallel", String.valueOf(parallel))
              .plus(CoreAdminParams.NODE, targetNode);
      if (asyncId != null) msg.getProperties().put(ASYNC, asyncId);
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
                              "Failed to create replica for collection=%s shard=%s on node=%s",
                              sourceCollection,
                              sourceReplica.getShard(),
                              targetNode);
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
                            sourceReplica.getShard(),
                            targetNode);
                      }
                    }
                  })
              .get(0);

      if (addedReplica != null) {
        createdReplicas.add(addedReplica);
        if (sourceReplica.isLeader() || waitForFinalState) {
          String shardName = sourceReplica.getShard();
          String replicaName = sourceReplica.getName();
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
                    shardName,
                    replicaName,
                    addedReplica.getStr(ZkStateReader.CORE_NAME_PROP),
                    replicasToRecover);
          }
          watchers.put(key, watcher);
          log.debug("--- adding {}, {}", key, watcher);
          ccc.getZkStateReader().registerCollectionStateWatcher(sourceCollection, watcher);
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
      ccc.getZkStateReader().removeCollectionStateWatcher(e.getKey(), e.getValue());
    }
    if (anyOneFailed.get()) {
      log.info("Failed to create some replicas. Cleaning up all newly created replicas.");
      SolrCloseableLatch cleanupLatch =
          new SolrCloseableLatch(createdReplicas.size(), ccc.getCloseableToLatchOn());
      for (ZkNodeProps createdReplica : createdReplicas) {
        NamedList<Object> deleteResult = new NamedList<>();
        try {
          new DeleteReplicaCmd(ccc)
              .deleteReplica(
                  ccc.getZkStateReader().getClusterState(),
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
      return false;
    }

    // we have reached this far, meaning all replicas should have been recreated.
    // now cleanup the original replicas
    return cleanupReplicas(
        results, ccc.getZkStateReader().getClusterState(), movements.keySet(), ccc, asyncId);
  }

  static boolean cleanupReplicas(
      NamedList<Object> results,
      ClusterState clusterState,
      Collection<Replica> sourceReplicas,
      CollectionCommandContext ccc,
      String async)
      throws IOException, InterruptedException {
    SolrCloseableLatch cleanupLatch =
        new SolrCloseableLatch(sourceReplicas.size(), ccc.getCloseableToLatchOn());
    for (Replica sourceReplica : sourceReplicas) {
      String coll = sourceReplica.getCollection();
      String shard = sourceReplica.getShard();
      String type = sourceReplica.getType().toString();
      String node = sourceReplica.getNodeName();
      log.info(
          "Deleting replica type={} for collection={} shard={} on node={}",
          type,
          coll,
          shard,
          node);
      NamedList<Object> deleteResult = new NamedList<>();
      try {
        ZkNodeProps cmdMessage = sourceReplica.toFullProps();
        if (async != null) cmdMessage = cmdMessage.plus(ASYNC, async);
        new DeleteReplicaCmd(ccc)
            .deleteReplica(
                clusterState,
                cmdMessage.plus("parallel", "true"),
                deleteResult,
                () -> {
                  cleanupLatch.countDown();
                  if (deleteResult.get("failure") != null) {
                    synchronized (results) {
                      results.add(
                          "failure",
                          String.format(
                              Locale.ROOT,
                              "Failed to delete replica for collection=%s shard=%s on node=%s",
                              coll,
                              shard,
                              node));
                    }
                  }
                });
      } catch (KeeperException e) {
        log.warn("Error deleting ", e);
        cleanupLatch.countDown();
      } catch (Exception e) {
        log.warn("Error deleting ", e);
        cleanupLatch.countDown();
        throw e;
      }
    }
    log.debug("Waiting for delete node action to complete");
    return cleanupLatch.await(5, TimeUnit.MINUTES);
  }

  static List<Replica> getReplicasOfNodes(Collection<String> nodeNames, ClusterState state) {
    List<Replica> sourceReplicas = new ArrayList<>();
    for (Map.Entry<String, DocCollection> e : state.getCollectionsMap().entrySet()) {
      for (Slice slice : e.getValue().getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (nodeNames.contains(replica.getNodeName())) {
            sourceReplicas.add(replica);
          }
        }
      }
    }
    return sourceReplicas;
  }

  static List<Replica> getReplicasOfNode(String nodeName, ClusterState state) {
    List<Replica> sourceReplicas = new ArrayList<>();
    for (Map.Entry<String, DocCollection> e : state.getCollectionsMap().entrySet()) {
      for (Slice slice : e.getValue().getSlices()) {
        for (Replica replica : slice.getReplicas()) {
          if (nodeName.equals(replica.getNodeName())) {
            sourceReplicas.add(replica);
          }
        }
      }
    }
    return sourceReplicas;
  }
}
