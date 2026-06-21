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

package org.apache.solr.cloud;

import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CoreDescriptor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

class ShardLeaderElectionContextBase extends ElectionContext {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Suffix appended to {@code leaderPath} (.../leaders/&lt;shard&gt;/leader) to form the single-leader
   * mutual-exclusion node (.../leaders/&lt;shard&gt;/leader_lock). It is a SIBLING of leaderPath -- not a
   * child -- so it never appears among leaderPath's children (which the fork reads back as the winning
   * replica's internal id). Created EPHEMERAL; exactly one replica per shard can hold it.
   */
  static final String LEADER_SINGLETON_SUFFIX = "_lock";

  protected final SolrZkClient zkClient;
  protected final LeaderElector leaderElector;
  protected volatile boolean closed;
  private final Integer id;
  private volatile boolean wasleader;
  private volatile boolean heldSingleton;

  public ShardLeaderElectionContextBase(LeaderElector leaderElector, String electionPath, String leaderPath, Replica replica, CoreDescriptor cd,
      SolrZkClient zkClient) {
    super(electionPath, leaderPath, replica, cd);
    this.zkClient = zkClient;
    this.id = replica.getInternalId();
    this.leaderElector = leaderElector;
  }

  @Override protected void cancelElection() throws InterruptedException, KeeperException {
    log.debug("cancel election for {}", replica);

    if (!zkClient.isAlive()) return;

    try {

      log.debug("Removing leader registration node on cancel");
      String leaderSeqPath = getLeaderSeqPath();
      if (leaderSeqPath != null) {
        zkClient.delete(leaderSeqPath, -1);
      }

      if (wasleader) {
        zkClient.delete(leaderPath + "/" + id, -1);
      }

      if (heldSingleton) {
        heldSingleton = false;
        try {
          zkClient.delete(leaderPath + LEADER_SINGLETON_SUFFIX, -1);
        } catch (NoNodeException ignore) {}
      }

    } catch (NoNodeException e) {

    } catch (Exception e) {
      log.info("Exception trying to cancel election {} {}", e.getClass().getName(), e.getMessage());
    }

    super.cancelElection();
  }

  @Override boolean runLeaderProcess(ElectionContext context, boolean weAreReplacement, int pauseBeforeStartMs)
      throws KeeperException, InterruptedException, IOException {
    wasleader = true;
    String leaderSeqPath = null;
    try {

      if (leaderElector.isLeader()) {
        return true;
      }

      leaderSeqPath = getLeaderSeqPath();

      if (leaderSeqPath == null) {
        log.warn("We have won as leader, but we have no leader election node known to us leaderPath {}", leaderPath);
        return true;
      }

      log.debug("Creating leader registration node {} after winning as {}", leaderPath, leaderSeqPath);

      // Single-leader mutual exclusion. The election queue is supposed to guarantee one head, but a
      // queue race (e.g. a recovering replica re-joining concurrently) can let two replicas of the
      // same shard both believe they are head and both reach this point. The fork's per-replica
      // registration node (leaderPath + '/' + internalId) is UNIQUE per replica, so it provides no
      // mutual exclusion -- both creates succeed and both replicas publish LEADER into the StateUpdates
      // channel, producing a two-leader slice state {1=L,2=L}. getLeader() then returns whichever
      // replica iterates first and, crucially, that stale LEADER entry is NOT ephemeral, so it survives
      // the leader's ZK-session loss and permanently wedges leader migration
      // (HttpPartitionTest.testLeaderZkSessionLoss: kill the leader, the survivor can never take over).
      // Guard with a single fixed-name EPHEMERAL node under leaderPath: only one replica per shard can
      // hold it at a time and it is released automatically when that replica's session ends, allowing a
      // clean re-election. A replica that loses this race must NOT become leader. The lock is a SIBLING
      // of leaderPath (.../leaders/<shard>/leader_lock), never a child, so it does not pollute the
      // leaderPath children that identify the winning replica.
      String singletonPath = leaderPath + LEADER_SINGLETON_SUFFIX;
      try {
        zkClient.mkdir(singletonPath, null, CreateMode.EPHEMERAL);
        heldSingleton = true;
      } catch (KeeperException.NodeExistsException e) {
        boolean ours = false;
        try {
          Stat stat = new Stat();
          zkClient.getData(singletonPath, null, stat, true);
          ours = stat.getEphemeralOwner() == zkClient.getSessionId();
        } catch (NoNodeException nne) {
          // released in the race gap; fail this pass so we rejoin the election and retry cleanly
        }
        if (!ours) {
          log.warn("Another replica already holds leadership for shard (path {}); not becoming leader {}", leaderPath, replica.getName());
          return false;
        }
        heldSingleton = true; // re-running while we still legitimately hold leadership
      }

      zkClient.mkdir(leaderPath + '/' + id, null, CreateMode.EPHEMERAL);

    } catch (NoNodeException e) {
      log.warn("No node exists for election {} {}", leaderSeqPath, leaderPath, e);
      return true;
    } catch (KeeperException.NodeExistsException e) {
      log.warn("Node already exists for election node={}", e.getPath(), e);

      return true;
    } catch (Exception e) {
      log.warn("Could not register as the leader because creating the ephemeral registration node in ZooKeeper failed", e);
      return false;
    }
    return true;
  }

  @Override public boolean isClosed() {
    return closed;
  }
}
