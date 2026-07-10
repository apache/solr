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

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.framework.api.transaction.OperationType;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ElectionContext implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  final String electionPath;
  final ZkNodeProps leaderProps;
  final String id;
  final String leaderPath;

  /** Parent of {@link #leaderPath}; derived once since {@link #leaderPath} is final. */
  final String leaderParentPath;

  volatile String leaderSeqPath;
  private SolrZkClient zkClient;

  /**
   * Version of {@link #leaderPath}'s parent captured when this context registered as leader (see
   * {@link #registerLeaderNode}); {@link #deleteLeaderNode} uses it on cancel to remove only our
   * own registration (ABA-safe). Null until/after registration. Thread-safety is provided by each
   * subclass's own election lock — there is no base-level lock guarding this field.
   */
  protected Integer leaderZkNodeParentVersion;

  public ElectionContext(
      final String coreNodeName,
      final String electionPath,
      final String leaderPath,
      final ZkNodeProps leaderProps,
      final SolrZkClient zkClient) {
    assert zkClient != null;
    this.id = coreNodeName;
    this.electionPath = electionPath;
    this.leaderPath = leaderPath;
    this.leaderParentPath = ZkMaintenanceUtils.getZkParent(leaderPath);
    this.leaderProps = leaderProps;
    this.zkClient = zkClient;
  }

  @Override
  public void close() {}

  public void cancelElection() throws InterruptedException, KeeperException {
    if (leaderSeqPath != null) {
      try {
        log.debug("Canceling election {}", leaderSeqPath);
        zkClient.delete(leaderSeqPath, -1);
      } catch (NoNodeException e) {
        // fine
        log.debug("cancelElection did not find election node to remove {}", leaderSeqPath);
      }
    } else {
      log.debug("cancelElection skipped as this context has not been initialized");
    }
  }

  abstract void runLeaderProcess(boolean weAreReplacement)
      throws KeeperException, InterruptedException;

  public void checkIfIamLeaderFired() {}

  public void joinedElectionFired() {}

  /**
   * Create a new instance when retrying the election.
   *
   * <p>This carries over all election parameters, but not current status (mostly the {@link
   * SyncStrategy} is not copied for shared leader election.
   */
  public ElectionContext copy() {
    throw new UnsupportedOperationException("copy");
  }

  /**
   * Registers {@link #leaderPath} as an ephemeral leader node in a single multi transaction that
   * also bumps the version of the leader node's parent (via a {@code setData}), capturing it into
   * {@link #leaderZkNodeParentVersion} so {@link #deleteLeaderNode()} can later remove <em>only our
   * own</em> registration, ABA-safe. The transaction also sanity-checks that {@link #leaderSeqPath}
   * still exists.
   *
   * <p>Does the ZooKeeper work only; callers own any retry, locking, and error handling around it
   * (which legitimately differ between overseer and shard leader election).
   */
  protected void registerLeaderNode(byte[] leaderData)
      throws KeeperException, InterruptedException {
    List<CuratorTransactionResult> results =
        zkClient.multi(
            op -> op.check().withVersion(-1).forPath(leaderSeqPath),
            op -> op.create().withMode(CreateMode.EPHEMERAL).forPath(leaderPath, leaderData),
            op -> op.setData().withVersion(-1).forPath(leaderParentPath, null));
    leaderZkNodeParentVersion =
        results.stream()
            .filter(
                CuratorTransactionResult.ofTypeAndPath(OperationType.SET_DATA, leaderParentPath))
            .findFirst()
            .orElseThrow(
                () ->
                    new RuntimeException(
                        "Could not set data for parent path in ZK: " + leaderParentPath))
            .getResultStat()
            .getVersion();
  }

  /**
   * Deletes {@link #leaderPath} guarded by {@link #leaderZkNodeParentVersion}, so it only removes a
   * registration whose parent version still matches — i.e. our own, never a newer lineage's/host's.
   * Must only be called when {@link #leaderZkNodeParentVersion} is non-null. Callers own locking,
   * any exception handling (e.g. treating {@link KeeperException.BadVersionException}/{@link
   * NoNodeException} as "not ours / already gone"), and clearing {@link #leaderZkNodeParentVersion}
   * afterward.
   */
  protected void deleteLeaderNode() throws KeeperException, InterruptedException {
    zkClient.multi(
        op -> op.check().withVersion(leaderZkNodeParentVersion).forPath(leaderParentPath),
        op -> op.delete().withVersion(-1).forPath(leaderPath));
  }
}
