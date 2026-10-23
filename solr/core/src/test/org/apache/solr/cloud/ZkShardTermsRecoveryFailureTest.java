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

import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.util.TestInjection;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Deterministic cloud-level test for SOLR-7394: when a recovering replica exhausts its recovery
 * attempts, {@link RecoveryStrategy} must clear the replica's {@code _recovering} shard-terms entry
 * and restore the term the replica had before recovery began, so the replica can win a future
 * leader election once no higher-term replica remains.
 *
 * <p>Recovery failure is forced deterministically via {@link TestInjection} hooks (armed only after
 * the replica publishes RECOVERING, so the failure path is exercised realistically). The test-only
 * retry limit seam in {@link TestInjection} ({@code recoveryMaxRetriesOverride}) keeps the run
 * fast.
 */
public class ZkShardTermsRecoveryFailureTest extends SolrCloudTestCase {

  private static final int NODE_COUNT = 3;

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.directoryFactory", "solr.StandardDirectoryFactory");
    System.setProperty("solr.ulog.numRecordsToKeep", "1000");
    System.setProperty("leaderVoteWait", "2000");
    configureCluster(NODE_COUNT).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testFailedRecoveryClearsShardTermsAndRestoresEligibility() throws Exception {
    final String collectionName = "recoveryFailure";

    // single replica on node 0 becomes the leader
    CollectionAdminRequest.createCollection(collectionName, 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);
    cluster.getSolrClient().add(collectionName, new SolrInputDocument("id", "1"));
    cluster.getSolrClient().commit(collectionName);

    Replica leader = getCollectionState(collectionName).getSlice("shard1").getLeader();
    assertNotNull("no leader elected for the new collection", leader);
    final String leaderNodeName = leader.getNodeName();

    String replicaNodeName = cluster.getJettySolrRunner(1).getNodeName();
    if (replicaNodeName.equals(leaderNodeName)) {
      replicaNodeName = cluster.getJettySolrRunner(2).getNodeName();
    }
    final String replicaNode = replicaNodeName;

    // keep the run fast: exhaust recovery retries after a handful of attempts;
    // force recovery to fail deterministically via TestInjection
    TestInjection.recoveryMaxRetriesOverride = 3;
    TestInjection.failRecovery = "true:100";
    try {
      // add an NRT replica on another node; it must start recovering and then give up
      CollectionAdminRequest.addReplicaToShard(collectionName, "shard1")
          .setNode(replicaNode)
          .process(cluster.getSolrClient());

      waitForState(
          "replica never published RECOVERING",
          collectionName,
          60,
          TimeUnit.SECONDS,
          state -> isReplicaInState(state, replicaNode, Replica.State.RECOVERING));

      waitForState(
          "replica never published RECOVERY_FAILED",
          collectionName,
          120,
          TimeUnit.SECONDS,
          state -> isReplicaInState(state, replicaNode, Replica.State.RECOVERY_FAILED));

      final String failedCoreNodeName =
          getCollectionState(collectionName).getSlice("shard1").getReplicas().stream()
              .filter(r -> r.getNodeName().equals(replicaNode))
              .map(Replica::getName)
              .findFirst()
              .orElseThrow(() -> new AssertionError("failed replica not found in cluster state"));

      // SOLR-7394: the failed recovery must have cleared the _recovering shard-terms entry and
      // restored the replica's pre-recovery term (0 here: the replica was brand new)
      try (ZkShardTerms zkShardTerms =
          new ZkShardTerms(collectionName, "shard1", cluster.getZkClient())) {
        assertFalse(
            "failed replica must not retain a _recovering shard-terms entry",
            zkShardTerms.isRecovering(failedCoreNodeName));
        assertEquals(
            "failed replica term must be restored to its pre-recovery value",
            0L,
            zkShardTerms.getTerm(failedCoreNodeName));
        // while the higher-term leader remains, the failed replica must not be eligible
        assertFalse(
            "failed replica must not be leader-eligible while a higher-term replica remains",
            zkShardTerms.canBecomeLeader(failedCoreNodeName));
      }

      // stop the higher-term leader; the failed replica must become the new leader, proving its
      // eligibility was restored
      TestInjection.failRecovery = null;
      cluster.stopJettySolrRunner(cluster.getJettySolrRunner(0));

      waitForState(
          "failed replica did not become leader after the higher-term leader was stopped",
          collectionName,
          120,
          TimeUnit.SECONDS,
          state -> {
            Replica newLeader = state.getSlice("shard1").getLeader();
            return newLeader != null && newLeader.getName().equals(failedCoreNodeName);
          });
    } finally {
      TestInjection.recoveryMaxRetriesOverride = null;
      TestInjection.failRecovery = null;
    }
  }

  private static boolean isReplicaInState(
      DocCollection state, String nodeName, Replica.State expected) {
    return state.getSlice("shard1").getReplicas().stream()
        .anyMatch(r -> r.getNodeName().equals(nodeName) && r.getState() == expected);
  }
}
