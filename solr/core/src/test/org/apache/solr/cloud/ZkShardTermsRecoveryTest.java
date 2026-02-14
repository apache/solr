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

import static org.hamcrest.Matchers.in;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ZkShardTermsRecoveryTest extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARDS = 2;
  private static final int NUM_REPLICAS = 5;
  private static int NUM_DOCS = 0;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3).addConfig("conf", configset("cloud-minimal")).configure();
    assertEquals(
        0,
        CollectionAdminRequest.createCollection(COLLECTION, "conf", NUM_SHARDS, NUM_REPLICAS)
            .process(cluster.getSolrClient())
            .getStatus());
    cluster.waitForActiveCollection(COLLECTION, 10, TimeUnit.SECONDS, 2, NUM_SHARDS * NUM_REPLICAS);
  }

  @Before
  public void waitForActiveState() {
    cluster.waitForActiveCollection(COLLECTION, 10, TimeUnit.SECONDS, 2, NUM_SHARDS * NUM_REPLICAS);
  }

  @Test
  public void testShardTermsInducedReplication() throws Exception {
    String shard = "shard2";
    if (random().nextBoolean()) {
      // Add uncommitted/committed documents, to test that part of the recovery
      UpdateRequest up = new UpdateRequest();
      for (int i = 0; i < 1000; i++) {
        up.add("id", "id2-" + i);
      }
      up.process(cluster.getSolrClient(), COLLECTION);
      NUM_DOCS += 1000;
      if (random().nextBoolean()) {
        cluster.getSolrClient().commit(COLLECTION);
      }
    }

    DocCollection docCollection = cluster.getZkStateReader().getCollection(COLLECTION);
    JettySolrRunner jetty = cluster.getRandomJetty(random());

    Slice shard2 = docCollection.getSlice(shard);
    Replica leader = shard2.getLeader();
    Replica replica = shard2.getReplicas(r -> !r.isLeader()).getFirst();
    List<Replica> recoveryReplicas =
        shard2.getReplicas(
            r -> r.getType().leaderEligible && !(r.equals(leader) || r.equals(replica)));

    ZkShardTerms shardTerms =
        jetty.getCoreContainer().getZkController().getShardTerms(COLLECTION, shard);
    // Increase the leader and another replica's shardTerms
    shardTerms.ensureHighestTerms(Set.of(leader.getName(), replica.getName()));
    ShardTerms shardTermsSnapshot = shardTerms.getShardTerms();
    for (Replica r : recoveryReplicas) {
      assertFalse(shardTermsSnapshot.canBecomeLeader(r.getName()));
    }
    waitForReplicasToGoIntoRecovery(shard, recoveryReplicas);

    // Recovery should succeed relatively quickly
    cluster.waitForActiveCollection(COLLECTION, 5, TimeUnit.SECONDS, 2, NUM_SHARDS * NUM_REPLICAS);
    shardTerms.refreshTerms();
    long maxTerm = shardTerms.getHighestTerm();
    for (Replica r : recoveryReplicas) {
      assertFalse(shardTerms.isRecovering(r.getName()));
      assertEquals(maxTerm, shardTerms.getTerm(r.getName()));
    }

    new UpdateRequest().commit(cluster.getSolrClient(), COLLECTION);
    waitForNumDocsInAllReplicas(NUM_DOCS, shard2.getReplicas(), "*:*");
  }

  @Test
  public void testShardTermsInducedLeaderElection() throws IOException, SolrServerException {
    String shard = "shard1";
    if (random().nextBoolean()) {
      // Add uncommitted/committed documents, to test that part of the recovery
      UpdateRequest up = new UpdateRequest();
      for (int i = 0; i < 1000; i++) {
        up.add("id", "id3-" + i);
      }
      up.process(cluster.getSolrClient(), COLLECTION);
      NUM_DOCS += 1000;
      if (random().nextBoolean()) {
        cluster.getSolrClient().commit(COLLECTION);
      }
    }

    DocCollection docCollection = cluster.getZkStateReader().getCollection(COLLECTION);
    JettySolrRunner jetty = cluster.getRandomJetty(random());

    // Increase the leader and another replica's shardTerms
    Slice shard1 = docCollection.getSlice(shard);
    Set<String> replicasToSetHighest =
        shard1.getReplicas(r -> !r.isLeader()).subList(1, 3).stream()
            .map(Replica::getName)
            .collect(Collectors.toSet());
    List<Replica> recoveryReplicas =
        shard1.getReplicas(r -> !replicasToSetHighest.contains(r.getName()));
    ZkShardTerms shardTerms =
        jetty.getCoreContainer().getZkController().getShardTerms(COLLECTION, shard);
    shardTerms.ensureHighestTerms(replicasToSetHighest);
    ShardTerms shardTermsSnapshot = shardTerms.getShardTerms();
    for (Replica r : recoveryReplicas) {
      assertFalse(shardTermsSnapshot.canBecomeLeader(r.getName()));
    }

    waitForState(
        "Wait for leadership to be given up", COLLECTION, dc -> dc.getLeader(shard) == null);
    waitForState("Wait for leadership to be taken", COLLECTION, dc -> dc.getLeader(shard) != null);
    waitForReplicasToGoIntoRecovery(shard, recoveryReplicas);
    cluster.waitForActiveCollection(COLLECTION, 5, TimeUnit.SECONDS, 2, NUM_SHARDS * NUM_REPLICAS);
    // Make sure that a leader election took place
    assertThat(
        cluster.getZkStateReader().getCollection(COLLECTION).getLeader(shard).getName(),
        in(replicasToSetHighest));
    shardTerms.refreshTerms();
    long maxTerm = shardTerms.getHighestTerm();
    for (Replica r : recoveryReplicas) {
      assertFalse(shardTerms.isRecovering(r.getName()));
      assertEquals(maxTerm, shardTerms.getTerm(r.getName()));
    }

    new UpdateRequest().commit(cluster.getSolrClient(), COLLECTION);
    waitForNumDocsInAllReplicas(NUM_DOCS, shard1.getReplicas(), "*:*");
  }

  private void waitForNumDocsInAllReplicas(int numDocs, Collection<Replica> replicas, String query)
      throws IOException, SolrServerException {
    for (Replica r : replicas) {
      if (!r.isActive(cluster.getSolrClient().getClusterState().getLiveNodes())) {
        continue;
      }
      try (SolrClient replicaClient = getHttpSolrClient(r)) {
        assertEquals(
            "Replica " + r.getName() + " not up to date",
            numDocs,
            replicaClient.query(new SolrQuery(query)).getResults().getNumFound());
      }
    }
  }

  private void waitForReplicasToGoIntoRecovery(String shard, Collection<Replica> recoveryReplicas) {
    Set<String> remainingReplicasToRecover =
        recoveryReplicas.stream().map(Replica::getName).collect(Collectors.toSet());

    waitForState(
        "Waiting for replicas to go into recovery",
        COLLECTION,
        5,
        TimeUnit.SECONDS,
        state -> {
          Slice shardState = state.getSlice(shard);
          for (Replica r : recoveryReplicas) {
            if (shardState.getReplica(r.getName()).getState() == Replica.State.RECOVERING) {
              remainingReplicasToRecover.remove(r.getName());
            }
          }
          return remainingReplicasToRecover.isEmpty();
        });
  }
}
