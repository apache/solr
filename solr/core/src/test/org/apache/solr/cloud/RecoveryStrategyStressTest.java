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

import com.carrotsearch.randomizedtesting.annotations.Nightly;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

@Nightly
public class RecoveryStrategyStressTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    cluster = configureCluster(4).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void stressTestRecovery() throws Exception {
    final String collection = "recoveryStressTest";
    CollectionAdminRequest.createCollection(collection, "conf", 1, 4)
        .process(cluster.getSolrClient());
    waitForState(
        "Expected a collection with one shard and two replicas", collection, clusterShape(1, 4));
    final var scheduledExecutorService =
        Executors.newScheduledThreadPool(1, new SolrNamedThreadFactory("stressTestRecovery"));
    try (SolrClient solrClient =
        cluster.basicSolrClientBuilder().withDefaultCollection(collection).build()) {
      final StoppableIndexingThread indexThread =
          new StoppableIndexingThread(null, solrClient, "1", true, 10, 1, true);

      final var startAndStopCount = new CountDownLatch(50);
      final Thread startAndStopRandomReplicas =
          new Thread(
              () -> {
                try {
                  while (startAndStopCount.getCount() > 0) {
                    DocCollection state = getCollectionState(collection);
                    Replica leader = state.getLeader("shard1");
                    Replica replica =
                        getRandomReplica(state.getSlice("shard1"), (r) -> !leader.equals(r));

                    JettySolrRunner jetty = cluster.getReplicaJetty(replica);
                    jetty.stop();
                    Thread.sleep(100);
                    jetty.start();
                    startAndStopCount.countDown();
                  }
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
      startAndStopRandomReplicas.start();
      // index and commit doc after fixed interval of 10 sec
      scheduledExecutorService.scheduleWithFixedDelay(
          indexThread, 1000, 10000, TimeUnit.MILLISECONDS);
      scheduledExecutorService.scheduleWithFixedDelay(
          () -> {
            try {
              new UpdateRequest().commit(solrClient, collection);
            } catch (IOException e) {
              throw new RuntimeException(e);
            } catch (SolrServerException e) {
              throw new RuntimeException(e);
            }
          },
          100,
          10000,
          TimeUnit.MILLISECONDS);

      startAndStopCount.await();
      scheduledExecutorService.shutdownNow();
      // final commit to make documents visible for replicas
      new UpdateRequest().commit(solrClient, collection);
    }
    cluster.getZkStateReader().waitForState(collection, 120, TimeUnit.SECONDS, clusterShape(1, 4));

    // test that leader and replica have same doc count
    DocCollection state = getCollectionState(collection);
    assertShardConsistency(state.getSlice("shard1"), true);
  }

  private void assertShardConsistency(Slice shard, boolean expectDocs) throws Exception {
    List<Replica> replicas = shard.getReplicas(r -> r.getState() == Replica.State.ACTIVE);
    long[] numCounts = new long[replicas.size()];
    int i = 0;
    for (Replica replica : replicas) {
      try (var client =
          new HttpSolrClient.Builder(replica.getBaseUrl())
              .withDefaultCollection(replica.getCoreName())
              .withHttpClient(((CloudLegacySolrClient) cluster.getSolrClient()).getHttpClient())
              .build()) {
        numCounts[i] =
            client.query(new SolrQuery("*:*").add("distrib", "false")).getResults().getNumFound();
        i++;
      }
    }
    for (int j = 1; j < replicas.size(); j++) {
      if (numCounts[j] != numCounts[j - 1])
        fail("Mismatch in counts between replicas"); // TODO improve this!
      if (numCounts[j] == 0 && expectDocs)
        fail("Expected docs on shard " + shard.getName() + " but found none");
    }
  }
}
