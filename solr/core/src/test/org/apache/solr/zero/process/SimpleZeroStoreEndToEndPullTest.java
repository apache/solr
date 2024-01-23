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
package org.apache.solr.zero.process;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.ZeroConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.After;
import org.junit.Test;

/** A simple end-to-end pull test for collections using a Zero store */
public class SimpleZeroStoreEndToEndPullTest extends ZeroStoreSolrCloudTestCase {

  @After
  public void teardownTest() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Tests that if an update gets processed by a leader and the leader pushes to the Zero store,
   * then a replica receiving a query should pull the deltas from the Zero store
   */
  @Test
  public void testCorePullSucceeds() throws Exception {
    System.setProperty(ZeroConfig.ZeroSystemProperty.corePullRetryDelayS.getPropertyName(), "0");
    setupCluster(2);
    CloudSolrClient cloudClient = cluster.getSolrClient();

    JettySolrRunner solrProcess1 = cluster.getJettySolrRunner(0);
    Map<String, CountDownLatch> asyncPullLatches1 = configureTestZeroProcessForNode(solrProcess1);

    JettySolrRunner solrProcess2 = cluster.getJettySolrRunner(1);
    Map<String, CountDownLatch> asyncPullLatches2 = configureTestZeroProcessForNode(solrProcess2);

    // this map tracks the async pull queues per solr process
    Map<String, Map<String, CountDownLatch>> solrProcessesTaskTracker = new HashMap<>();
    solrProcessesTaskTracker.put(solrProcess1.getNodeName(), asyncPullLatches1);
    solrProcessesTaskTracker.put(solrProcess2.getNodeName(), asyncPullLatches2);

    String collectionName = "zeroCollection";
    int numReplicas = 2;
    // specify a comma-delimited string of shard names for multiple shards when using
    // an implicit router
    String shardNames = "shard1";
    setupZeroCollectionWithShardNames(collectionName, numReplicas, shardNames);

    // send an update to the cluster
    UpdateRequest updateReq = new UpdateRequest();
    updateReq.add("id", "1");
    updateReq.commit(cloudClient, collectionName);

    // get the leader replica and follower replicas
    DocCollection collection =
        cluster.getZkStateReader().getClusterState().getCollection(collectionName);
    Replica shardLeaderReplica = collection.getLeader("shard1");
    Replica followerReplica = null;
    for (Replica repl : collection.getSlice("shard1").getReplicas()) {
      if (!repl.getName().equals(shardLeaderReplica.getName())) {
        followerReplica = repl;
        break;
      }
    }

    // verify the update wasn't forwarded to the follower and it didn't commit by checking the core
    // this gives us confidence that the subsequent query we do triggers the pull
    CoreContainer replicaCC = getCoreContainer(followerReplica.getNodeName());
    SolrCore core = null;
    SolrClient followerDirectClient = null;
    SolrClient leaderDirectClient = null;
    try {
      core = replicaCC.getCore(followerReplica.getCoreName());
      // the follower should only have the default segments file
      assertEquals(1, core.getDeletionPolicy().getLatestCommit().getFileNames().size());

      // query the leader directly to verify it should have the document
      leaderDirectClient =
          getHttpSolrClient(
              shardLeaderReplica.getBaseUrl() + "/" + shardLeaderReplica.getCoreName());
      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set("q", "*:*").set("distrib", "false");
      QueryResponse resp = leaderDirectClient.query(params);
      assertEquals(1, resp.getResults().getNumFound());
      assertEquals("1", resp.getResults().get(0).getFieldValue("id"));

      // we want to wait until the pull completes so set up a count down latch for the follower's
      // core that we'll wait until pull finishes for
      CountDownLatch latch = new CountDownLatch(1);
      Map<String, CountDownLatch> asyncPullTasks =
          solrProcessesTaskTracker.get(followerReplica.getNodeName());
      asyncPullTasks.put(followerReplica.getCoreName(), latch);

      // query the follower directly to trigger the pull, this query should yield no results
      // as it returns immediately
      followerDirectClient =
          getHttpSolrClient(followerReplica.getBaseUrl() + "/" + followerReplica.getCoreName());
      primingPullQuery(followerDirectClient, params);

      // wait until pull is finished
      assertTrue(latch.await(60, TimeUnit.SECONDS));

      // do another query to verify we've pulled everything
      resp = followerDirectClient.query(params);

      // verify we pulled
      assertTrue(core.getDeletionPolicy().getLatestCommit().getFileNames().size() > 1);

      // verify the document is present
      assertEquals(1, resp.getResults().getNumFound());
      assertEquals("1", resp.getResults().get(0).getFieldValue("id"));
    } finally {
      leaderDirectClient.close();
      followerDirectClient.close();
      core.close();
    }
  }
}
