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

import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.BeforeClass;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * See SOLR-9504.
 *
 * MRM TODO: Requires a production fix before this test can pass (June 2026 investigation).
 *
 * Confirmed root cause — SOLR-9504 data-loss via graceful-stop term removal:
 *
 *   1. The original leader (replicaJetty) stops. CoreContainer.unload() calls
 *      ZkController.unregister(..., removeTerms=true), which deletes the leader's shard-term
 *      entry from ZK. After this, no replica has term {@literal >} 0 in ZK.
 *
 *   2. addReplica creates a brand-new empty replica (term=0, no UpdateLog data).
 *      In ShardLeaderElectionContext.runLeaderProcess, the new replica is NOT yet registered
 *      in ZkShardTerms (zkShardTerms.registered(coreName)==false), so the outer SOLR-9504
 *      guard block is bypassed entirely and the empty replica proceeds to PeerSync.
 *
 *   3. PeerSync finds no live peers. The anotherReplicaHasData() check is reached. The
 *      original leader's cluster state is DOWN (StateUpdates entry removed when its live node
 *      left) and its shard term was deleted in step 1, so anotherReplicaHasData() returns
 *      false. The new empty replica becomes leader.
 *
 *   4. The original leader restarts, recovers FROM the empty new leader, and loses its 10
 *      committed docs. Both replicas end up with 0 docs; assertEquals(10, numFound) fails.
 *
 * Required production fix: preserve shard terms on graceful stop (pass removeTerms=false in
 * CoreContainer.unload for the normal stop path), so that anotherReplicaHasData()'s term-based
 * fallback can still detect the data-bearing original leader. Alternatively, ensure the
 * addReplica-path guard fires even when the new replica has not yet registered in ZkShardTerms.
 */
public class TestLeaderElectionWithEmptyReplica extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String COLLECTION_NAME = "solr_9504";

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Ensure IndexWriter.close() commits on node stop — the global test-framework default
    // sets solr.skipCommitOnClose=true (for fast teardown), but this test verifies on-disk
    // data after restart and needs the full commit path to function correctly.
    System.setProperty("solr.skipCommitOnClose", "false");

    useFactory(null);
    configureCluster(2)
        .addConfig("config", SolrTestUtil.TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION_NAME, "config", 1, 1)
        .process(cluster.getSolrClient());
  }

  @Test
  public void test() throws Exception {
    CloudHttp2SolrClient solrClient = cluster.getSolrClient();
    solrClient.setDefaultCollection(COLLECTION_NAME);
    for (int i=0; i<10; i++)  {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", String.valueOf(i));
      solrClient.add(doc);
    }
    solrClient.commit();

    // find the leader node

    JettySolrRunner replicaJetty = cluster.getShardLeaderJetty(COLLECTION_NAME, "s1");


    // kill the leader
    replicaJetty.stop();

    // add a replica (asynchronously)
    CollectionAdminRequest.AddReplica addReplica = CollectionAdminRequest.addReplicaToShard(COLLECTION_NAME, "s1");
    String asyncId = addReplica.processAsync(solrClient);

    //cluster.waitForActiveCollection(COLLECTION_NAME, 1, 2);


    // bring the old leader node back up
    replicaJetty.start();

    // This is a restart scenario (kill leader -> add empty replica -> restart old leader). Full
    // convergence requires overseer leadership handoff (the killed node was the overseer), the empty
    // replica declining leadership via the SOLR-9504 guard and waiting out leaderVoteWait (5s), and
    // the restarted leader re-registering and re-winning the shard election. That legitimately takes
    // longer than the default 10s wait, so the collection occasionally was still mid-convergence when
    // the wait expired (~10% flaky TimeoutException here). Give it the restart-test-standard 60s.
    cluster.waitForActiveCollection(COLLECTION_NAME, 60, java.util.concurrent.TimeUnit.SECONDS, 1, 2);

    // now query each replica and check for consistency
    assertConsistentReplicas(solrClient, solrClient.getZkStateReader().getClusterState().getCollection(COLLECTION_NAME).getSlice("s1"));

    // sanity check that documents still exist
    QueryResponse response = solrClient.query(new SolrQuery("*:*"));
    assertEquals("Indexed documents not found", 10, response.getResults().getNumFound());
  }

  private static int assertConsistentReplicas(CloudHttp2SolrClient cloudClient, Slice shard) throws SolrServerException, IOException {
    long numFound = Long.MIN_VALUE;
    int count = 0;
    for (Replica replica : shard.getReplicas()) {
      Http2SolrClient client = new Http2SolrClient.Builder(replica.getCoreUrl())
          .withHttpClient(cloudClient.getHttpClient()).build();
      QueryResponse response = client.query(new SolrQuery("q", "*:*", "distrib", "false"));
      if (log.isInfoEnabled()) {
        log.info("Found numFound={} on replica: {}", response.getResults().getNumFound(), replica.getCoreUrl());
      }
      if (numFound == Long.MIN_VALUE)  {
        numFound = response.getResults().getNumFound();
      } else  {
        assertEquals("Shard " + shard.getName() + " replicas do not have same number of documents", numFound, response.getResults().getNumFound());
      }
      count++;
    }
    return count;
  }
}
