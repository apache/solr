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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class LeaderElectionIntegrationTest extends SolrCloudTestCase {
  private static final int NUM_REPLICAS_OF_SHARD1 = 5;

  @BeforeClass
  public static void beforeClass() {
    System.setProperty("solrcloud.skip.autorecovery", "true");
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    configureCluster(6).addConfig("conf", configset("cloud-minimal")).configure();
  }

  private void createCollection(String collection) throws IOException, SolrServerException {
    assertEquals(
        0,
        CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
            .process(cluster.getSolrClient())
            .getStatus());
    for (int i = 1; i < NUM_REPLICAS_OF_SHARD1; i++) {
      assertTrue(
          CollectionAdminRequest.addReplicaToShard(collection, "shard1")
              .process(cluster.getSolrClient())
              .isSuccess());
    }
  }

  @Test
  public void testSimpleSliceLeaderElection() throws Exception {
    String collection = "collection1";
    createCollection(collection);

    waitForState(
        "Timeout waiting for collection to become active",
        collection,
        clusterShape(2, NUM_REPLICAS_OF_SHARD1 + 1));
    List<JettySolrRunner> stoppedRunners = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      // who is the leader?
      String leader = getLeader(collection);
      JettySolrRunner jetty = getRunner(leader);
      assertNotNull(jetty);
      assertEquals(
          "shard1",
          jetty
              .getCoreContainer()
              .getCoreDescriptors()
              .iterator()
              .next()
              .getCloudDescriptor()
              .getShardId());
      String jettyNodeName = jetty.getNodeName(); // must get before shutdown
      jetty.stop();
      stoppedRunners.add(jetty);
      waitForState(
          "Leader should not be " + jettyNodeName,
          collection,
          c ->
              c.getLeader("shard1") != null
                  && !jettyNodeName.equals(c.getLeader("shard1").getNodeName()));
    }

    for (JettySolrRunner runner : stoppedRunners) {
      runner.start();
    }
    waitForState(
        "Expected to see nodes come back for " + collection, collection, (n, c) -> n.size() == 6);
    CollectionAdminRequest.deleteCollection(collection).process(cluster.getSolrClient());

    // testLeaderElectionAfterClientTimeout
    collection = "collection2";
    createCollection(collection);

    // TODO: work out the best timing here...
    System.setProperty("zkClientTimeout", Integer.toString(ZkTestServer.TICK_TIME * 2 + 100));
    // timeout the leader
    String leader = getLeader(collection);
    JettySolrRunner jetty = getRunner(leader);
    assertNotNull(jetty);
    cluster.expireZkSession(jetty);

    // Wait until leadership has moved away from the expired-session node
    waitForState(
        "Expected leader to move away after expiring zk session",
        collection,
        c -> {
          var l = c.getLeader("shard1");
          return l != null && !jetty.getNodeName().equals(l.getNodeName());
        });

    // Wait until the expired-session node is live again before stopping others
    waitForState(
        "Expected expired-session node to rejoin live nodes",
        collection,
        (liveNodes, c) -> liveNodes.contains(jetty.getNodeName()));

    // kill everyone but the first leader that should have reconnected by now
    for (JettySolrRunner jetty2 : cluster.getJettySolrRunners()) {
      if (jetty != jetty2) {
        jetty2.stop();
      }
    }

    waitForState(
        "Expected original node to become leader after others stopped",
        collection,
        c -> {
          var l = c.getLeader("shard1");
          return l != null && jetty.getNodeName().equals(l.getNodeName());
        });
  }

  private JettySolrRunner getRunner(String nodeName) {
    for (JettySolrRunner jettySolrRunner : cluster.getJettySolrRunners()) {
      if (!jettySolrRunner.isStopped() && nodeName.equals(jettySolrRunner.getNodeName()))
        return jettySolrRunner;
    }
    return null;
  }

  private String getLeader(String collection) throws InterruptedException {

    ZkNodeProps props = cluster.getZkStateReader().getLeaderRetry(collection, "shard1", 30000);

    return props.getStr(ZkStateReader.NODE_NAME_PROP);
  }
}
