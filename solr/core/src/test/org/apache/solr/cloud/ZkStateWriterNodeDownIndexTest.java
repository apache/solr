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

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies the finding #6 node-down path: ZkStateWriter resolves a downed node's replicas through its
 * nodeName-&gt;placement index ({@code getReplicasOnNode}) instead of scanning every collection and every
 * replica, and marks EXACTLY that node's replicas DOWN while replicas on still-live nodes are unaffected.
 *
 * This exercises the real production path end to end (the live-nodes listener and
 * WorkQueueWatcher.processStateUpdateNode both now resolve placements via the index), so a drift between
 * the index and cluster state would surface as either a missed DOWN on the victim node or a spurious DOWN
 * on a survivor.
 */
public class ZkStateWriterNodeDownIndexTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(3)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }

  @Test
  public void testNodeDownMarksExactlyThatNodesReplicasDown() throws Exception {
    final String collection = "nodeDownIndex";
    CollectionAdminRequest.createCollection(collection, "conf", 2, 2)
        .setMaxShardsPerNode(4)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 2, 4);

    // Pick a victim node that hosts at least one replica while at least one other live node also hosts a
    // replica, so the "exactly that node" assertion is meaningful (a downed node's replicas DOWN, a
    // survivor's replicas not DOWN).
    final DocCollection dc = getCollectionState(collection);
    JettySolrRunner victim = null;
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      final String node = jetty.getNodeName();
      boolean hostsReplica = dc.getReplicas().stream().anyMatch(r -> node.equals(r.getNodeName()));
      boolean othersHostReplica = dc.getReplicas().stream().anyMatch(r -> !node.equals(r.getNodeName()));
      if (hostsReplica && othersHostReplica) {
        victim = jetty;
        break;
      }
    }
    assertNotNull("expected a node hosting some but not all of the collection's replicas", victim);

    final String victimNode = victim.getNodeName();
    final Set<String> victimReplicas = new HashSet<>();
    final Set<String> survivorReplicas = new HashSet<>();
    for (Replica r : dc.getReplicas()) {
      if (victimNode.equals(r.getNodeName())) {
        victimReplicas.add(r.getName());
      } else {
        survivorReplicas.add(r.getName());
      }
    }
    log.info("victim node={} victimReplicas={} survivorReplicas={}", victimNode, victimReplicas, survivorReplicas);
    assertFalse("victim must host at least one replica", victimReplicas.isEmpty());
    assertFalse("at least one survivor replica must exist", survivorReplicas.isEmpty());

    victim.stop();

    waitForState("Expected exactly the downed node's replicas to be marked DOWN", collection,
        (liveNodes, state) -> {
          // Every replica on the downed node must be DOWN ...
          for (String name : victimReplicas) {
            Replica r = state.getReplica(name);
            if (r == null || r.getState() != Replica.State.DOWN) {
              return false;
            }
          }
          // ... and no replica on a still-live node may be DOWN (it may be ACTIVE or the new LEADER).
          for (String name : survivorReplicas) {
            Replica r = state.getReplica(name);
            if (r == null || r.getState() == Replica.State.DOWN) {
              return false;
            }
          }
          return true;
        }, 60, TimeUnit.SECONDS);
  }
}
