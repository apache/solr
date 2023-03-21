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
package org.apache.solr.core;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.Utils;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCoreInconsistentReplica extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  /**
   * Tests core registration during core startup. If the node name mismatch:
   *
   * <ul>
   *   <li>it should NOT publish to state.json to "autocorrect" the node name
   *   <li>it should NOT interfere with other core creations that have correct node name
   * </ul>
   */
  public void testCoreLoad() throws Exception {
    String collectionName = "testCoreInconsistentReplica";

    CollectionAdminRequest.createCollection(collectionName, "conf", 2, 1)
        .process(cluster.getSolrClient());
    waitForState(
        "Expected a cluster of 2 shards and 1 replica each",
        collectionName,
        (n, c) -> {
          return DocCollection.isFullyActive(n, c, 2, 1);
        });
    JettySolrRunner oldJetty = cluster.getJettySolrRunner(0);
    cluster.stopJettySolrRunner(oldJetty);

    // now modify one core to have a different node name
    byte[] data =
        cluster
            .getZkClient()
            .getData(DocCollection.getCollectionPath(collectionName), null, null, true);
    Object json = Utils.fromJSON(data);
    @SuppressWarnings("unchecked")
    Map<String, Object> replicasJson =
        (Map<String, Object>)
            Utils.getObjectByPath(json, false, collectionName + "/shards/shard1/replicas");
    @SuppressWarnings("unchecked")
    Map<String, Object> replicaJson = (Map<String, Object>) replicasJson.values().iterator().next();
    String oldNodeName = (String) replicaJson.get("node_name");
    String oldBaseUrl = (String) replicaJson.get("base_url");
    String newNodeName =
        oldNodeName.replace(":" + oldJetty.getLocalPort(), ":" + (oldJetty.getLocalPort() + 1));
    String newBaseUrl =
        oldBaseUrl.replace(":" + oldJetty.getLocalPort(), ":" + (oldJetty.getLocalPort() + 1));
    replicaJson.put("node_name", newNodeName);
    replicaJson.put("base_url", newBaseUrl);

    cluster
        .getZkClient()
        .setData(DocCollection.getCollectionPath(collectionName), Utils.toJSON(json), true);

    cluster.startJettySolrRunner(oldJetty, true);

    waitForState(
        "Expected state.json to remain the same, with shard2 core to come up",
        collectionName,
        (n, docCollection) -> {
          if (docCollection.getActiveSlices().size()
              != 2) { // both shards/slices should still be active
            return false;
          }
          List<Replica> replicas = docCollection.getReplicas(oldJetty.getNodeName());
          if (replicas.size()
              != 1) { // should only have one replica of shard2 left on this node (as we manually
            // modified shard1)
            return false;
          }
          if (!"shard2".equals(replicas.get(0).getShard())) {
            return false;
          }
          if (replicas.get(0).getState()
              != Replica.State.ACTIVE) { // this replica should come up no problem
            return false;
          }

          // shard1 replica should NOT be reverted to the old node name in state.json
          List<Replica> movedReplicas = docCollection.getReplicas(newNodeName);
          if (movedReplicas.size() != 1) {
            return false;
          }
          if (!"shard1".equals(movedReplicas.get(0).getShard())) {
            return false;
          }
          return true;
        });
  }
}
