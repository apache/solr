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

import java.util.Locale;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

public class SplitShardWithNodeRoleTest extends SolrCloudTestCase {
  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2).addConfig("conf", configset("cloud-minimal")).configure();
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "data:off,coordinator:on");

    try {
      cluster.startJettySolrRunner();
      cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }

    JettySolrRunner overseer1 = null;
    JettySolrRunner overseer2 = null;
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "data:off,overseer:preferred");
    try {
      overseer1 = cluster.startJettySolrRunner();
      overseer2 = cluster.startJettySolrRunner();
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }

    Thread.sleep(10000);
    String overseerLeader = getOverseerLeader(zkClient());
    String msg =
        String.format(
            Locale.ROOT,
            "Overseer leader should be from overseer %d or %d  node but %s",
            overseer1.getLocalPort(),
            overseer2.getLocalPort(),
            overseerLeader);
    assertTrue(
        msg,
        overseerLeader.contains(String.valueOf(overseer1.getLocalPort()))
            || overseerLeader.contains(String.valueOf(overseer2.getLocalPort())));
  }

  @Test
  public void testSolrClusterWithNodeRoleWithSingleReplica() throws Exception {
    doSplit("coll_NO_HA", 1, 1, 0);
  }

  @Test
  public void testSolrClusterWithNodeRoleWithHA() throws Exception {
    doSplit("coll_HA", 1, 1, 1);
  }

  public void doSplit(String collName, int shard, int nrtReplica, int pullReplica)
      throws Exception {
    CloudSolrClient client = cluster.getSolrClient();
    CollectionAdminRequest.createCollection(collName, "conf", shard, nrtReplica, 0, pullReplica)
        .setPerReplicaState(true)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collName, shard, nrtReplica + pullReplica);
    UpdateRequest ur = new UpdateRequest();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc2 = new SolrInputDocument();
      doc2.addField("id", "" + i);
      ur.add(doc2);
    }

    ur.commit(client, collName);

    CollectionAdminRequest.SplitShard splitShard =
        CollectionAdminRequest.splitShard(collName).setShardName("shard1");
    splitShard.process(cluster.getSolrClient());
    waitForState(
        "Timed out waiting for sub shards to be active. Number of active shards="
            + cluster
                .getSolrClient()
                .getClusterState()
                .getCollection(collName)
                .getActiveSlices()
                .size(),
        collName,
        activeClusterShape(shard + 1, 3 * (nrtReplica + pullReplica)));
  }
}
