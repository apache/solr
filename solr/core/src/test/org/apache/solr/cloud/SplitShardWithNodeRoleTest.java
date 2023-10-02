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
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.util.LogLevel;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LogLevel(
    "org.apache.solr.cloud.overseer=DEBUG;org.apache.solr.cloud=DEBUG;org.apache.solr.cloud.api.collections=DEBUG")
public class SplitShardWithNodeRoleTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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

    Set<String> overseerNodes = new HashSet<>();
    System.setProperty(NodeRoles.NODE_ROLES_PROP, "data:off,overseer:preferred");
    try {
      overseerNodes.add(cluster.startJettySolrRunner().getNodeName());
      overseerNodes.add(cluster.startJettySolrRunner().getNodeName());
    } finally {
      System.clearProperty(NodeRoles.NODE_ROLES_PROP);
    }
    OverseerRolesTest.waitForNewOverseer(10, overseerNodes::contains, false);
  }

  @Test
  public void testSolrClusterWithNodeRoleWithSingleReplica() throws Exception {
    doSplit("coll_ONLY_NRT", 1, 1, 0);
  }

  @Test
  public void testSolrClusterWithNodeRoleWithPull() throws Exception {
    doSplit("coll_NRT_PULL", 1, 1, 1);
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
