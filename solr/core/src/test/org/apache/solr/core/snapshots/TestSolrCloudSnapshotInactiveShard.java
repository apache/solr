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
package org.apache.solr.core.snapshots;

import java.util.Collection;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A collection snapshot must not be recorded as successful when a shard has no active replica to
 * take a core snapshot.
 */
public class TestSolrCloudSnapshotInactiveShard extends SolrCloudTestCase {

  @BeforeClass
  public static void setupClass() throws Exception {
    useFactory("solr.StandardDirectoryFactory");
    configureCluster(2)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @Test
  public void testSnapshotFailsWhenShardHasNoActiveReplica() throws Exception {
    CloudSolrClient solrClient = cluster.getSolrClient();
    String collectionName = "snapshotInactiveShard";
    JettySolrRunner hostingNode = cluster.getJettySolrRunner(1);

    CollectionAdminRequest.createCollection(collectionName, "conf1", 1, 1)
        .setCreateNodeSet(hostingNode.getNodeName())
        .process(solrClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);

    cluster.stopJettySolrRunner(hostingNode);
    cluster.waitForJettyToStop(hostingNode);
    waitForState(
        "the only replica should not be active",
        collectionName,
        (liveNodes, collection) ->
            collection != null
                && collection.replicaStream().noneMatch(r -> r.getState() == Replica.State.ACTIVE));

    String commitName = "snap";
    SolrException e =
        expectThrows(
            SolrException.class,
            () ->
                new CollectionAdminRequest.CreateSnapshot(collectionName, commitName)
                    .process(solrClient));
    assertTrue(e.getMessage(), e.getMessage().contains("Failed to create snapshot on shards"));

    Collection<CollectionSnapshotMetaData> snapshots =
        SolrSnapshotManager.listSnapshots(
            ZkStateReader.from(solrClient).getZkClient(), collectionName);
    assertEquals(1, snapshots.size());
    CollectionSnapshotMetaData meta = snapshots.iterator().next();
    assertEquals(commitName, meta.getName());
    assertEquals(CollectionSnapshotMetaData.SnapshotStatus.Failed, meta.getStatus());
    assertTrue(meta.getReplicaSnapshots().isEmpty());
  }
}
