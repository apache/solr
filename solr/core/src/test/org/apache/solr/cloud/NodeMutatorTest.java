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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4Test;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.ClusterStateProvider;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL // tests compare for http:
public class NodeMutatorTest extends SolrTestCaseJ4Test {

  private static final String NODE3 = "baseUrl3:8985_";
  private static final String NODE3_URL = "http://baseUrl3:8985/solr";

  private static final String NODE2 = "baseUrl2:8984_";
  private static final String NODE2_URL = "http://baseUrl2:8984/solr";

  private static final String NODE1 = "baseUrl1:8983_";
  private static final String NODE1_URL = "http://baseUrl1:8983/solr";

  @Test
  public void downNodeReportsAllImpactedCollectionsAndNothingElse() {
    NodeMutator nm = new NodeMutator(null);

    // Collection1: 2 shards X 1 replica = replica1 on node1 and replica2 on node2
    // Collection2: 1 shard X 1 replica = replica1 on node2
    ZkStateReader reader = ClusterStateMockUtil.buildClusterState("csrr2rDcsr2", 1, NODE1, NODE2);
    ClusterState clusterState = reader.getClusterState();
    assertEquals(
        clusterState.getCollection("collection1").getReplica("replica1").getBaseUrl(), NODE1_URL);
    assertEquals(
        clusterState.getCollection("collection1").getReplica("replica2").getBaseUrl(), NODE2_URL);
    assertEquals(
        clusterState.getCollection("collection2").getReplica("replica4").getBaseUrl(), NODE2_URL);

    ZkNodeProps props = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, NODE1);
    List<ZkWriteCommand> writes = nm.downNode(clusterState, props);
    assertEquals(writes.size(), 1);
    assertEquals(writes.get(0).name, "collection1");
    assertEquals(writes.get(0).collection.getReplica("replica1").getState(), Replica.State.DOWN);
    assertEquals(writes.get(0).collection.getReplica("replica2").getState(), Replica.State.ACTIVE);
    reader.close();

    // Collection1: 2 shards X 1 replica = replica1 on node1 and replica2 on node2
    // Collection2: 1 shard X 1 replica = replica1 on node2
    // Collection3: 1 shard X 3 replica = replica1 on node1 , replica2 on node2, replica3 on node3
    reader = ClusterStateMockUtil.buildClusterState("csrr2rDcsr2csr1r2r3", 1, NODE1, NODE2, NODE3);
    clusterState = reader.getClusterState();
    assertEquals(
        clusterState.getCollection("collection1").getReplica("replica1").getBaseUrl(), NODE1_URL);
    assertEquals(
        clusterState.getCollection("collection1").getReplica("replica2").getBaseUrl(), NODE2_URL);

    assertEquals(
        clusterState.getCollection("collection2").getReplica("replica4").getBaseUrl(), NODE2_URL);

    assertEquals(
        clusterState.getCollection("collection3").getReplica("replica5").getBaseUrl(), NODE1_URL);
    assertEquals(
        clusterState.getCollection("collection3").getReplica("replica6").getBaseUrl(), NODE2_URL);
    assertEquals(
        clusterState.getCollection("collection3").getReplica("replica7").getBaseUrl(), NODE3_URL);

    writes = nm.downNode(clusterState, props);
    assertEquals(writes.size(), 2);
    for (ZkWriteCommand write : writes) {
      if (write.name.equals("collection1")) {
        assertEquals(write.collection.getReplica("replica1").getState(), Replica.State.DOWN);
        assertEquals(write.collection.getReplica("replica2").getState(), Replica.State.ACTIVE);
      } else if (write.name.equals("collection3")) {
        assertEquals(write.collection.getReplica("replica5").getState(), Replica.State.DOWN);
        assertEquals(write.collection.getReplica("replica6").getState(), Replica.State.ACTIVE);
        assertEquals(write.collection.getReplica("replica7").getState(), Replica.State.ACTIVE);
      } else {
        fail("No other collection needs to be changed");
      }
    }
    reader.close();
  }

  @Test
  public void downNodeLoadsOnlyTheCollectionsTheMessageLists() {
    // Collection1: replica1 on node1, replica2 on node2
    // Collection2: replica4 on node2
    // Collection3: replica5 on node1, replica6 on node2, replica7 on node3
    ZkStateReader reader =
        ClusterStateMockUtil.buildClusterState("csrr2rDcsr2csr1r2r3", 1, NODE1, NODE2, NODE3);
    ClusterState clusterState = reader.getClusterState();
    // collection1 is left out to show that the list, not a scan, decides what is loaded
    ZkNodeProps listed =
        new ZkNodeProps(
            Map.of(
                ZkStateReader.NODE_NAME_PROP,
                NODE1,
                NodeMutator.DOWN_NODE_COLLECTIONS_PROP,
                List.of("collection3", "collection2", "noSuchCollection")));

    // the list is ignored unless the switch is on
    assertEquals(2, new NodeMutator(null).downNode(clusterState, listed).size());

    assumeWorkingMockito();
    ClusterStateProvider clusterProps = mock(ClusterStateProvider.class);
    when(clusterProps.getClusterProperty(ZkStateReader.USE_LISTED_COLLECTIONS_ONLY_FOR_DOWN_NODE))
        .thenReturn("false", "true");
    SolrCloudManager cloudManager = mock(SolrCloudManager.class);
    when(cloudManager.getClusterStateProvider()).thenReturn(clusterProps);
    assertEquals(2, new NodeMutator(cloudManager).downNode(clusterState, listed).size());

    try {
      NodeMutator nm = new NodeMutator(cloudManager);
      int loadsBefore = loads(clusterState, "collection1");
      List<ZkWriteCommand> writes = nm.downNode(clusterState, listed);
      assertEquals(1, writes.size());
      assertEquals("collection3", writes.get(0).name);
      assertEquals(Replica.State.DOWN, writes.get(0).collection.getReplica("replica5").getState());
      assertEquals(
          Replica.State.ACTIVE, writes.get(0).collection.getReplica("replica6").getState());
      assertEquals(loadsBefore, loads(clusterState, "collection1"));

      // an empty list loads nothing
      ZkNodeProps empty =
          new ZkNodeProps(
              Map.of(
                  ZkStateReader.NODE_NAME_PROP,
                  NODE1,
                  NodeMutator.DOWN_NODE_COLLECTIONS_PROP,
                  List.of()));
      assertTrue(nm.downNode(clusterState, empty).isEmpty());
      assertEquals(loadsBefore, loads(clusterState, "collection1"));

      // a message without the list still scans every collection
      ZkNodeProps unlisted = new ZkNodeProps(ZkStateReader.NODE_NAME_PROP, NODE1);
      assertEquals(2, nm.downNode(clusterState, unlisted).size());

      // a message whose list isn't a list is rejected
      ZkNodeProps notAList =
          new ZkNodeProps(
              ZkStateReader.NODE_NAME_PROP,
              NODE1,
              NodeMutator.DOWN_NODE_COLLECTIONS_PROP,
              "collection3");
      expectThrows(ClassCastException.class, () -> nm.downNode(clusterState, notAList));
      // but not while the switch is off
      assertEquals(2, new NodeMutator(null).downNode(clusterState, notAList).size());
    } finally {
      reader.close();
    }
  }

  private static int loads(ClusterState clusterState, String collection) {
    return clusterState.getCollectionRef(collection).getCount();
  }
}
