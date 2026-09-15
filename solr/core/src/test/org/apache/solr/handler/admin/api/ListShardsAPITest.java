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
package org.apache.solr.handler.admin.api;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.api.model.ListShardsResponse;
import org.apache.solr.client.api.model.ListShardsResponse.ShardSummary;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for {@link ListShards}. */
public class ListShardsAPITest extends MockV2APITest {

  private ListShards api;

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(true);
    api = new ListShards(mockCoreContainer, mockQueryRequest, queryResponse);
  }

  @Test
  public void testReportsErrorIfCollectionNameMissing() {
    final SolrException thrown = expectThrows(SolrException.class, () -> api.listShards(null));

    assertEquals(400, thrown.code());
    assertEquals("Missing required parameter: collection", thrown.getMessage());
  }

  @Test
  public void testReportsErrorIfNotSolrCloud() {
    when(mockCoreContainer.isZooKeeperAware()).thenReturn(false);

    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.listShards("someCollection"));

    assertEquals(400, thrown.code());
    assertThat(thrown.getMessage(), containsString("not running in SolrCloud mode"));
  }

  @Test
  public void testReportsErrorIfCollectionMissing() {
    when(mockClusterState.getCollectionOrNull(eq("missingCollection"))).thenReturn(null);

    final SolrException thrown =
        expectThrows(SolrException.class, () -> api.listShards("missingCollection"));

    assertEquals(404, thrown.code());
    assertEquals("Collection not found: missingCollection", thrown.getMessage());
  }

  @Test
  public void testListsShardSummaries() {
    final Replica leader = replica("core_node1", "node1", Replica.State.ACTIVE, true);
    final Slice shard1 =
        slice("shard1", "80000000-ffffffff", Slice.State.ACTIVE, Map.of("core_node1", leader));
    final DocCollection collection = mock(DocCollection.class);
    when(collection.getSlicesMap()).thenReturn(Map.of("shard1", shard1));
    when(mockClusterState.getCollectionOrNull(eq("coll"))).thenReturn(collection);
    when(mockClusterState.getLiveNodes()).thenReturn(Set.of("node1"));

    final ListShardsResponse response = api.listShards("coll");

    assertEquals(1, response.shards.size());
    final ShardSummary summary = response.shards.get("shard1");
    assertEquals("active", summary.state);
    assertEquals("80000000-ffffffff", summary.range);
    assertEquals("GREEN", summary.replicaHealth);
    assertEquals(Integer.valueOf(1), summary.replicaCount);
    assertEquals(Integer.valueOf(1), summary.activeReplicaCount);
  }

  @Test
  public void testOmitsRangeWhenShardHasNone() {
    final Replica leader = replica("core_node1", "node1", Replica.State.ACTIVE, true);
    final Slice shard1 = slice("shard1", null, Slice.State.ACTIVE, Map.of("core_node1", leader));

    final ShardSummary summary = ListShards.toShardSummary(shard1, Set.of("node1"));

    assertNull(summary.range);
    assertEquals("GREEN", summary.replicaHealth);
  }

  @Test
  public void testReplicaHealthYellowWhenMinorityDown() {
    final Map<String, Replica> replicas = new LinkedHashMap<>();
    replicas.put("core_node1", replica("core_node1", "node1", Replica.State.ACTIVE, true));
    replicas.put("core_node2", replica("core_node2", "node2", Replica.State.ACTIVE, false));
    replicas.put("core_node3", replica("core_node3", "node3", Replica.State.DOWN, false));
    final Slice shard = slice("shard1", "0-7fffffff", Slice.State.ACTIVE, replicas);

    final ShardSummary summary =
        ListShards.toShardSummary(shard, Set.of("node1", "node2", "node3"));

    assertEquals("YELLOW", summary.replicaHealth);
    assertEquals(Integer.valueOf(3), summary.replicaCount);
    assertEquals(Integer.valueOf(2), summary.activeReplicaCount);
  }

  @Test
  public void testReplicaHealthOrangeWhenMajorityDown() {
    final Map<String, Replica> replicas = new LinkedHashMap<>();
    replicas.put("core_node1", replica("core_node1", "node1", Replica.State.ACTIVE, true));
    replicas.put("core_node2", replica("core_node2", "node2", Replica.State.DOWN, false));
    replicas.put("core_node3", replica("core_node3", "node3", Replica.State.DOWN, false));
    final Slice shard = slice("shard1", "0-7fffffff", Slice.State.ACTIVE, replicas);

    final ShardSummary summary =
        ListShards.toShardSummary(shard, Set.of("node1", "node2", "node3"));

    assertEquals("ORANGE", summary.replicaHealth);
    assertEquals(Integer.valueOf(1), summary.activeReplicaCount);
  }

  @Test
  public void testReplicaHealthRedWhenNoLeader() {
    final Map<String, Replica> replicas = new LinkedHashMap<>();
    replicas.put("core_node1", replica("core_node1", "node1", Replica.State.ACTIVE, false));
    replicas.put("core_node2", replica("core_node2", "node2", Replica.State.ACTIVE, false));
    final Slice shard = slice("shard1", "0-7fffffff", Slice.State.ACTIVE, replicas);

    final ShardSummary summary = ListShards.toShardSummary(shard, Set.of("node1", "node2"));

    assertEquals("RED", summary.replicaHealth);
    assertEquals(Integer.valueOf(2), summary.activeReplicaCount);
  }

  @Test
  public void testNonLiveNodeDoesNotCountAsActive() {
    final Map<String, Replica> replicas = new LinkedHashMap<>();
    replicas.put("core_node1", replica("core_node1", "node1", Replica.State.ACTIVE, true));
    replicas.put("core_node2", replica("core_node2", "dead-node", Replica.State.ACTIVE, false));
    final Slice shard = slice("shard1", "0-7fffffff", Slice.State.ACTIVE, replicas);

    final ShardSummary summary = ListShards.toShardSummary(shard, Set.of("node1"));

    assertEquals("ORANGE", summary.replicaHealth);
    assertEquals(Integer.valueOf(2), summary.replicaCount);
    assertEquals(Integer.valueOf(1), summary.activeReplicaCount);
  }

  private static Replica replica(String name, String node, Replica.State state, boolean leader) {
    final Map<String, Object> props = new LinkedHashMap<>();
    props.put(Replica.ReplicaStateProps.CORE_NAME, name + "_core");
    props.put(Replica.ReplicaStateProps.NODE_NAME, node);
    props.put(Replica.ReplicaStateProps.BASE_URL, "http://" + node + ":8983/solr");
    props.put(Replica.ReplicaStateProps.STATE, state.toString());
    props.put(Replica.ReplicaStateProps.TYPE, Replica.Type.NRT.toString());
    if (leader) {
      props.put(Replica.ReplicaStateProps.LEADER, "true");
    }
    return new Replica(name, props, "coll", "shard1");
  }

  private static Slice slice(
      String name, String range, Slice.State state, Map<String, Replica> replicas) {
    final Map<String, Object> props = new LinkedHashMap<>();
    props.put(Slice.SliceStateProps.STATE_PROP, state.toString());
    if (range != null) {
      props.put(Slice.SliceStateProps.RANGE, range);
    }
    return new Slice(name, replicas, props, "coll");
  }
}
