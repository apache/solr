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
package org.apache.solr.handler.admin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.Test;

/**
 * Unit tests for {@link ClusterStatus}'s shard/collection "health" computation (SOLR-15300),
 * specifically its interaction with the live-node cross-check.
 *
 * <p>state.json isn't proactively rewritten the instant a node dies - only live_nodes membership
 * changes immediately. A replica on a dead node can therefore still read "active" in state.json
 * for a window of time. {@link ClusterStatus#crossCheckReplicaStateWithLiveNodes} corrects this
 * for display, but if health were computed from the data *before* that correction runs (as it was
 * prior to this fix - see SOLR-15395), a shard with a replica on a dead node is reported GREEN,
 * contradicting the "down" state shown for that same replica in the same response.
 */
public class ClusterStatusTest extends SolrTestCaseJ4 {

  @Test
  @SuppressWarnings("unchecked")
  public void testHealthReflectsReplicaOnDeadNode() {
    // shard1: 2 replicas, both say "active" in state.json (as a real dead node would, before
    // Overseer gets around to rewriting it) - but only node1 is actually live.
    Map<String, Object> replica1 = new LinkedHashMap<>();
    replica1.put("state", "active");
    replica1.put("node_name", "node1:8983_solr");
    replica1.put("leader", "true");

    Map<String, Object> replica2 = new LinkedHashMap<>();
    replica2.put("state", "active");
    replica2.put("node_name", "node2:8983_solr"); // this node is dead

    Map<String, Object> replicas = new LinkedHashMap<>();
    replicas.put("core_node1", replica1);
    replicas.put("core_node2", replica2);

    Map<String, Object> shard1 = new LinkedHashMap<>();
    shard1.put("replicas", replicas);

    Map<String, Object> shards = new LinkedHashMap<>();
    shards.put("shard1", shard1);

    Map<String, Object> docCollection = new LinkedHashMap<>();
    docCollection.put("shards", shards);

    List<String> liveNodes = List.of("node1:8983_solr"); // node2 is absent

    // Exercise the same two steps, in the same order, as ClusterStatus.buildResponseForCollection:
    // cross-check against live nodes first, then compute health from the corrected data.
    ClusterStatus clusterStatus = new ClusterStatus(null, new ModifiableSolrParams());
    clusterStatus.crossCheckReplicaStateWithLiveNodes(liveNodes, docCollection);
    Map<String, Object> result = ClusterStatus.postProcessCollectionJSON(docCollection);

    // the dead replica's displayed state is corrected...
    assertEquals("down", replica2.get("state"));
    // ...and health must reflect that same corrected state (1 of 2 replicas up -> ORANGE),
    // not the stale "both active" view that produced GREEN before this fix.
    Map<String, Object> resultShard1 = (Map<String, Object>) ((Map<String, Object>) result.get("shards")).get("shard1");
    assertEquals("ORANGE", resultShard1.get("health"));
    assertEquals("ORANGE", result.get("health"));
  }
}
