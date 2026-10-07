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

/** Tests that cluster health reflects replica states corrected against live nodes. */
public class ClusterStatusTest extends SolrTestCaseJ4 {

  @Test
  @SuppressWarnings("unchecked")
  public void testHealthReflectsReplicaOnDeadNode() {
    // Both replicas are marked active, but only node1 is live.
    Map<String, Object> replica1 = new LinkedHashMap<>();
    replica1.put("state", "active");
    replica1.put("node_name", "node1:8983_solr");
    replica1.put("leader", "true");

    Map<String, Object> replica2 = new LinkedHashMap<>();
    replica2.put("state", "active");
    replica2.put("node_name", "node2:8983_solr");

    Map<String, Object> replicas = new LinkedHashMap<>();
    replicas.put("core_node1", replica1);
    replicas.put("core_node2", replica2);

    Map<String, Object> shard1 = new LinkedHashMap<>();
    shard1.put("replicas", replicas);

    Map<String, Object> shards = new LinkedHashMap<>();
    shards.put("shard1", shard1);

    Map<String, Object> docCollection = new LinkedHashMap<>();
    docCollection.put("shards", shards);

    List<String> liveNodes = List.of("node1:8983_solr");

    // Match buildResponseForCollection: correct replica states, then compute health.
    ClusterStatus clusterStatus = new ClusterStatus(null, new ModifiableSolrParams());
    clusterStatus.crossCheckReplicaStateWithLiveNodes(liveNodes, docCollection);
    Map<String, Object> result = ClusterStatus.postProcessCollectionJSON(docCollection);

    assertEquals("down", replica2.get("state"));
    // One of two replicas is active, so shard and collection health are ORANGE.
    Map<String, Object> resultShard1 =
        (Map<String, Object>) ((Map<String, Object>) result.get("shards")).get("shard1");
    assertEquals("ORANGE", resultShard1.get("health"));
    assertEquals("ORANGE", result.get("health"));
  }
}
