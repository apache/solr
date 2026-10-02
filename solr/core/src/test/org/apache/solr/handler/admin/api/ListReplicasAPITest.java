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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.api.model.ReplicaInfo;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.ReplicaStateProps;
import org.junit.Test;

/** Unit tests for {@link ListReplicas#toReplicaInfo}. */
public class ListReplicasAPITest extends SolrTestCase {

  @Test
  public void testCopiesClusterStatusReplicaFields() {
    Replica replica = replica("core_node1", "active", true, "host:8983_solr");

    ReplicaInfo info = ListReplicas.toReplicaInfo(replica, Set.of("host:8983_solr"));

    assertEquals("coll_shard1_replica_n1", info.core);
    assertEquals("http://host:8983/solr", info.baseUrl);
    assertEquals("host:8983_solr", info.nodeName);
    assertEquals("active", info.state);
    assertEquals("NRT", info.type);
    assertEquals(Boolean.TRUE, info.leader);
    assertEquals(Boolean.FALSE, info.forceSetState);
  }

  @Test
  public void testOmitsLeaderWhenReplicaIsNotLeader() {
    Replica replica = replica("core_node2", "active", false, "host:8983_solr");

    ReplicaInfo info = ListReplicas.toReplicaInfo(replica, Set.of("host:8983_solr"));

    assertNull(info.leader);
  }

  @Test
  public void testMarksReplicaDownWhenNodeIsNotLive() {
    Replica replica = replica("core_node1", "active", true, "dead:8983_solr");

    ReplicaInfo info = ListReplicas.toReplicaInfo(replica, Set.of("host:8983_solr"));

    assertEquals("down", info.state);
    assertEquals(Boolean.TRUE, info.leader);
  }

  @Test
  public void testCopiesAdditionalReplicaProperties() {
    Replica replica = replica("core_node1", "active", true, "host:8983_solr");
    replica.getProperties().put("property.preferredleader", "true");

    ReplicaInfo info = ListReplicas.toReplicaInfo(replica, Set.of("host:8983_solr"));

    assertEquals("true", info.getAdditionalProperties().get("property.preferredleader"));
    assertFalse(info.getAdditionalProperties().containsKey(ReplicaStateProps.CORE_NAME));
  }

  private static Replica replica(String name, String state, boolean leader, String nodeName) {
    Map<String, Object> props = new HashMap<>();
    props.put(ReplicaStateProps.CORE_NAME, "coll_shard1_replica_n1");
    props.put(ReplicaStateProps.NODE_NAME, nodeName);
    props.put(ReplicaStateProps.BASE_URL, "http://host:8983/solr");
    props.put(ReplicaStateProps.TYPE, "NRT");
    props.put(ReplicaStateProps.STATE, state);
    props.put(ReplicaStateProps.FORCE_SET_STATE, "false");
    if (leader) {
      props.put(ReplicaStateProps.LEADER, "true");
    }
    return new Replica(name, props, "coll", "shard1");
  }
}
