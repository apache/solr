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
package org.apache.solr.common.cloud;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

public class ReplicaTest extends SolrTestCaseJ4 {

  private static Replica newReplica(String name, Integer id) {
    Object2ObjectMap<String, Object> props = new Object2ObjectOpenHashMap<>();
    props.put(ZkStateReader.NODE_NAME_PROP, "127.0.0.1:8983_solr");
    props.put(ZkStateReader.REPLICA_TYPE, "NRT");
    if (id != null) {
      props.put("id", id);
    }
    return new Replica(name, props, "coll", 1, "shard1");
  }

  /** A5: a synthetic/bare replica with a null id must not NPE in equals(). */
  @Test
  public void testEqualsWithNullIdDoesNotThrow() {
    Replica a = newReplica("core_node1", null);
    Replica b = newReplica("core_node1", null);
    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  /** equals() must return false (not ClassCastException) for a foreign argument type. */
  @Test
  public void testEqualsForeignTypeReturnsFalse() {
    Replica a = newReplica("core_node1", 5);
    assertFalse(a.equals("not a replica"));
    assertFalse(a.equals(null));
  }

  /** A replica with an id is not equal to an otherwise-identical one with a null id. */
  @Test
  public void testEqualsDifferentId() {
    Replica withId = newReplica("core_node1", 5);
    Replica nullId = newReplica("core_node1", null);
    assertFalse(withId.equals(nullId));
    assertFalse(nullId.equals(withId));
  }
}
