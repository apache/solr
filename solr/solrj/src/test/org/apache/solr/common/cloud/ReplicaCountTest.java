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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import org.junit.Test;

/** Unit tests for {@link ReplicaCount}. */
public class ReplicaCountTest {

  @Test
  public void testPutAndGet() {
    ReplicaCount replicaCount = ReplicaCount.empty();
    replicaCount.put(Replica.Type.NRT, 1);
    replicaCount.put(Replica.Type.TLOG, 2);

    assertTrue(replicaCount.contains(Replica.Type.NRT));
    assertTrue(replicaCount.contains(Replica.Type.TLOG));
    assertFalse(replicaCount.contains(Replica.Type.PULL));

    assertEquals(1, replicaCount.get(Replica.Type.NRT));
    assertEquals(2, replicaCount.get(Replica.Type.TLOG));
    assertEquals(0, replicaCount.get(Replica.Type.PULL));
  }

  @Test
  public void testPutNullValue() {
    ReplicaCount replicaCount = ReplicaCount.of(Replica.Type.NRT, 1);
    replicaCount.put(Replica.Type.NRT, null);

    assertFalse(replicaCount.contains(Replica.Type.NRT));
    assertEquals(0, replicaCount.get(Replica.Type.NRT));
  }

  @Test
  public void testPutNegativeValue() {
    ReplicaCount replicaCount = ReplicaCount.of(Replica.Type.NRT, -1);

    assertFalse(replicaCount.contains(Replica.Type.NRT));
    assertEquals(0, replicaCount.get(Replica.Type.NRT));
  }

  @Test
  public void testIncrement() {
    ReplicaCount replicaCount = ReplicaCount.empty();
    replicaCount.put(Replica.Type.NRT, 1);
    replicaCount.increment(Replica.Type.NRT);
    replicaCount.increment(Replica.Type.TLOG);

    assertTrue(replicaCount.contains(Replica.Type.NRT));
    assertTrue(replicaCount.contains(Replica.Type.TLOG));
    assertFalse(replicaCount.contains(Replica.Type.PULL));

    assertEquals(2, replicaCount.get(Replica.Type.NRT));
    assertEquals(1, replicaCount.get(Replica.Type.TLOG));
  }

  @Test
  public void testDecrement() {
    ReplicaCount replicaCount = ReplicaCount.empty();
    replicaCount.put(Replica.Type.NRT, 1);
    replicaCount.put(Replica.Type.PULL, 2);
    replicaCount.decrement(Replica.Type.NRT);
    replicaCount.decrement(Replica.Type.PULL);

    assertTrue(replicaCount.contains(Replica.Type.NRT));
    assertTrue(replicaCount.contains(Replica.Type.PULL));

    assertEquals(0, replicaCount.get(Replica.Type.NRT));
    assertEquals(1, replicaCount.get(Replica.Type.PULL));
  }

  @Test
  public void testDecrementZero() {
    ReplicaCount replicaCount = ReplicaCount.empty();
    replicaCount.decrement(Replica.Type.NRT);

    assertFalse(replicaCount.contains(Replica.Type.NRT));
    assertEquals(0, replicaCount.get(Replica.Type.NRT));
  }

  @Test
  public void testTotal() {
    ReplicaCount replicaCount = ReplicaCount.empty();
    replicaCount.put(Replica.Type.NRT, 1);
    replicaCount.put(Replica.Type.TLOG, 2);

    assertEquals(3, replicaCount.total());
  }

  @Test
  public void testToString() {
    ReplicaCount replicaCount = ReplicaCount.empty();
    replicaCount.put(Replica.Type.NRT, 1);
    replicaCount.put(Replica.Type.TLOG, 2);

    assertEquals("nrt=1, tlog=2, pull=0", replicaCount.toString());
  }

  @Test
  public void testGetLeaderType() {
    // With only 1 NRT, NRT should be returned.
    ReplicaCount replicaCount = ReplicaCount.of(Replica.Type.NRT, 1);
    assertEquals(Replica.Type.NRT, replicaCount.getLeaderType());

    // With 1 NRT and 1 TLOG, NRT is still returned as the leader type, as it comes
    // first in the order of preference.
    replicaCount.increment(Replica.Type.TLOG);
    assertEquals(Replica.Type.NRT, replicaCount.getLeaderType());

    // With only 1 TLOG, TLOG should now be returned.
    replicaCount.decrement(Replica.Type.NRT);
    assertEquals(Replica.Type.TLOG, replicaCount.getLeaderType());
  }

  @Test
  public void testHasLeaderReplica() {
    assertTrue(ReplicaCount.of(Replica.Type.NRT, 1).hasLeaderReplica());
    assertTrue(ReplicaCount.of(Replica.Type.TLOG, 1).hasLeaderReplica());
    assertFalse(ReplicaCount.of(Replica.Type.PULL, 1).hasLeaderReplica());
  }

  @Test
  public void createFromProps() {
    ReplicaCount numReplicas =
        ReplicaCount.fromProps(
            Map.of("nrtReplicas", "1", "tlogReplicas", "2", "pullReplicas", "3"));
    assertEquals(ReplicaCount.of(1, 2, 3), numReplicas);

    numReplicas =
        ReplicaCount.fromProps(Map.of("nrtReplicas", 1, "tlogReplicas", 2, "pullReplicas", 3));
    assertEquals(ReplicaCount.of(1, 2, 3), numReplicas);

    numReplicas = ReplicaCount.fromProps(Map.of("tlogReplicas", 1));
    assertEquals(Set.of(Replica.Type.TLOG), numReplicas.keySet());
  }

  @Test
  public void createFromMessage() {
    ReplicaCount numReplicas =
        ReplicaCount.fromMessage(
            new ZkNodeProps(Map.of("nrtReplicas", "1", "tlogReplicas", "2", "pullReplicas", "3")));
    assertEquals(ReplicaCount.of(1, 2, 3), numReplicas);

    numReplicas =
        ReplicaCount.fromMessage(
            new ZkNodeProps(Map.of("nrtReplicas", 1, "tlogReplicas", 2, "pullReplicas", 3)));
    assertEquals(ReplicaCount.of(1, 2, 3), numReplicas);

    numReplicas = ReplicaCount.fromMessage(new ZkNodeProps(Map.of("tlogReplicas", 1)));
    assertEquals(Set.of(Replica.Type.TLOG), numReplicas.keySet());
  }

  @Test
  public void createFromMessageWithReplicationFactor() {
    // "replicationFactor" in message
    ReplicaCount numReplicas =
        ReplicaCount.fromMessage(
            new ZkNodeProps(Map.of("replicationFactor", 10, "tlogReplicas", 2, "pullReplicas", 3)));
    assertEquals(ReplicaCount.of(10, 2, 3), numReplicas);

    // "replicationFactor" and "type" in message
    numReplicas =
        ReplicaCount.fromMessage(
            new ZkNodeProps(
                Map.of(
                    "type", "tlog", "replicationFactor", 10, "nrtReplicas", 1, "pullReplicas", 3)));
    assertEquals(ReplicaCount.of(1, 10, 3), numReplicas);
  }

  @Test
  public void createFromMessageWithDefaultReplicationFactor() {
    // default replication factor specified
    ReplicaCount numReplicas =
        ReplicaCount.fromMessage(
            new ZkNodeProps(Map.of("tlogReplicas", 2, "pullReplicas", 3)), null, 10);
    assertEquals(ReplicaCount.of(10, 2, 3), numReplicas);

    // default replication factor specified and "type" in message
    numReplicas =
        ReplicaCount.fromMessage(
            new ZkNodeProps(Map.of("type", "tlog", "nrtReplicas", 1, "pullReplicas", 3)), null, 10);
    assertEquals(ReplicaCount.of(1, 10, 3), numReplicas);
  }

  @Test
  public void createFromMessageWithCollection() {
    DocCollection collection =
        DocCollection.create(
            "coll",
            Map.of(),
            Map.of("nrtReplicas", 1, "tlogReplicas", 2, "pullReplicas", 3),
            null,
            1,
            null);
    ReplicaCount numReplicas =
        ReplicaCount.fromMessage(new ZkNodeProps(Map.of("tlogReplicas", 1)), collection);
    assertEquals(ReplicaCount.of(1, 1, 3), numReplicas);
  }
}
