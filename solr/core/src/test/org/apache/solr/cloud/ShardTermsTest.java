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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

public class ShardTermsTest extends SolrTestCase {
  @Test
  public void testIncreaseTerms() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 0L);
    ShardTerms terms = new ShardTerms(map, 0);
    terms = terms.increaseTerms("leader", Set.of("replica"));
    assertEquals(1L, terms.getTerm("leader").longValue());

    map.put("leader", 2L);
    map.put("live-replica", 2L);
    map.put("dead-replica", 1L);
    terms = new ShardTerms(map, 0);
    assertNull(terms.increaseTerms("leader", Set.of("dead-replica")));

    terms = terms.increaseTerms("leader", Set.of("leader"));
    assertEquals(3L, terms.getTerm("live-replica").longValue());
    assertEquals(2L, terms.getTerm("leader").longValue());
    assertEquals(1L, terms.getTerm("dead-replica").longValue());
  }

  @Test
  public void testSetHighestTerms() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 0L);
    ShardTerms terms = new ShardTerms(map, 0);
    terms = terms.setHighestTerms(Set.of("leader"));
    assertNull(terms);

    map.put("leader", 2L);
    map.put("live-replica", 2L);
    map.put("another-replica", 2L);
    map.put("bad-replica", 2L);
    map.put("dead-replica", 1L);
    terms = new ShardTerms(map, 0);

    terms = terms.setHighestTerms(Set.of("live-replica", "another-replica"));
    assertEquals(3L, terms.getTerm("live-replica").longValue());
    assertEquals(3L, terms.getTerm("another-replica").longValue());
    assertEquals(2L, terms.getTerm("leader").longValue());
    assertEquals(2L, terms.getTerm("bad-replica").longValue());
    assertEquals(1L, terms.getTerm("dead-replica").longValue());
  }

  @Test
  public void testRecoveryFailedClearsRecoveringState() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 7L);
    map.put("replica", 6L);
    ShardTerms terms = new ShardTerms(map, 0);

    terms = terms.startRecovering("replica");
    assertTrue(terms.isRecovering("replica"));

    terms = terms.recoveryFailed("replica");
    assertFalse(terms.isRecovering("replica"));
    // the speculative raise is rolled back: the replica keeps the term its data had reached
    assertEquals(6L, terms.getTerm("replica").longValue());
    assertFalse(terms.canBecomeLeader("replica"));
  }

  @Test
  public void testRecoveryFailedKeepsFreshestFailedReplicaLeaderEligible() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 7L);
    map.put("fresh-replica", 6L);
    map.put("stale-replica", 3L);
    ShardTerms terms = new ShardTerms(map, 0);

    terms = terms.startRecovering("fresh-replica");
    terms = terms.startRecovering("stale-replica");
    terms = terms.recoveryFailed("fresh-replica");
    terms = terms.recoveryFailed("stale-replica");
    terms = terms.removeTerm("leader");

    assertEquals(6L, terms.getTerm("fresh-replica").longValue());
    assertEquals(3L, terms.getTerm("stale-replica").longValue());
    // of the two failed replicas, only the one whose data was most current may lead
    assertTrue(terms.canBecomeLeader("fresh-replica"));
    assertFalse(terms.canBecomeLeader("stale-replica"));
  }

  @Test
  public void testRecoveryFailedDoesNotChangeReplicaWithoutRecoveringState() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 2L);
    map.put("replica", 1L);
    ShardTerms terms = new ShardTerms(map, 0);

    assertNull(terms.recoveryFailed("replica"));
    assertEquals(1L, terms.getTerm("replica").longValue());
  }

  @Test
  public void testRecoveryFailedLetsIsolatedReplicaRejoinLeaderElection() {
    Map<String, Long> map = new HashMap<>();
    map.put("leader", 7L);
    map.put("replica", 6L);
    ShardTerms terms = new ShardTerms(map, 0);

    terms = terms.startRecovering("replica");
    terms = terms.removeTerm("leader");
    assertFalse(terms.canBecomeLeader("replica"));

    terms = terms.recoveryFailed("replica");
    assertFalse(terms.isRecovering("replica"));
    assertEquals(6L, terms.getTerm("replica").longValue());
    assertTrue(terms.canBecomeLeader("replica"));
  }
}
