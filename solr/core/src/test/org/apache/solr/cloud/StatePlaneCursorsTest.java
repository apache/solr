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

import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.StatePlaneCursors;
import org.junit.Test;

/**
 * Review P2 #2: the deferred replay buffer ({@link StatePlaneCursors}) must be bounded so a skipped
 * transition whose replica id never materializes in structure cannot accumulate without limit. The
 * cap is {@code solr.statePlane.maxDeferredEntries} (default 50000), evicting the eldest live entry.
 */
public class StatePlaneCursorsTest extends SolrTestCaseJ4 {

  // Mirror of StatePlaneCursors.MAX_DEFERRED default; the constant is read once at class-load so we
  // exercise the real default rather than racing a per-test system-property override.
  private static final int MAX_DEFERRED =
      Integer.getInteger("solr.statePlane.maxDeferredEntries", 50000);

  private static final String SHARD = "shard1";
  private static final int DOWN = 5;

  @Test
  public void testDeferredBufferIsBoundedAndEvictsOldest() {
    StatePlaneCursors cursors = new StatePlaneCursors();

    // Defer one more than the cap, all under one shard with distinct replica ids in insertion order.
    final int overfill = MAX_DEFERRED + 1;
    for (int rid = 0; rid < overfill; rid++) {
      cursors.defer(SHARD, rid, DOWN);
    }

    Map<Integer, Integer> snap = cursors.deferredSnapshot(SHARD);
    assertEquals(
        "deferred buffer must be capped at MAX_DEFERRED", MAX_DEFERRED, snap.size());
    assertFalse(
        "the eldest deferred entry (replicaId 0) must have been evicted", snap.containsKey(0));
    assertTrue(
        "the newest deferred entry must be retained", snap.containsKey(overfill - 1));
  }

  @Test
  public void testUndeferReclaimsCapacity() {
    StatePlaneCursors cursors = new StatePlaneCursors();
    for (int rid = 0; rid < MAX_DEFERRED; rid++) {
      cursors.defer(SHARD, rid, DOWN);
    }
    // Undefer one, then add two more distinct ids: the buffer must accept the first (capacity freed)
    // and still respect the cap on the second (evicting the new eldest live entry).
    cursors.undefer(SHARD, 0);
    cursors.defer(SHARD, MAX_DEFERRED, DOWN);
    assertEquals(MAX_DEFERRED, cursors.deferredSnapshot(SHARD).size());

    cursors.defer(SHARD, MAX_DEFERRED + 1, DOWN);
    Map<Integer, Integer> snap = cursors.deferredSnapshot(SHARD);
    assertEquals("cap holds after refill", MAX_DEFERRED, snap.size());
    assertTrue("freshest id retained", snap.containsKey(MAX_DEFERRED + 1));
  }

  @Test
  public void testAdoptIfAheadBoundsMergedDeferred() {
    StatePlaneCursors a = new StatePlaneCursors();
    StatePlaneCursors b = new StatePlaneCursors();
    a.advance(SHARD, 1, 1);
    b.advance(SHARD, 1, 2); // b ahead so adoptIfAhead proceeds
    for (int rid = 0; rid < MAX_DEFERRED; rid++) {
      a.defer(SHARD, rid, DOWN);
    }
    for (int rid = MAX_DEFERRED; rid < MAX_DEFERRED + 1000; rid++) {
      b.defer(SHARD, rid, DOWN);
    }
    a.adoptIfAhead(b);
    assertEquals(
        "adopted deferred entries must still respect the cap",
        MAX_DEFERRED,
        a.deferredSnapshot(SHARD).size());
  }

  @Test
  public void testCopyPreservesBoundedDeferred() {
    StatePlaneCursors cursors = new StatePlaneCursors();
    for (int rid = 0; rid < 10; rid++) {
      cursors.defer(SHARD, rid, DOWN);
    }
    StatePlaneCursors copy = cursors.copy();
    assertEquals(10, copy.deferredSnapshot(SHARD).size());
    // The copy's bound bookkeeping must be live: undefer one and add 10 more, staying consistent.
    copy.undefer(SHARD, 0);
    assertEquals(9, copy.deferredSnapshot(SHARD).size());
  }
}
