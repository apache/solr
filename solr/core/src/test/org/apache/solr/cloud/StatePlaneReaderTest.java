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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ShardStateLog;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StatePlaneCursors;
import org.apache.solr.common.cloud.StatePlaneReader;
import org.apache.solr.common.cloud.StateSnapshot;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

/**
 * PR-3 READER unit tests for the StateUpdates delta plane apply logic ({@link StatePlaneReader} +
 * {@link StatePlaneCursors}). Pure (no ZK, no MiniSolrCloudCluster): build a {@link DocCollection}
 * directly (mirroring {@link ClusterStateMockUtil}) and fold decoded Phase-1
 * {@link ShardStateLog} / {@link StateSnapshot} objects onto it.
 *
 * <p>Short states (raw, on the wire): LEADER=1, ACTIVE=2, BUFFERING=3, RECOVERING=4, DOWN=5.
 * The fork collapses raw LEADER(1) to ACTIVE on {@code published=true} reads — so a promoted leader
 * is simultaneously {@code getRawState()==LEADER} and {@code isActive()==true} (AGENTS.md).
 *
 * <p>The ZK dispatch / migration-fallback path ({@code ZkStateReaderQueue.getAndProcessDeltaUpdates}
 * and the legacy {@code _statupdates} fallback) is covered ZK-backed in
 * {@link StatePlaneReaderQueueTest}.
 */
public class StatePlaneReaderTest extends SolrTestCaseJ4 {

  private static final int LEADER = 1;
  private static final int ACTIVE = 2;
  private static final int RECOVERING = 4;

  // ---- builders ----

  /** A single-shard {@link DocCollection} with replicas at the given internal ids + published states. */
  private static DocCollection collection(String name, String shard, int[] ids, Replica.State[] states) {
    Map<String, Replica> replicaMap = new LinkedHashMap<>();
    for (int i = 0; i < ids.length; i++) {
      Map<String, Object> props = new HashMap<>();
      props.put(ZkStateReader.NODE_NAME_PROP, "127.0.0.1:898" + i + "_solr");
      props.put(ZkStateReader.STATE_PROP, states[i].toString());
      props.put(ZkStateReader.REPLICA_TYPE, Replica.Type.NRT.name());
      props.put("id", ids[i]);
      String replicaName = shard + "_replica_n" + ids[i];
      Object2ObjectMap<String, Object> pm = new Object2ObjectLinkedOpenHashMap<>(props);
      replicaMap.put(replicaName, new Replica(replicaName, pm, name, -1, shard, null));
    }
    Slice slice = new Slice(shard, replicaMap, null, name, -1);
    Map<String, Slice> slices = new HashMap<>();
    slices.put(shard, slice);
    Map<String, Object> collProps = new HashMap<>();
    collProps.put("id", -1L);
    // DocCollection ctor runs setStates(): seeds the live StateUpdates map from each replica's
    // published state and links each replica to its AtomicInteger, so updateState(id, ..) mutates it.
    return new DocCollection(name, slices, collProps, CompositeIdRouter.DEFAULT);
  }

  private static StateDelta delta(int epoch, long seq, int promoteId, int promoteState) {
    return new StateDelta(epoch, seq,
        Collections.singletonList(new StateDelta.Entry(promoteId, promoteState)),
        Collections.emptyList(), ACTIVE);
  }

  private static ShardStateLog ring(int epoch, long baseSeq, long lastSeq, StateDelta... deltas) {
    return new ShardStateLog(epoch, baseSeq, lastSeq, "writer-e1", Arrays.asList(deltas));
  }

  private static Replica.State raw(DocCollection dc, int id) {
    return dc.getReplicaById(id).getRawState();
  }

  private static int rawLeaderCount(DocCollection dc, String shard) {
    int n = 0;
    for (Replica r : dc.getSlice(shard).getReplicas()) {
      if (r.getRawState() == Replica.State.LEADER) n++;
    }
    return n;
  }

  // ---- 1. apply only deltas strictly after the cursor ----

  @Test
  public void testApplyAfterPosition() {
    DocCollection dc = collection("c1", "s1", new int[] {101, 102},
        new Replica.State[] {Replica.State.DOWN, Replica.State.DOWN});
    StatePlaneCursors cursors = new StatePlaneCursors();
    cursors.advance("s1", 1, 1); // reader already applied up to (epoch=1, seq=1)

    // seq=1 promotes 102->ACTIVE (already covered by the cursor -> must be skipped),
    // seq=2 promotes 101->RECOVERING (new -> must be applied).
    ShardStateLog log = ring(1, 0, 2, delta(1, 1, 102, ACTIVE), delta(1, 2, 101, RECOVERING));

    assertFalse("same-epoch ring with baseSeq<=cursor applies incrementally",
        StatePlaneReader.needsSnapshotCatchup(log, cursors.get("s1")));

    boolean applied = StatePlaneReader.applyRing(dc, "s1", log, cursors);

    assertTrue("a newer delta was applied", applied);
    assertEquals("seq=1 was stale and skipped", Replica.State.DOWN, raw(dc, 102));
    assertEquals("seq=2 was applied", Replica.State.RECOVERING, raw(dc, 101));
    assertEquals("cursor advanced to the last applied seq", 2L, cursors.get("s1")[1]);
    assertEquals(1L, cursors.get("s1")[0]);
  }

  // ---- 2. duplicate / stale deltas are idempotent (no regression, no generation bump) ----

  @Test
  public void testDuplicateAndStaleSkipped() {
    DocCollection dc = collection("c2", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    StatePlaneCursors cursors = new StatePlaneCursors();
    cursors.advance("s1", 1, 0);

    ShardStateLog log = ring(1, 0, 1, delta(1, 1, 101, ACTIVE));

    assertTrue(StatePlaneReader.applyRing(dc, "s1", log, cursors));
    assertEquals(Replica.State.ACTIVE, raw(dc, 101));
    assertEquals(1L, cursors.get("s1")[1]);
    int genAfterFirst = cursors.getGeneration();

    // Re-firing the same ring (a re-delivered watch) applies nothing and does not bump generation.
    boolean appliedAgain = StatePlaneReader.applyRing(dc, "s1", log, cursors);
    assertFalse("re-applying an already-consumed ring is a no-op", appliedAgain);
    assertEquals("generation must not advance on a no-op", genAfterFirst, cursors.getGeneration());
    assertEquals("cursor unchanged", 1L, cursors.get("s1")[1]);
    assertEquals(Replica.State.ACTIVE, raw(dc, 101));
  }

  // ---- 3. snapshot catch-up + newer deltas reconstruct the effective state ----

  @Test
  public void testSnapshotPlusNewerDeltasReconstruct() {
    DocCollection dc = collection("c3", "s1", new int[] {101, 102},
        new Replica.State[] {Replica.State.DOWN, Replica.State.DOWN});

    // Snapshot at (epoch=2, baseSeq=5): both replicas ACTIVE.
    Map<Integer, Integer> base = new HashMap<>();
    base.put(101, ACTIVE);
    base.put(102, ACTIVE);
    StateSnapshot snapshot = new StateSnapshot(2, "s1", 5, base);

    // Ring at epoch=2 advancing past the snapshot: seq=6 promotes 102->LEADER, seq=7 moves 101->RECOVERING.
    ShardStateLog log = ring(2, 5, 7, delta(2, 6, 102, LEADER), delta(2, 7, 101, RECOVERING));

    StatePlaneCursors cursors = new StatePlaneCursors(); // fresh reader at (0,0)
    assertTrue("epoch jump forces snapshot catch-up",
        StatePlaneReader.needsSnapshotCatchup(log, cursors.get("s1")));

    StatePlaneReader.applySnapshotAndDeltas(dc, "s1", snapshot, log, cursors);

    assertEquals("102 promoted to raw LEADER via the ring", Replica.State.LEADER, raw(dc, 102));
    assertTrue("raw LEADER reads as active", dc.getReplicaById(102).isActive());
    assertEquals("101 moved to RECOVERING by the last delta", Replica.State.RECOVERING, raw(dc, 101));
    assertEquals("exactly one visible leader after reconstruct", 1, rawLeaderCount(dc, "s1"));
    assertEquals("cursor at the ring head (epoch, lastSeq)", 2L, cursors.get("s1")[0]);
    assertEquals(7L, cursors.get("s1")[1]);
  }

  // ---- 5. delta-plane state survives a structure refresh without being clobbered ----

  @Test
  public void testDeltaStateSurvivesStructureRefresh() {
    // prev: a collection whose live state has been driven forward by the delta plane.
    DocCollection prev = collection("c5", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    StatePlaneCursors cursors = prev.getOrCreateStatePlaneCursors();
    cursors.advance("s1", 1, 0);
    StatePlaneReader.applyRing(prev, "s1", ring(1, 0, 1, delta(1, 1, 101, LEADER)), cursors);
    assertEquals(Replica.State.LEADER, raw(prev, 101));
    assertTrue("delta-driven generation advanced past the fresh-reader default",
        prev.getStatePlaneGeneration() > 0);

    // newState: a fresh state.json structure refresh (published DOWN, no cursors) for the SAME
    // collection. Without the clobber guard this stale fetch would drop the newer LEADER state.
    DocCollection fresh = collection("c5", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    assertEquals(Replica.State.DOWN, raw(fresh, 101));
    assertEquals("fresh refresh has no delta-plane cursors", -1, fresh.getStatePlaneGeneration());

    StatePlaneReader.carryForwardStateUpdates(fresh, prev);

    assertEquals("newer delta-plane state carried forward onto the refresh (gate b)",
        Replica.State.LEADER, raw(fresh, 101));
    assertNotNull("cursors carried forward", fresh.getStatePlaneCursorsOrNull());
    assertEquals("generation (delta domain) carried forward",
        prev.getStatePlaneGeneration(), fresh.getStatePlaneGeneration());
  }

  /** No cursors (before any delta apply), equal versions: carry-forward is a no-op. */
  @Test
  public void testCarryForwardNoOpWithoutCursors() {
    DocCollection prev = collection("c5b", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    DocCollection fresh = collection("c5b", "s1", new int[] {101},
        new Replica.State[] {Replica.State.ACTIVE});

    assertNull("no delta cursors before any apply", prev.getStatePlaneCursorsOrNull());
    StatePlaneReader.carryForwardStateUpdates(fresh, prev);

    assertNull("still no cursors", fresh.getStatePlaneCursorsOrNull());
    assertEquals("fresh state untouched by a non-ahead prev", Replica.State.ACTIVE, raw(fresh, 101));
  }

  // ---- 6. a LEADER promotion yields exactly one visible leader in the slice ----

  @Test
  public void testLeaderPromotionYieldsSingleVisibleLeader() {
    DocCollection dc = collection("c6", "s1", new int[] {101, 102, 103},
        new Replica.State[] {Replica.State.LEADER, Replica.State.ACTIVE, Replica.State.ACTIVE});
    assertEquals("starts with 101 as the only leader", 1, rawLeaderCount(dc, "s1"));
    assertEquals(Integer.valueOf(101), dc.getSlice("s1").getLeader().getInternalId());

    StatePlaneCursors cursors = dc.getOrCreateStatePlaneCursors();
    cursors.advance("s1", 1, 0);
    // 102 is promoted to LEADER; the reader must demote the prior leader 101 to ACTIVE (D14).
    StatePlaneReader.applyRing(dc, "s1", ring(1, 0, 1, delta(1, 1, 102, LEADER)), cursors);

    assertEquals("the new leader carries raw LEADER", Replica.State.LEADER, raw(dc, 102));
    assertEquals("the prior leader was demoted to ACTIVE", Replica.State.ACTIVE, raw(dc, 101));
    assertEquals("untouched replica stays ACTIVE", Replica.State.ACTIVE, raw(dc, 103));
    assertEquals("exactly one visible leader per slice", 1, rawLeaderCount(dc, "s1"));

    Replica leader = dc.getSlice("s1").getLeader();
    assertEquals("Slice.getLeader() resolves the new leader", Integer.valueOf(102), leader.getInternalId());
    assertTrue("raw LEADER is active on published reads", leader.isActive());
    assertEquals(Replica.State.ACTIVE, leader.getState());
  }

  // ---- 7. cross-domain clobber guard: a high StateUpdates version must NOT regress delta state ----

  @Test
  public void testDeltaStateSurvivesHighStateUpdatesVersionRefresh() {
    // prev: delta-plane driven. Non-empty cursors, LOW generation (1), 101 promoted to LEADER.
    DocCollection prev = collection("c7", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    StatePlaneCursors cursors = prev.getOrCreateStatePlaneCursors();
    cursors.advance("s1", 1, 0);
    StatePlaneReader.applyRing(prev, "s1", ring(1, 0, 1, delta(1, 1, 101, LEADER)), cursors);
    assertEquals(Replica.State.LEADER, raw(prev, 101));
    assertTrue("prev's delta generation is small (a few applied batches), well below 47",
        prev.getStatePlaneGeneration() > 0 && prev.getStatePlaneGeneration() < 47);
    assertEquals("prev has no StateUpdates version set (delta-plane driven)",
        -1, prev.getStateUpdatesZkVersion());

    // fresh: a stale structure refresh with a HIGH StateUpdates version (47) but NO cursors.
    // Pre-fix this high int out-competed the small delta generation and clobbered LEADER->DOWN.
    DocCollection fresh = collection("c7", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    fresh.setStateUpdatesZkVersion(47);
    assertEquals("fresh carries a high StateUpdates version", 47, fresh.getStateUpdatesZkVersion());
    assertEquals("fresh has no delta-plane cursors", -1, fresh.getStatePlaneGeneration());
    assertEquals(Replica.State.DOWN, raw(fresh, 101));

    StatePlaneReader.carryForwardStateUpdates(fresh, prev);

    // Gate (a) StateUpdates-version domain: prev(-1) > fresh(47) is FALSE -> no carry.
    // Gate (b) delta domain:                prev(1)  > fresh(-1) is TRUE  -> adopt prev's StateUpdates + cursors.
    assertEquals("delta LEADER state survives the high-version refresh (no cross-domain clobber)",
        Replica.State.LEADER, raw(fresh, 101));
    assertNotNull("prev's delta cursors carried forward", fresh.getStatePlaneCursorsOrNull());
    assertEquals("prev's delta generation carried forward",
        prev.getStatePlaneGeneration(), fresh.getStatePlaneGeneration());
  }

  // ---- 8. snapshot reconstruct applies in-structure ids and observably skips an un-seeded id ----

  @Test
  public void testSnapshotReconstructSkipsUnseededId() {
    // Local structure only knows 101 and 102. The snapshot additionally references 999, which is NOT
    // in local state.json structure (un-seeded). updateState(999, ..) is a no-op; the reader must
    // apply the in-structure ids correctly and skip 999 observably (logged), without corrupting them.
    DocCollection dc = collection("c8", "s1", new int[] {101, 102},
        new Replica.State[] {Replica.State.DOWN, Replica.State.DOWN});
    assertNull("999 is absent from local structure", dc.getReplicaById(999));

    Map<Integer, Integer> base = new HashMap<>();
    base.put(101, ACTIVE);
    base.put(102, ACTIVE);
    base.put(999, ACTIVE); // present in the snapshot, absent from structure -> must be skipped
    StateSnapshot snapshot = new StateSnapshot(2, "s1", 5, base);

    // Ring advances past the snapshot: seq=6 promotes 102->LEADER, seq=7 moves 101->RECOVERING.
    ShardStateLog log = ring(2, 5, 7, delta(2, 6, 102, LEADER), delta(2, 7, 101, RECOVERING));

    StatePlaneCursors cursors = new StatePlaneCursors();
    assertTrue("epoch jump forces snapshot catch-up",
        StatePlaneReader.needsSnapshotCatchup(log, cursors.get("s1")));

    StatePlaneReader.applySnapshotAndDeltas(dc, "s1", snapshot, log, cursors);

    // In-structure ids reconstructed correctly.
    assertEquals("102 promoted to raw LEADER via the ring", Replica.State.LEADER, raw(dc, 102));
    assertEquals("101 moved to RECOVERING by the last delta", Replica.State.RECOVERING, raw(dc, 101));
    assertEquals("exactly one visible leader after reconstruct", 1, rawLeaderCount(dc, "s1"));
    // The un-seeded id never materialized a replica; the skip did not corrupt the applied state.
    assertNull("un-seeded 999 remains absent (observably skipped, not synthesized)",
        dc.getReplicaById(999));
    assertFalse("un-seeded 999 was not added to the live state map", dc.replicaIsInStateUpdates(999));
    // Cursor still advances to the ring head (catch-up limitation documented in production code).
    assertEquals("cursor at the ring head epoch", 2L, cursors.get("s1")[0]);
    assertEquals("cursor at the ring head seq", 7L, cursors.get("s1")[1]);
  }
}
