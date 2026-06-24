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
 * <p>The ZK dispatch path ({@code ZkStateReaderQueue.getAndProcessDeltaUpdates}) is covered ZK-backed
 * in {@link StatePlaneReaderQueueTest}.
 */
public class StatePlaneReaderTest extends SolrTestCaseJ4 {

  private static final int LEADER = 1;
  private static final int ACTIVE = 2;
  private static final int RECOVERING = 4;
  private static final String COLL_ID = "101";


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
    collProps.put("id", Integer.valueOf(COLL_ID));
    // DocCollection ctor runs setStates(): seeds the live StateUpdates map from each replica's
    // published state and links each replica to its AtomicInteger, so updateState(id, ..) mutates it.
    return new DocCollection(name, slices, collProps, CompositeIdRouter.DEFAULT);
  }

  private static StateDelta delta(int epoch, long seq, int promoteId, int promoteState) {
    return delta("s1", epoch, seq, promoteId, promoteState);
  }

  private static StateDelta delta(String shard, int epoch, long seq, int promoteId, int promoteState) {
    return new StateDelta(COLL_ID, shard, epoch, seq,
        Collections.singletonList(new StateDelta.Entry(promoteId, promoteState)),
        Collections.emptyList(), ACTIVE);
  }

  private static ShardStateLog ring(int epoch, long baseSeq, long lastSeq, StateDelta... deltas) {
    return ring("s1", epoch, baseSeq, lastSeq, deltas);
  }

  private static ShardStateLog ring(String shard, int epoch, long baseSeq, long lastSeq, StateDelta... deltas) {
    return new ShardStateLog(COLL_ID, shard, epoch, baseSeq, lastSeq, "writer-e1", Arrays.asList(deltas));
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
    StateSnapshot snapshot = new StateSnapshot(Long.parseLong(COLL_ID), "c3", 2, "s1", 5, base);

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

  // ---- 7. delta-plane clobber guard: a bare un-folded structure refresh must NOT regress delta state ----

  @Test
  public void testDeltaStateSurvivesUnfoldedStructureRefresh() {
    // prev: delta-plane driven (FOLDED). Non-empty cursors (generation >= 0), 101 promoted to LEADER.
    DocCollection prev = collection("c7", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    StatePlaneCursors cursors = prev.getOrCreateStatePlaneCursors();
    cursors.advance("s1", 1, 0);
    StatePlaneReader.applyRing(prev, "s1", ring(1, 0, 1, delta(1, 1, 101, LEADER)), cursors);
    assertEquals(Replica.State.LEADER, raw(prev, 101));
    assertTrue("prev folded the delta plane (generation >= 0)", prev.getStatePlaneGeneration() >= 0);

    // fresh: an UN-FOLDED bare structure refresh (published DOWN, no cursors) for the SAME collection.
    // Without the delta-plane clobber guard this stale fetch would drop the newer LEADER state.
    DocCollection fresh = collection("c7", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    assertEquals("fresh has no delta-plane cursors (un-folded)", -1, fresh.getStatePlaneGeneration());
    assertEquals(Replica.State.DOWN, raw(fresh, 101));

    StatePlaneReader.carryForwardStateUpdates(fresh, prev);

    // Gate (b) delta domain: prev folded (gen >= 0), fresh un-folded (gen < 0) -> adopt prev's
    // StateUpdates + cursors so the bare refresh cannot regress the freshly-folded LEADER.
    assertEquals("delta LEADER state survives the un-folded structure refresh",
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
    StateSnapshot snapshot = new StateSnapshot(Long.parseLong(COLL_ID), "c8", 2, "s1", 5, base);

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

  // ---- 9. a folded structure refresh must NOT be clobbered by a staler prev whose per-instance
  //         generation merely happens to be higher (low-activity shard leader survives catch-up) ----

  @Test
  public void testFoldedRefreshNotClobberedByStalerPrevGeneration() {
    // Reproduces the LeaderElectionIntegrationTest low-activity-shard hang. A single-replica shard
    // (s2, id=2) publishes exactly ONE delta (2 -> LEADER) while id=2 is still un-seeded in the
    // reader's state.json structure. The previously-watched collection (prev) folds that ring while
    // id=2 is absent, so prev SKIPS id=2 (its live map never gains the key) yet its per-instance
    // generation keeps climbing from a high-activity shard (s1). When structure finally seeds id=2, a
    // FRESH full-fetch folds 2 -> LEADER correctly and is authoritative. Pre-fix, carry-forward
    // wholesale-overwrote that freshly-folded map with prev's (which lacks id=2) purely because prev's
    // generation was numerically higher, regressing the leader to the DOWN baseline forever.
    final int S2_LEADER_ID = 2;
    StateSnapshot s2snap =
        new StateSnapshot(Long.parseLong(COLL_ID), "collection1", 1, "s2", 0, Collections.singletonMap(S2_LEADER_ID, LEADER));
    ShardStateLog s2ring = ring("s2", 1, 0, 1, delta("s2", 1, 1, S2_LEADER_ID, LEADER));

    // prev: structure does NOT yet contain id=2 (un-seeded) -> folding s2's ring skips id=2.
    DocCollection prev = collection("collection1", "s2", new int[] {}, new Replica.State[] {});
    StatePlaneCursors prevCursors = prev.getOrCreateStatePlaneCursors();
    StatePlaneReader.applySnapshotAndDeltas(prev, "s2", s2snap, s2ring, prevCursors);
    assertNull("prev never seeded id=2, so its live map has no entry for it",
        prev.getReplicaById(S2_LEADER_ID));
    // A high-activity shard drives prev's per-instance generation well above a fresh single-shard fold.
    prevCursors.advance("s1", 1, 5);
    prevCursors.advance("s1", 1, 9);
    prevCursors.advance("s1", 1, 14);

    // freshV9: state.json has caught up and seeded id=2 (DOWN baseline). A full-fetch folds s2's ring,
    // correctly promoting id=2 -> raw LEADER. This freshly-folded collection is authoritative.
    DocCollection freshV9 = collection("collection1", "s2", new int[] {S2_LEADER_ID},
        new Replica.State[] {Replica.State.DOWN});
    StatePlaneCursors freshCursors = freshV9.getOrCreateStatePlaneCursors();
    StatePlaneReader.applySnapshotAndDeltas(freshV9, "s2", s2snap, s2ring, freshCursors);
    assertEquals("fresh full-fetch folds the only delta -> raw LEADER",
        Replica.State.LEADER, raw(freshV9, S2_LEADER_ID));
    assertTrue("fresh fold initialized cursors (generation >= 0)",
        freshV9.getStatePlaneGeneration() >= 0);
    assertTrue("prev's per-instance generation outruns the fresh fold's",
        prev.getStatePlaneGeneration() > freshV9.getStatePlaneGeneration());

    // Carry-forward must NOT regress the freshly-folded authoritative leader just because prev's
    // per-instance generation is higher: a folded newState already read the CURRENT ZK rings.
    StatePlaneReader.carryForwardStateUpdates(freshV9, prev);

    assertEquals("the low-activity shard's leader survives the structure catch-up (no clobber)",
        Replica.State.LEADER, raw(freshV9, S2_LEADER_ID));
    assertTrue("leader still reads active on published reads",
        freshV9.getReplicaById(S2_LEADER_ID).isActive());
    assertEquals("exactly one visible leader in s2", 1, rawLeaderCount(freshV9, "s2"));
  }

  // ---- 10. updateWatchedCollection replace-gate: a freshly-folded state at EQUAL structure version
  //          must replace the stale watched fold (LeaderElectionIntegrationTest / Beast_068 leader loss).
  //          A fresher fold carrying a just-published LEADER must not be discarded as a same-version
  //          duplicate; when the shard then went quiet, no leader would ever be seen otherwise.

  @Test
  public void testShouldReplaceWatchedFoldedStateAtEqualStructureVersion() {
    final int LEADER_ID = 4;

    // current watched (v): stale folded state at structure v=9, generation >= 0, replica 4 ACTIVE.
    DocCollection current = collection("c10", "s1", new int[] {LEADER_ID},
        new Replica.State[] {Replica.State.ACTIVE});
    current.setZnodeVersion(9);
    current.getOrCreateStatePlaneCursors().advance("s1", 1, 3);
    assertTrue("current folded the plane", current.getStatePlaneGeneration() >= 0);

    // incoming: a fresh full-fetch fold at the SAME structure version (v=9) that promoted 4 -> LEADER.
    DocCollection incoming = collection("c10", "s1", new int[] {LEADER_ID},
        new Replica.State[] {Replica.State.DOWN});
    incoming.setZnodeVersion(9);
    StatePlaneCursors inCursors = incoming.getOrCreateStatePlaneCursors();
    StatePlaneReader.applyRing(incoming, "s1", ring(1, 0, 1, delta(1, 1, LEADER_ID, LEADER)), inCursors);
    assertEquals("incoming folded 4 -> raw LEADER", Replica.State.LEADER, raw(incoming, LEADER_ID));
    assertTrue("incoming folded the plane", incoming.getStatePlaneGeneration() >= 0);

    assertTrue("a freshly-folded same-structure-version state must replace the stale watched fold",
        StatePlaneReader.shouldReplaceWatched(incoming, current));

    // An UN-folded bare structure refresh (no cursors) at the same version must NOT replace folded
    // state — carryForwardStateUpdates handles that case, the gate must not let the baseline overwrite.
    DocCollection bareRefresh = collection("c10", "s1", new int[] {LEADER_ID},
        new Replica.State[] {Replica.State.DOWN});
    bareRefresh.setZnodeVersion(9);
    assertEquals("bare refresh is un-folded", -1, bareRefresh.getStatePlaneGeneration());
    assertFalse("an un-folded baseline refresh at equal version must not replace folded state",
        StatePlaneReader.shouldReplaceWatched(bareRefresh, current));

    // Structure version strictly newer always replaces; strictly older never does (even if folded).
    DocCollection newerStructure = collection("c10", "s1", new int[] {LEADER_ID},
        new Replica.State[] {Replica.State.DOWN});
    newerStructure.setZnodeVersion(10);
    assertTrue("a strictly newer structure version always replaces",
        StatePlaneReader.shouldReplaceWatched(newerStructure, current));

    DocCollection olderStructure = collection("c10", "s1", new int[] {LEADER_ID},
        new Replica.State[] {Replica.State.DOWN});
    olderStructure.setZnodeVersion(8);
    olderStructure.getOrCreateStatePlaneCursors().advance("s1", 1, 99);
    assertFalse("a strictly older structure version never replaces, even when folded",
        StatePlaneReader.shouldReplaceWatched(olderStructure, current));
  }

  // ---- 11. finding #3: a transition for an un-seeded id is BUFFERED (not dropped past the advancing
  //          cursor) and REPLAYED once the structure catches up and seeds that id. ----

  @Test
  public void testDeferredTransitionReplayedAfterStructureSeedsId() {
    // The advancing per-shard cursor must not permanently strand a transition for a replica the local
    // structure has not observed yet. The reader buffers it in the shared cursors and replays it on the
    // next apply once the id is seeded.
    StatePlaneCursors cursors = new StatePlaneCursors();

    // dc1: structure only knows 101; 102 is not yet seeded. A delta promotes the un-seeded 102 -> LEADER.
    DocCollection dc1 = collection("c11", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    boolean applied = StatePlaneReader.applyRing(dc1, "s1",
        ring(1, 0, 1, delta(1, 1, 102, LEADER)), cursors);
    assertTrue("the ring delta advanced the cursor", applied);
    assertNull("102 is absent from dc1's structure, so it was not applied there", dc1.getReplicaById(102));
    assertTrue("the un-seeded transition was buffered, not dropped", cursors.hasDeferred("s1"));
    assertEquals("cursor still advanced past the buffered delta", 1L, cursors.get("s1")[1]);

    // dc2: a structure refresh that has caught up and seeded 102 (DOWN baseline). It carries the SAME
    // cursors forward (as carryForwardStateUpdates/adoptStatePlaneCursors do in production). The next
    // apply — even one whose ring deltas are all stale — flushes the buffered 102 -> LEADER onto dc2.
    DocCollection dc2 = collection("c11", "s1", new int[] {101, 102},
        new Replica.State[] {Replica.State.DOWN, Replica.State.DOWN});
    boolean appliedAgain = StatePlaneReader.applyRing(dc2, "s1",
        ring(1, 0, 1, delta(1, 1, 102, LEADER)), cursors);
    assertFalse("the ring deltas are all stale (cursor already at seq=1)", appliedAgain);

    assertEquals("the buffered transition was replayed once 102 was seeded",
        Replica.State.LEADER, raw(dc2, 102));
    assertEquals("exactly one visible leader after the replay", 1, rawLeaderCount(dc2, "s1"));
    assertFalse("the buffer drained once the transition was replayed", cursors.hasDeferred("s1"));
  }

  // ---- 12. finding #3 safety: a stale buffered LEADER must NOT resurrect after a later handoff. The
  //          buffer holds at most one LEADER per shard, so replay yields the LATEST leader only. ----

  @Test
  public void testBufferedStaleLeaderDoesNotResurrectAfterHandoff() {
    StatePlaneCursors cursors = new StatePlaneCursors();

    // dc1: structure only knows 101; 102 and 103 are un-seeded. Two deltas hand leadership 102 -> 103
    // while both are un-seeded, so both promotions are buffered. The buffer must demote the earlier
    // buffered leader (102) to ACTIVE when the later one (103) is buffered.
    DocCollection dc1 = collection("c12", "s1", new int[] {101},
        new Replica.State[] {Replica.State.DOWN});
    StatePlaneReader.applyRing(dc1, "s1",
        ring(1, 0, 2, delta(1, 1, 102, LEADER), delta(1, 2, 103, LEADER)), cursors);
    assertTrue("both un-seeded promotions were buffered", cursors.hasDeferred("s1"));

    // dc2: structure caught up and seeded both 102 and 103 (DOWN). Flushing the buffer must yield ONLY
    // 103 as leader — the superseded 102 must not be replayed as a (stale) second leader.
    DocCollection dc2 = collection("c12", "s1", new int[] {101, 102, 103},
        new Replica.State[] {Replica.State.DOWN, Replica.State.DOWN, Replica.State.DOWN});
    StatePlaneReader.applyRing(dc2, "s1",
        ring(1, 0, 2, delta(1, 1, 102, LEADER), delta(1, 2, 103, LEADER)), cursors);

    assertEquals("the latest leader (103) was replayed as LEADER", Replica.State.LEADER, raw(dc2, 103));
    assertEquals("the superseded leader (102) replayed as ACTIVE, not a resurrected leader",
        Replica.State.ACTIVE, raw(dc2, 102));
    assertEquals("exactly one visible leader survives the handoff replay", 1, rawLeaderCount(dc2, "s1"));
    assertFalse("the buffer drained after the replay", cursors.hasDeferred("s1"));
  }



    private static void assertFailsClosed(Runnable action) {
        try {
            action.run();
            fail("state-plane identity mismatch should fail closed");
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    public void testRingCollectionIdentityMismatchFailsClosed() {
        DocCollection dc = collection(
            "collection1", "s1", new int[] {10}, new Replica.State[] {Replica.State.ACTIVE});
        ShardStateLog badRing = new ShardStateLog(
            "202", "s1", 1, 0L, 1L, "writer-e1",
            Collections.singletonList(delta("s1", 1, 1, 10, ACTIVE)));

        assertFailsClosed(
            () -> StatePlaneReader.applyRing(dc, "s1", badRing, new StatePlaneCursors()));
    }

    public void testRingShardIdentityMismatchFailsClosed() {
        DocCollection dc = collection(
            "collection1", "s1", new int[] {10}, new Replica.State[] {Replica.State.ACTIVE});
        ShardStateLog badRing = new ShardStateLog(
            COLL_ID, "s2", 1, 0L, 1L, "writer-e1",
            Collections.singletonList(delta("s2", 1, 1, 10, ACTIVE)));

        assertFailsClosed(
            () -> StatePlaneReader.applyRing(dc, "s1", badRing, new StatePlaneCursors()));
    }

    public void testDeltaIdentityMismatchFailsClosed() {
        DocCollection dc = collection(
            "collection1", "s1", new int[] {10}, new Replica.State[] {Replica.State.ACTIVE});
        StateDelta wrongShardDelta = new StateDelta(
            COLL_ID, "s2", 1, 1L, Collections.singletonList(new StateDelta.Entry(10, ACTIVE)), Collections.emptyList(), ACTIVE);
        ShardStateLog ring = new ShardStateLog(
            COLL_ID, "s1", 1, 0L, 1L, "writer-e1", Collections.singletonList(wrongShardDelta));

        assertFailsClosed(
            () -> StatePlaneReader.applyRing(dc, "s1", ring, new StatePlaneCursors()));
    }

    public void testSnapshotIdentityMismatchFailsClosed() {
        DocCollection dc = collection(
            "collection1", "s1", new int[] {10}, new Replica.State[] {Replica.State.ACTIVE});
        ShardStateLog ring = ring("s1", 1, 0L, 0L);
        StateSnapshot wrongCollectionSnapshot = new StateSnapshot(
            Long.parseLong(COLL_ID), "otherCollection", 1, "s1", 0L, Collections.emptyMap());

        assertFailsClosed(
            () -> StatePlaneReader.applySnapshotAndDeltas(
                dc, "s1", wrongCollectionSnapshot, ring, new StatePlaneCursors()));
    }

}
