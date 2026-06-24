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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PR-3 READER apply logic for the StateUpdates delta plane. Pure (no ZK I/O): it consumes already
 * decoded Phase-1 {@link ShardStateLog} / {@link StateSnapshot} objects and folds them onto a
 * {@link DocCollection}'s live {@link StateUpdates} map through {@link DocCollection#updateState}.
 *
 * <p>Invariants (mirroring the writer, D2/D5/D14):
 * <ul>
 *   <li>Deltas apply in {@code (epoch, seq)} order; higher epoch wins, then higher seq.
 *   <li>A delta at or below the per-shard cursor is skipped (idempotent — a re-fired watch or an
 *       older structure refresh cannot regress applied state).
 *   <li>Demotions are applied before promotions within a delta.
 *   <li>A LEADER promotion (raw shortState 1) demotes any OTHER raw-LEADER replica in the same slice
 *       to ACTIVE(2) (D14), so the reader exposes exactly one visible leader per slice.
 * </ul>
 *
 * <p>The writer-side {@code writerId}/election fence (D4) is NEVER validated here — readers trust
 * {@code (epoch, seq)} ordering only.
 */
public final class StatePlaneReader {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** LEADER raw short state on the wire; collapses to ACTIVE on read via {@code published=true}. */
  public static final int LEADER = 1;
  /** ACTIVE short state — a deposed leader becomes ACTIVE (D14). */
  public static final int ACTIVE = 2;

  private StatePlaneReader() {}

  /**
   * True when the reader is too far behind to apply {@code ring} incrementally and must catch up from
   * the per-shard snapshot first: either the epoch changed (takeover/recreation, or a fresh reader vs
   * a seeded ring) or the ring no longer contains the next seq after the cursor ({@code baseSeq} has
   * advanced past {@code cursor+1}, i.e. {@code baseSeq > cursorSeq}).
   */
  public static boolean needsSnapshotCatchup(ShardStateLog ring, long[] cursor) {
    int cursorEpoch = (int) cursor[0];
    long cursorSeq = cursor[1];
    if (ring.epoch != cursorEpoch) {
      return true;
    }
    return ring.baseSeq > cursorSeq;
  }

  /**
   * Bounded catch-up decision (PR-5, D5). Forces a snapshot catch-up in the base cases
   * ({@link #needsSnapshotCatchup(ShardStateLog, long[])}) AND when the reader is more than
   * {@code readerCatchupLimit} seqs behind the ring head within the same epoch — folding that many
   * incremental deltas would be wasteful, so rebase from the snapshot instead. A
   * {@code readerCatchupLimit <= 0} disables the distance bound (always incremental when in-epoch).
   */
  public static boolean needsSnapshotCatchup(ShardStateLog ring, long[] cursor, int readerCatchupLimit) {
    if (needsSnapshotCatchup(ring, cursor)) {
      return true;
    }
    if (readerCatchupLimit > 0) {
      int cursorEpoch = (int) cursor[0];
      long cursorSeq = cursor[1];
      if (ring.epoch == cursorEpoch && ring.lastSeq - cursorSeq > readerCatchupLimit) {
        return true;
      }
    }
    return false;
  }

  /**
   * Incremental apply: fold every delta in {@code ring} with {@code (epoch, seq)} strictly after the
   * shard cursor onto {@code dc}, then advance the cursor to the last applied position. Returns true
   * if any delta was applied.
   */
  public static boolean applyRing(DocCollection dc, String shard, ShardStateLog ring,
                                  StatePlaneCursors cursors) {
    ring.validateIdentity(dc, shard);
    long[] cur = cursors.get(shard);
    int curEpoch = (int) cur[0];
    long curSeq = cur[1];

    // finding #3: before applying new deltas, replay any previously-skipped transition whose replica id
    // has since been seeded into local structure, so the advancing cursor never permanently strands it.
    flushDeferred(dc, shard, cursors);

    List<StateDelta> sorted = new ArrayList<>(ring.entries);
    Collections.sort(sorted);

    boolean applied = false;
    for (StateDelta d : sorted) {
      d.validateIdentity(dc, shard);
      if (d.isStale(curEpoch, curSeq)) {
        continue;
      }
      if (log.isTraceEnabled()) {
        log.trace("applyRing shard={} applying delta (epoch={}, seq={}) entries={} demoted={}->{}",
            shard, d.epoch, d.seq, d.entries, d.demotedReplicaIds, d.demotedShortState);
      }
      applyDelta(dc, d, shard, cursors);
      curEpoch = d.epoch;
      curSeq = d.seq;
      applied = true;
    }
    if (applied) {
      cursors.advance(shard, curEpoch, curSeq);
      if (log.isDebugEnabled()) {
        log.debug("applyRing shard={} advanced cursor to (epoch={}, seq={}); ringHead=(epoch={}, baseSeq={}, lastSeq={})",
            shard, curEpoch, curSeq, ring.epoch, ring.baseSeq, ring.lastSeq);
      }
    }
    return applied;
  }

  /**
   * Snapshot catch-up: reconstruct the shard's effective {@code replicaId -> shortState} from the
   * {@code snapshot} base plus the deltas still present in {@code ring} ({@link StateSnapshot#reconstruct}),
   * apply it to {@code dc}, and set the cursor to the snapshot+deltas head. A missing snapshot is
   * treated as an empty base at the ring's {@code (epoch, baseSeq)}.
   */
  public static void applySnapshotAndDeltas(DocCollection dc, String shard, StateSnapshot snapshot,
                                            ShardStateLog ring, StatePlaneCursors cursors) {
    // finding #3: replay resolvable previously-skipped transitions before rebasing from the snapshot.
    flushDeferred(dc, shard, cursors);

    ring.validateIdentity(dc, shard);
    if (snapshot != null) {
      snapshot.validateIdentity(dc, shard);
    }
    for (StateDelta d : ring.entries) {
      d.validateIdentity(dc, shard);
    }

    StateSnapshot base = snapshot != null ? snapshot
        : new StateSnapshot(Long.parseLong(ring.collectionId), dc.getName(), ring.epoch, shard,
            ring.baseSeq, Collections.emptyMap());
    Map<Integer, Integer> effective = base.reconstruct(ring.entries);

    if (log.isDebugEnabled()) {
      log.debug("applySnapshotAndDeltas shard={} snapshotPresent={} ring=(epoch={}, baseSeq={}, lastSeq={}) effective={}",
          shard, snapshot != null, ring.epoch, ring.baseSeq, ring.lastSeq, effective);
    }

    // Apply the reconstructed effective state. reconstruct() already folds demotions-before-promotions
    // in (epoch, seq) order, so it yields a single leader; we additionally enforce single-leader
    // defensively below in case a snapshot was hand-seeded with two raw leaders.
    //
    // DocCollection.updateState(id, ..) is a no-op for an id absent from the live StateUpdates map (the
    // map is seeded in setStates() only from replicas present in the current state.json structure). A
    // reconstructed id that is not yet in local structure is buffered by applyOrDefer (finding #3) rather
    // than dropped: the cursor still advances to the ring head, but the transition is replayed by
    // flushDeferred once structure catches up and seeds the id — so a far-behind catch-up cannot strand
    // a leader/active transition for a replica the structure has not yet observed.
    for (Map.Entry<Integer, Integer> e : effective.entrySet()) {
      applyOrDefer(dc, shard, cursors, e.getKey(), e.getValue());
    }
    enforceSingleLeaderPerSlice(dc, shard, effective);

    int headEpoch = ring.epoch;
    long headSeq = Math.max(ring.lastSeq, base.baseSeq);
    cursors.advance(shard, headEpoch, headSeq);
    if (log.isDebugEnabled()) {
      log.debug("applySnapshotAndDeltas shard={} advanced cursor to (epoch={}, seq={})", shard, headEpoch, headSeq);
    }
  }

  /**
   * Replace-gate decision for {@link ZkStateReader#updateWatchedCollection}: should the just-fetched
   * {@code incoming} collection replace the currently-watched {@code current}?
   *
   * <p>Structure version ({@code state.json} {@code znodeVersion}) is the primary key: a strictly
   * newer structure always wins, a strictly older one never does. The subtle case is EQUAL structure
   * version, where the only thing that changed is live replica state on the delta plane:
   * <ul>
   *   <li>A {@code incoming} that folded the delta plane ({@link DocCollection#getStatePlaneGeneration()}
   *       {@code >= 0}) read the CURRENT per-shard rings on a full fetch and is authoritative — it must
   *       win. A fresher fold carrying a just-published {@code LEADER} must not be discarded; if it were,
   *       a shard that went quiet right after the promotion would show no leader until the next structural
   *       change (LeaderElectionIntegrationTest single-replica-shard leader loss under load).
   *   <li>An UN-folded ({@code generation < 0}) bare structure refresh carries only the {@code DOWN}
   *       baseline; it must NOT replace folded state. {@link #carryForwardStateUpdates} separately
   *       adopts the previous live state for that case.
   * </ul>
   */
  public static boolean shouldReplaceWatched(DocCollection incoming, DocCollection current) {
    if (current == null) {
      return true;
    }
    int inV = incoming.getZNodeVersion();
    int curV = current.getZNodeVersion();
    if (inV != curV) {
      return inV > curV;
    }
    // Equal structure version: a freshly-folded delta plane is authoritative (read the current rings).
    if (incoming.getStatePlaneGeneration() >= 0) {
      return true;
    }
    // Un-folded refresh: do not replace the currently-watched (folded) state.
    return false;
  }

  /**
   * Carry-forward for the {@code updateWatchedCollection} clobber guard: when {@code prev} (the
   * previously watched collection) is ahead of {@code newState} (a just-fetched structure refresh),
   * adopt {@code prev}'s live {@link StateUpdates} map (and, on the delta plane, its per-shard cursors)
   * so an older {@code state.json} structure read cannot regress newer state.
   *
   * <p>The delta-plane gate compares the per-shard cursor generation
   * ({@link DocCollection#getStatePlaneGeneration()}). When {@code prev}'s cursors are ahead it adopts
   * {@code prev}'s StateUpdates + cursors, so an older structure fetch carrying no cursors cannot
   * regress newer delta state.
   */
  public static void carryForwardStateUpdates(DocCollection newState, DocCollection prev) {
    if (newState == null || prev == null) {
      return;
    }
    // Delta-plane carry-forward — adopt prev's already-folded StateUpdates + per-shard cursors so
    //     an UN-folded structure refresh cannot regress newer delta state.
    //
    //     Gated on {@code newState.getStatePlaneGeneration() < 0}: this fires ONLY when newState has no
    //     per-shard cursors of its own, i.e. it has NOT folded the delta plane (a bare state.json
    //     structure refresh). Such a refresh carries only the DOWN baseline and genuinely needs prev's
    //     live state.
    //
    //     A newState that DID fold the plane (generation >= 0) read the CURRENT ZK rings and is
    //     authoritative — it must keep its own freshly-applied state. The previous gate compared the
    //     two per-INSTANCE generations ({@code prev.gen > newState.gen}); but generation is a monotone
    //     count that a long-lived prev accumulates from high-activity shards far above a freshly-fetched
    //     newState's. That let a staler prev (whose own map never contained a low-activity shard's
    //     leader, because it skipped that id while it was un-seeded) clobber newState's correctly-folded
    //     leader back to the state.json baseline — the low-activity shard then stayed DOWN forever
    //     (LeaderElectionIntegrationTest single-replica-shard register timeout under load).
    //
    //     Same-incarnation only. A collection deleted and recreated with the same name gets a brand-new
    //     delta ring whose seq restarts low under the SAME (unchanged) node epoch; adopting the prior
    //     incarnation's far-ahead cursors would make applyRing silently skip every delta of the new ring
    //     (curSeq <= carried cursor), so leader/active never propagate (TestTlogReplica create-and-wait
    //     timeout on every iteration after the first).
    if (sameIncarnation(newState, prev)
        && newState.getStatePlaneGeneration() < 0
        && prev.getStatePlaneGeneration() >= 0) {
      newState.setStateUpdates(deepCopy(prev.getStateUpdates()));
      newState.adoptStatePlaneCursors(prev);
    }
  }

  /**
   * Deep-copy a {@link StateUpdates} map: fresh map + a NEW {@link AtomicInteger} per entry (value
   * preserved). Carry-forward MUST NOT alias {@code prev}'s map or its
   * AtomicInteger values — {@link DocCollection#updateState} mutates entries in place
   * ({@code sateForReplica.set(state)}), so a shared instance would let an {@code updateState} on the
   * new generation bleed into {@code prev} (still referenced by other in-flight ClusterState
   * snapshots). A fresh AtomicInteger per entry isolates the generations while preserving the
   * value-aware {@code hashCode()} freshness token (equal values → equal hash).
   */
  private static StateUpdates<Integer, AtomicInteger> deepCopy(StateUpdates<Integer, AtomicInteger> src) {
    StateUpdates<Integer, AtomicInteger> copy = new StateUpdates<>();
    if (src != null) {
      src.forEach((k, v) -> copy.put((Integer) k, new AtomicInteger(((AtomicInteger) v).get())));
    }
    return copy;
  }

  /**
   * True when {@code a} and {@code b} are the same collection incarnation (equal {@link
   * DocCollection#getId()}). When either id is unknown (null) we conservatively treat them as the same
   * incarnation so carry-forward behavior is unchanged for callers that do not assign collection ids.
   */
  private static boolean sameIncarnation(DocCollection a, DocCollection b) {
    Integer ia = a.getId();
    Integer ib = b.getId();
    return ia == null || ib == null || ia.equals(ib);
  }

  // ---- internals ----

  private static void applyDelta(DocCollection dc, StateDelta d, String shard, StatePlaneCursors cursors) {
    // Demotions first (deposed leaders / node-down), then promotions (ordering invariant).
    for (Integer demotedId : d.demotedReplicaIds) {
      applyOrDefer(dc, shard, cursors, demotedId, d.demotedShortState);
    }
    for (StateDelta.Entry e : d.entries) {
      applyOrDefer(dc, shard, cursors, e.replicaId, e.shortState);
    }
  }

  /**
   * Apply a single {@code replicaId -> shortState} transition to {@code dc}, or buffer it for deferred
   * replay when the id is not yet present in local state.json structure ({@link DocCollection#updateState}
   * is a silent no-op for an absent id). Buffering (finding #3) prevents the advancing per-shard cursor
   * from permanently stranding a transition for a replica the structure has not caught up to yet — it is
   * replayed by {@link #flushDeferred} once the id is seeded. The buffer holds at most one LEADER per
   * shard: a newer LEADER demotes any earlier buffered (or live) LEADER, so a structure catch-up cannot
   * resurrect a stale leader.
   */
  private static void applyOrDefer(DocCollection dc, String shard, StatePlaneCursors cursors,
                                   int replicaId, int shortState) {
    if (dc.getReplicaById(replicaId) == null) {
      if (shortState == LEADER) {
        demoteDeferredLeadersExcept(cursors, shard, replicaId);
      }
      cursors.defer(shard, replicaId, shortState);
      return;
    }
    cursors.undefer(shard, replicaId); // resolved; drop any now-stale buffered value
    if (shortState == LEADER) {
      demoteOtherLeaders(dc, replicaId);
      demoteDeferredLeadersExcept(cursors, shard, replicaId);
    }
    dc.updateState(replicaId, shortState);
  }

  /**
   * Replay buffered transitions (finding #3) whose replica id is now present in local structure. Each is
   * an independent per-replica state set and the in-buffer single-LEADER invariant guarantees at most one
   * buffered LEADER per shard, so replay order is immaterial. Applied entries are dropped from the buffer;
   * still-un-seeded entries are left for a later flush.
   */
  private static void flushDeferred(DocCollection dc, String shard, StatePlaneCursors cursors) {
    if (!cursors.hasDeferred(shard)) {
      return;
    }
    for (Map.Entry<Integer, Integer> e : cursors.deferredSnapshot(shard).entrySet()) {
      int replicaId = e.getKey();
      if (dc.getReplicaById(replicaId) == null) {
        continue; // still un-seeded — keep buffered
      }
      int shortState = e.getValue();
      if (shortState == LEADER) {
        demoteOtherLeaders(dc, replicaId);
        demoteDeferredLeadersExcept(cursors, shard, replicaId);
      }
      dc.updateState(replicaId, shortState);
      cursors.undefer(shard, replicaId);
      if (log.isDebugEnabled()) {
        log.debug("flushDeferred shard={} replayed buffered transition replicaId={} state={}",
            shard, replicaId, shortState);
      }
    }
  }

  /** Downgrade any buffered LEADER in {@code shard} other than {@code exceptId} to ACTIVE. */
  private static void demoteDeferredLeadersExcept(StatePlaneCursors cursors, String shard, int exceptId) {
    for (Map.Entry<Integer, Integer> e : cursors.deferredSnapshot(shard).entrySet()) {
      if (e.getKey() != exceptId && e.getValue() == LEADER) {
        cursors.defer(shard, e.getKey(), ACTIVE);
      }
    }
  }

  /** Read-side single-leader enforcement: demote any other current raw-LEADER replica in the slice. */
  private static void demoteOtherLeaders(DocCollection dc, int promotedId) {
    Replica promoted = dc.getReplicaById(promotedId);
    if (promoted == null) {
      return;
    }
    String sliceName = promoted.getSlice();
    if (sliceName == null) {
      return;
    }
    Slice slice = dc.getSlice(sliceName);
    if (slice == null) {
      return;
    }
    for (Replica r : slice.getReplicas()) {
      Integer rid = r.getInternalId();
      if (rid != null && rid != promotedId && r.getRawState() == Replica.State.LEADER) {
        dc.updateState(rid, ACTIVE);
      }
    }
  }

  /** Defensive: if a reconstructed snapshot left two raw leaders in a slice, keep only the highest id. */
  private static void enforceSingleLeaderPerSlice(DocCollection dc, String shard,
                                                  Map<Integer, Integer> effective) {
    Slice slice = dc.getSlice(shard);
    if (slice == null) {
      return;
    }
    Integer keep = null;
    for (Replica r : slice.getReplicas()) {
      Integer rid = r.getInternalId();
      if (rid != null && r.getRawState() == Replica.State.LEADER) {
        if (keep == null || rid > keep) {
          keep = rid;
        }
      }
    }
    if (keep == null) {
      return;
    }
    for (Replica r : slice.getReplicas()) {
      Integer rid = r.getInternalId();
      if (rid != null && !rid.equals(keep) && r.getRawState() == Replica.State.LEADER) {
        dc.updateState(rid, ACTIVE);
      }
    }
  }
}
