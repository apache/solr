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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.ShardStateLog;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StateDeltaCodec;
import org.apache.solr.common.cloud.StateDeltaConfig;
import org.apache.solr.common.cloud.StatePlanePaths;
import org.apache.solr.common.cloud.StatePlaneReader;
import org.apache.solr.common.cloud.StatePlaneWriter;
import org.apache.solr.common.cloud.StateSnapshot;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * PR-5 compaction tests for {@link StatePlaneWriter}: real snapshot fold (replacing the blind trim),
 * snapshot-durable-before-delta-delete ordering, crash/interrupt idempotency, and the
 * compaction-vs-new-delta race. ZK-backed via {@link ZkTestServer}.
 *
 * <p>Short states: LEADER=1, ACTIVE=2, BUFFERING=3, RECOVERING=4, DOWN=5.
 */
public class StatePlaneCompactionTest extends SolrTestCaseJ4 {
  private static final String COLL_ID = "101";

    private static final int LEADER = 1;
    private static final int ACTIVE = 2;
    private static final int BUFFERING = 3;
    private static final int RECOVERING = 4;
    private static final int DOWN = 5;

    /** Small compaction trigger so a handful of publishes folds. */
    private static final int COMPACT_AFTER = 5;

    private ZkTestServer server;
    private SolrZkClient zkClient;
    private String savedCompactAfter;

    static class AlwaysElectedFence implements StatePlaneWriter.ElectionFence {
        final String id;
        AlwaysElectedFence(String id) { this.id = id; }
        public boolean stillElected() { return true; }
        public String writerId() { return id; }
        public boolean isFencedBy(String ringWriterId) { return false; }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        savedCompactAfter = System.getProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP);
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, Integer.toString(COMPACT_AFTER));
        Path zkDir = SolrTestUtil.createTempDir("zkData-spc");
        server = new ZkTestServer(zkDir);
        server.run(true);
        zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
        zkClient.start();
    }

    @After
    public void tearDown() throws Exception {
        if (zkClient != null) zkClient.close();
        if (server != null) server.shutdown();
        if (savedCompactAfter == null) {
            System.clearProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP);
        } else {
            System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, savedCompactAfter);
        }
        super.tearDown();
    }

    private StatePlaneWriter writer(String id) {
        return new StatePlaneWriter(zkClient, new AlwaysElectedFence(id));
    }

    private ShardStateLog readRing(String coll, String shard) throws Exception {
        String path = StatePlanePaths.shardDeltas(StatePlanePaths.collectionPath(coll), shard);
        byte[] data = zkClient.getData(path, null, new Stat(), true);
        return StateDeltaCodec.decodeShardStateLog(data);
    }

    private StateSnapshot readSnapshot(String coll, String shard) throws Exception {
        String path = StatePlanePaths.shardSnapshot(StatePlanePaths.collectionPath(coll), shard);
        if (!zkClient.exists(path, true)) return null;
        byte[] data = zkClient.getData(path, null, new Stat(), true);
        return StateDeltaCodec.decodeStateSnapshot(data);
    }

    private static List<StateDelta.Entry> promo(int replicaId, int shortState) {
        return Collections.singletonList(new StateDelta.Entry(replicaId, shortState));
    }

    /** Effective state seen by a reader: read snapshot (or empty base) folded with the live ring. */
    private Map<Integer, Integer> effective(String coll, String shard) throws Exception {
        ShardStateLog ring = readRing(coll, shard);
        StateSnapshot snap = readSnapshot(coll, shard);
        if (snap == null) {
            snap = new StateSnapshot(Long.parseLong(COLL_ID), coll, ring.epoch, shard, ring.baseSeq, Collections.emptyMap());
        }
        return snap.reconstruct(ring.entries);
    }

    // ---- 1. compaction creates a valid snapshot and bounds the ring ----

    @Test
    public void compactionCreatesValidSnapshot() throws Exception {
        StatePlaneWriter w = writer("e1");
        assertNull("no snapshot before any compaction", readSnapshot("c1", "s1"));

        // 5 real transitions across two replicas → the 5th append (ring size == COMPACT_AFTER) folds.
        w.publish("c1", "s1", COLL_ID, promo(10, ACTIVE), null);      // seq1
        w.publish("c1", "s1", COLL_ID, promo(11, ACTIVE), null);      // seq2
        w.publish("c1", "s1", COLL_ID, promo(10, RECOVERING), null);  // seq3
        w.publish("c1", "s1", COLL_ID, promo(11, DOWN), null);        // seq4
        w.publish("c1", "s1", COLL_ID, promo(10, DOWN), null);        // seq5 -> compaction at upToSeq=5

        StateSnapshot snap = readSnapshot("c1", "s1");
        assertNotNull("snapshot written by compaction", snap);
        assertEquals("snapshot upToSeq == folded prefix head", 5L, snap.upToSeq());
        assertEquals("baseSeq mirrors upToSeq", 5L, snap.baseSeq);
        assertEquals("collection name carried into snapshot identity", "c1", snap.collectionName);
        Map<Integer, Integer> m = snap.replicaStates;
        assertEquals("replica 10 folded to DOWN", Integer.valueOf(DOWN), m.get(10));
        assertEquals("replica 11 folded to DOWN", Integer.valueOf(DOWN), m.get(11));

        ShardStateLog ring = readRing("c1", "s1");
        assertTrue("ring bounded after compaction (<= compactAfterCount)", ring.entries.size() <= COMPACT_AFTER);
        assertEquals("folded prefix removed from ring", 0, ring.entries.size());
        assertEquals("ring baseSeq advanced to upToSeq", 5L, ring.baseSeq);
        assertEquals("lastSeq high-water preserved across the fold", 5L, ring.lastSeq);

        // The reader-visible effective state is unchanged by compaction.
        Map<Integer, Integer> eff = effective("c1", "s1");
        assertEquals(Integer.valueOf(DOWN), eff.get(10));
        assertEquals(Integer.valueOf(DOWN), eff.get(11));
    }

    // ---- 2. old deltas removed only after the snapshot is durable (no stale window) ----

    @Test
    public void deltasDeletedOnlyAfterSnapshotDurable() throws Exception {
        StatePlaneWriter w = writer("e1");
        for (int i = 0; i < COMPACT_AFTER; i++) {
            w.publish("c2", "s1", COLL_ID, promo(10, i % 2 == 0 ? ACTIVE : RECOVERING), null);
        }
        ShardStateLog ring = readRing("c2", "s1");
        StateSnapshot snap = readSnapshot("c2", "s1");

        // Invariant proving the order: the ring's baseSeq advanced past the folded prefix, and a
        // snapshot covering EXACTLY that prefix (upToSeq == baseSeq) is already present. There is no
        // window where deltas are gone (baseSeq advanced) but the snapshot is missing or stale.
        assertTrue("ring advanced its baseSeq (deltas folded out)", ring.baseSeq > 0);
        assertNotNull("snapshot durable whenever baseSeq has advanced", snap);
        assertEquals("snapshot fully covers everything below the ring baseSeq", ring.baseSeq, snap.upToSeq());

        // And the snapshot+remaining-ring reconstructs the true effective state (nothing lost).
        Map<Integer, Integer> eff = effective("c2", "s1");
        assertEquals("last write (seq5 -> ACTIVE) is captured in the snapshot", Integer.valueOf(ACTIVE), eff.get(10));
    }

    // ---- 3. a reader holding a pre-compaction cursor catches up from snapshot + remaining deltas ----

    @Test
    public void readerCatchUpFromCompactedSnapshot() throws Exception {
        StatePlaneWriter w = writer("e1");
        for (int i = 0; i < COMPACT_AFTER; i++) {
            w.publish("c3", "s1", COLL_ID, promo(10, i % 2 == 0 ? ACTIVE : RECOVERING), null);
        } // compaction at upToSeq=5, snapshot {10: ACTIVE}, ring empty
        // Two more post-compaction deltas (seq6, seq7) so the reader folds snapshot + remaining deltas.
        w.publish("c3", "s1", COLL_ID, promo(10, DOWN), null);        // seq6
        w.publish("c3", "s1", COLL_ID, promo(11, BUFFERING), null);   // seq7

        ShardStateLog ring = readRing("c3", "s1");
        StateSnapshot snap = readSnapshot("c3", "s1");
        assertNotNull(snap);
        assertEquals(5L, snap.upToSeq());

        // A fresh reader (cursor {0,0}) and a reader stuck at a pre-compaction position both must
        // force a snapshot catch-up because the ring's baseSeq advanced past their cursor.
        assertTrue("fresh reader catches up from snapshot",
                StatePlaneReader.needsSnapshotCatchup(ring, new long[] {0L, 0L}));
        assertTrue("reader stuck mid-prefix (seq=3) catches up from snapshot",
                StatePlaneReader.needsSnapshotCatchup(ring, new long[] {ring.epoch, 3L}));
        // A reader already at the snapshot head folds the remaining deltas incrementally (no catch-up).
        assertFalse("reader at the snapshot head applies remaining deltas incrementally",
                StatePlaneReader.needsSnapshotCatchup(ring, new long[] {ring.epoch, 5L}));

        // snapshot + remaining-ring reconstructs the correct effective state.
        Map<Integer, Integer> eff = snap.reconstruct(ring.entries);
        assertEquals("10 -> DOWN (seq6)", Integer.valueOf(DOWN), eff.get(10));
        assertEquals("11 -> BUFFERING (seq7)", Integer.valueOf(BUFFERING), eff.get(11));
    }

    // ---- 4. interrupted compaction (snapshot written, deltas NOT yet deleted) loses no state ----

    @Test
    public void interruptedCompactionLosesNoState() throws Exception {
        StatePlaneWriter w = writer("e1");
        // Drive 4 deltas — one short of the compaction trigger, so the ring still holds all 4.
        w.publish("c4", "s1", COLL_ID, promo(10, ACTIVE), null);      // seq1
        w.publish("c4", "s1", COLL_ID, promo(10, RECOVERING), null);  // seq2
        w.publish("c4", "s1", COLL_ID, promo(11, DOWN), null);        // seq3
        w.publish("c4", "s1", COLL_ID, promo(10, DOWN), null);        // seq4
        ShardStateLog ring = readRing("c4", "s1");
        assertEquals("no compaction yet (under the trigger)", 4, ring.entries.size());

        Map<Integer, Integer> before = effective("c4", "s1");

        // Simulate a CRASH between snapshot-write and delta-delete: write a snapshot folding the whole
        // ring (upToSeq = lastSeq) but leave every delta in place (the delete never happened).
        Map<Integer, Integer> folded =
                new StateSnapshot(Long.parseLong(COLL_ID), "c4", ring.epoch, "s1", ring.baseSeq, Collections.emptyMap())
                        .reconstruct(ring.entries);
        StateSnapshot crashSnap = new StateSnapshot(Long.parseLong(COLL_ID), "c4", ring.epoch, "s1", ring.lastSeq, folded);
        String snapPath = StatePlanePaths.shardSnapshot(StatePlanePaths.collectionPath("c4"), "s1");
        zkClient.makePath(snapPath, StateDeltaCodec.encodeStateSnapshot(crashSnap),
                org.apache.zookeeper.CreateMode.PERSISTENT, true);
        // Ring is untouched: all 4 deltas (seq <= upToSeq) still present.
        assertEquals("leftover deltas still in ring after the 'crash'", 4, readRing("c4", "s1").entries.size());

        // Reconstruct: the leftover deltas are all stale (seq <= snapshot.upToSeq) and skipped — the
        // effective state is identical to before the interrupted compaction. No double-apply, no loss.
        Map<Integer, Integer> after = effective("c4", "s1");
        assertEquals("interrupted compaction is idempotent — state unchanged", before, after);
        assertEquals("replica 10 still DOWN", Integer.valueOf(DOWN), after.get(10));
        assertEquals("replica 11 still DOWN", Integer.valueOf(DOWN), after.get(11));

        // A subsequent normal publish still moves forward correctly off the crash snapshot + ring.
        assertTrue(w.publish("c4", "s1", COLL_ID, promo(11, ACTIVE), null));
        assertEquals("forward progress after recovery", Integer.valueOf(ACTIVE), effective("c4", "s1").get(11));
    }

    // ---- 5. a new higher-seq delta interleaved with compaction survives and wins (no regression) ----

    @Test
    public void compactionRaceWithNewDeltaDoesNotRegress() throws Exception {
        StatePlaneWriter w = writer("e1");
        for (int i = 0; i < COMPACT_AFTER; i++) {
            w.publish("c5", "s1", COLL_ID, promo(10, i % 2 == 0 ? ACTIVE : RECOVERING), null);
        } // compaction at upToSeq=5
        StateSnapshot snap = readSnapshot("c5", "s1");
        assertEquals(5L, snap.upToSeq());
        long lastSeqAfterCompaction = readRing("c5", "s1").lastSeq;
        assertEquals("high-water never regresses across compaction", 5L, lastSeqAfterCompaction);

        // A new delta lands AFTER compaction: it must take seq > upToSeq and win — never be folded away.
        assertTrue(w.publish("c5", "s1", COLL_ID, promo(10, LEADER), null)); // seq6
        ShardStateLog ring = readRing("c5", "s1");
        assertEquals("new delta got seq strictly above the compacted upToSeq", 6L, ring.lastSeq);
        StateDelta newest = ring.entries.get(ring.entries.size() - 1);
        assertEquals("new delta carried in the ring (not folded into the snapshot)", 6L, newest.seq);
        assertEquals(LEADER, newest.entries.get(0).shortState);

        // The new state wins on a full reconstruct.
        assertEquals("post-compaction delta wins", Integer.valueOf(LEADER), effective("c5", "s1").get(10));

        // A re-publish of the already-current state is a no-op — a retry cannot regress (CAS safety).
        assertFalse("re-publish of current state is an idempotent no-op",
                w.publish("c5", "s1", COLL_ID, promo(10, LEADER), null));
        assertEquals("still LEADER, never regressed", Integer.valueOf(LEADER), effective("c5", "s1").get(10));

        // A second writer (overseer takeover) can append safely on top of the compacted snapshot.
        StatePlaneWriter w2 = writer("e2");
        assertTrue(w2.publish("c5", "s1", COLL_ID, promo(10, DOWN), null)); // seq7
        assertEquals("cross-writer seq monotonic past compaction", 7L, readRing("c5", "s1").lastSeq);
        assertEquals(Integer.valueOf(DOWN), effective("c5", "s1").get(10));
    }

    // ---- 6. time-based compaction trigger folds even under the count threshold ----

    @Test
    public void timeBasedCompactionTrigger() throws Exception {
        String saved = System.getProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP);
        // Disable the count trigger; enable an immediate time trigger.
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, "1000");
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP, "1");
        try {
            StatePlaneWriter w = writer("e1");
            // First publish establishes the per-shard timer baseline (no compaction yet).
            w.publish("c6", "s1", COLL_ID, promo(10, ACTIVE), null); // seq1
            Thread.sleep(20); // exceed the 1ms threshold
            // Second publish: elapsed >= compactAfterMillis → compaction folds despite count < 1000.
            w.publish("c6", "s1", COLL_ID, promo(10, RECOVERING), null); // seq2 -> time-triggered compaction
            StateSnapshot snap = readSnapshot("c6", "s1");
            assertNotNull("time-based trigger compacted under the count threshold", snap);
            assertEquals("snapshot folded up to the latest seq", 2L, snap.upToSeq());
            assertEquals(Integer.valueOf(RECOVERING), snap.replicaStates.get(10));
            assertEquals("ring emptied by time-based compaction", 0, readRing("c6", "s1").entries.size());
        } finally {
            if (saved == null) System.clearProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP);
            else System.setProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP, saved);
            System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, Integer.toString(COMPACT_AFTER));
        }
    }

    // ---- 7. hard-cap trim folds the snapshot FIRST and never advances baseSeq past coverage ----

    /**
     * Finding #8: the ring-cap blind trim must not drop deltas the snapshot does not cover. With the
     * normal count/time compaction triggers disabled and a tiny {@code ringCap}, every over-cap publish
     * must fold the committed ring into the snapshot before trimming, so {@code ring.baseSeq} never runs
     * ahead of {@code snapshot.upToSeq} (an unrecoverable gap) and a full reconstruct is always exact.
     */
    @Test
    public void hardCapTrimFoldsSnapshotAndPreservesReconstructability() throws Exception {
        // Disable the count trigger so the ONLY snapshot writer is the hard-cap fold path.
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, "1000");
        String savedCap = System.getProperty(StatePlaneWriter.RING_CAP_SYSPROP);
        System.setProperty(StatePlaneWriter.RING_CAP_SYSPROP, "3");
        try {
            StatePlaneWriter w = writer("e1"); // reads ringCap=3 at construction
            final String coll = "hc1", shard = "s1";

            w.publish(coll, shard, COLL_ID, promo(10, ACTIVE), null);     // seq1
            assertNoGap(coll, shard);
            w.publish(coll, shard, COLL_ID, promo(11, ACTIVE), null);     // seq2
            assertNoGap(coll, shard);
            w.publish(coll, shard, COLL_ID, promo(12, ACTIVE), null);     // seq3 (ring size == cap, no trim yet)
            assertNoGap(coll, shard);
            assertNull("under the cap, no hard-cap fold yet", readSnapshot(coll, shard));

            w.publish(coll, shard, COLL_ID, promo(10, RECOVERING), null); // seq4 -> over cap -> fold+trim
            assertNoGap(coll, shard);
            w.publish(coll, shard, COLL_ID, promo(11, DOWN), null);       // seq5 -> over cap -> fold+trim
            assertNoGap(coll, shard);
            w.publish(coll, shard, COLL_ID, promo(12, RECOVERING), null); // seq6 -> over cap -> fold+trim
            assertNoGap(coll, shard);

            ShardStateLog ring = readRing(coll, shard);
            StateSnapshot snap = readSnapshot(coll, shard);
            assertNotNull("hard-cap path wrote a snapshot before trimming", snap);
            assertTrue("ring bounded by the hard cap", ring.entries.size() <= 3);
            assertTrue("baseSeq never advanced past snapshot coverage (no gap)",
                    ring.baseSeq <= snap.upToSeq());

            // Full reconstruct (snapshot + remaining ring) preserves EVERY replica's latest state — no
            // transition was lost to the trim, including replicas whose only delta fell in a dropped prefix.
            Map<Integer, Integer> eff = effective(coll, shard);
            assertEquals("r10 latest = RECOVERING (seq4)", Integer.valueOf(RECOVERING), eff.get(10));
            assertEquals("r11 latest = DOWN (seq5)", Integer.valueOf(DOWN), eff.get(11));
            assertEquals("r12 latest = RECOVERING (seq6)", Integer.valueOf(RECOVERING), eff.get(12));
            assertEquals("no phantom replicas", 3, eff.size());
        } finally {
            System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, Integer.toString(COMPACT_AFTER));
            if (savedCap == null) System.clearProperty(StatePlaneWriter.RING_CAP_SYSPROP);
            else System.setProperty(StatePlaneWriter.RING_CAP_SYSPROP, savedCap);
        }
    }

    // ---- 8. hard-cap fails safe: if the fold cannot be made durable, NO delta is dropped ----

    /** A writer whose snapshot fold can never be made durable (returns -1), to drive the fail-safe path. */
    static class FoldFailingWriter extends StatePlaneWriter {
        FoldFailingWriter(SolrZkClient zk, ElectionFence fence) { super(zk, fence); }
        @Override
        protected long foldCommittedRingIntoSnapshot(String collPath, String coll, String shard, ShardStateLog ring) {
            return -1L; // simulate a snapshot fold that cannot be made durable
        }
    }

    /**
     * Finding #8 fail-safe: when the hard-cap path cannot fold the committed ring into a durable
     * snapshot, it must retain ALL deltas (the ring grows past the cap) rather than advance baseSeq past
     * un-snapshotted deltas. Reconstruct-ability is preserved entirely from the (un-trimmed) ring.
     */
    @Test
    public void hardCapRetainsAllDeltasWhenSnapshotFoldCannotAdvance() throws Exception {
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, "1000");
        String savedCap = System.getProperty(StatePlaneWriter.RING_CAP_SYSPROP);
        System.setProperty(StatePlaneWriter.RING_CAP_SYSPROP, "3");
        try {
            StatePlaneWriter w = new FoldFailingWriter(zkClient, new AlwaysElectedFence("e1"));
            final String coll = "hc2", shard = "s1";

            w.publish(coll, shard, COLL_ID, promo(20, ACTIVE), null);      // seq1
            w.publish(coll, shard, COLL_ID, promo(21, ACTIVE), null);      // seq2
            w.publish(coll, shard, COLL_ID, promo(22, ACTIVE), null);      // seq3 (== cap)
            w.publish(coll, shard, COLL_ID, promo(23, RECOVERING), null);  // seq4 over cap, fold fails -> no trim
            w.publish(coll, shard, COLL_ID, promo(24, DOWN), null);        // seq5 over cap, fold fails -> no trim

            ShardStateLog ring = readRing(coll, shard);
            assertNull("fold never succeeded -> no snapshot written", readSnapshot(coll, shard));
            assertEquals("fail-safe retained every delta (ring may exceed the cap)", 5, ring.entries.size());
            assertEquals("baseSeq never advanced past un-snapshotted deltas", 0L, ring.baseSeq);

            // The full (un-trimmed) ring still reconstructs every replica's state — nothing was lost.
            Map<Integer, Integer> eff = effective(coll, shard);
            assertEquals(Integer.valueOf(ACTIVE), eff.get(20));
            assertEquals(Integer.valueOf(ACTIVE), eff.get(21));
            assertEquals(Integer.valueOf(ACTIVE), eff.get(22));
            assertEquals(Integer.valueOf(RECOVERING), eff.get(23));
            assertEquals(Integer.valueOf(DOWN), eff.get(24));
            assertEquals(5, eff.size());
        } finally {
            System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, Integer.toString(COMPACT_AFTER));
            if (savedCap == null) System.clearProperty(StatePlaneWriter.RING_CAP_SYSPROP);
            else System.setProperty(StatePlaneWriter.RING_CAP_SYSPROP, savedCap);
        }
    }

    // ---- 9. snapshot writes are monotonic: a stale writer cannot regress a higher-coverage snapshot ----

    /** Surfaces the {@code protected} fold seam so a test can drive the snapshot-write primitive directly. */
    static class ExposingWriter extends StatePlaneWriter {
        ExposingWriter(SolrZkClient zk, ElectionFence fence) { super(zk, fence); }
        long fold(String collPath, String coll, String shard, ShardStateLog ring) {
            return foldCommittedRingIntoSnapshot(collPath, coll, shard, ring);
        }
    }

    private static ShardStateLog ringWith(int epoch, long lastSeq, String writerId, int replicaId, int shortState) {
        List<StateDelta> entries = new ArrayList<>();
        entries.add(new StateDelta(COLL_ID, "s1", epoch, lastSeq, promo(replicaId, shortState),
                Collections.emptyList(), ACTIVE));
        return new ShardStateLog(COLL_ID, "s1", epoch, 0L, lastSeq, writerId, entries);
    }

    /**
     * Fix #3: {@code writeSnapshotMonotonic} must never let a stale writer (one that computed a LOWER
     * {@code upToSeq}) overwrite a newer writer's higher-coverage snapshot. Combined with a ring trim,
     * a regressed snapshot opens an unreconstructable gap between {@code snapshot.upToSeq} and
     * {@code ring.baseSeq}. Here a newer writer has already folded to upToSeq=100; a stale fold at
     * upToSeq=5 must be skipped, leaving the upToSeq=100 snapshot intact.
     */
    @Test
    public void snapshotWriteRejectsStaleRegression() throws Exception {
        final String coll = "mono1", shard = "s1";
        final String collPath = StatePlanePaths.collectionPath(coll);

        // A newer writer's snapshot already covers up to seq 100.
        Map<Integer, Integer> newerStates = new HashMap<>();
        newerStates.put(10, DOWN);
        newerStates.put(11, LEADER);
        StateSnapshot newer = new StateSnapshot(Long.parseLong(COLL_ID), coll, 0, shard, 100L, newerStates);
        String snapPath = StatePlanePaths.shardSnapshot(collPath, shard);
        zkClient.makePath(snapPath, StateDeltaCodec.encodeStateSnapshot(newer),
                org.apache.zookeeper.CreateMode.PERSISTENT, true);

        // A stale writer folds a ring whose lastSeq (5) is far below the live snapshot's coverage (100).
        ExposingWriter stale = new ExposingWriter(zkClient, new AlwaysElectedFence("stale"));
        long covered = stale.fold(collPath, coll, shard, ringWith(0, 5L, "stale", 10, ACTIVE));

        StateSnapshot after = readSnapshot(coll, shard);
        assertNotNull(after);
        assertEquals("regressive snapshot write rejected — coverage never drops below the newer writer",
                100L, after.upToSeq());
        assertEquals("higher-coverage replica states preserved (10 stays DOWN, not the stale ACTIVE)",
                Integer.valueOf(DOWN), after.replicaStates.get(10));
        assertEquals("replica only present in the newer snapshot survives the rejected regression",
                Integer.valueOf(LEADER), after.replicaStates.get(11));
        // The fold still returns its own (conservative) upToSeq; the snapshot already covers >= that.
        assertEquals("fold returns its computed upToSeq as a conservative lower bound", 5L, covered);
    }

    /**
     * The monotonic guard must not over-block: a fold whose {@code upToSeq} is strictly HIGHER than the
     * existing snapshot's coverage must still advance the snapshot (CAS overwrite at the read version).
     */
    @Test
    public void snapshotWriteAdvancesOnHigherCoverage() throws Exception {
        final String coll = "mono2", shard = "s1";
        final String collPath = StatePlanePaths.collectionPath(coll);

        StateSnapshot older = new StateSnapshot(Long.parseLong(COLL_ID), coll, 0, shard, 5L,
                Collections.singletonMap(10, ACTIVE));
        String snapPath = StatePlanePaths.shardSnapshot(collPath, shard);
        zkClient.makePath(snapPath, StateDeltaCodec.encodeStateSnapshot(older),
                org.apache.zookeeper.CreateMode.PERSISTENT, true);

        ExposingWriter w = new ExposingWriter(zkClient, new AlwaysElectedFence("e1"));
        long covered = w.fold(collPath, coll, shard, ringWith(0, 150L, "e1", 10, DOWN));

        StateSnapshot after = readSnapshot(coll, shard);
        assertNotNull(after);
        assertEquals("higher-coverage fold advances the snapshot", 150L, after.upToSeq());
        assertEquals("new state folded in (10 -> DOWN at seq150)", Integer.valueOf(DOWN),
                after.replicaStates.get(10));
        assertEquals(150L, covered);
    }

    /** No unrecoverable gap: if the ring's baseSeq advanced, a snapshot must cover at least that far. */
    private void assertNoGap(String coll, String shard) throws Exception {
        ShardStateLog ring = readRing(coll, shard);
        StateSnapshot snap = readSnapshot(coll, shard);
        if (ring.baseSeq > 0) {
            assertNotNull("baseSeq advanced to " + ring.baseSeq + " but no snapshot exists — gap!", snap);
            assertTrue("ring.baseSeq (" + ring.baseSeq + ") must not exceed snapshot.upToSeq ("
                            + snap.upToSeq() + ") — that is an unrecoverable gap",
                    ring.baseSeq <= snap.upToSeq());
        }
    }
    @Test
    public void testCompactionSnapshotWriteRequiresAuthoritativeOwnership() throws Exception {
        String oldCount = System.getProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP);
        String oldMillis = System.getProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP);
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, "1");
        System.setProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP, "0");
        try {
            class Fence implements StatePlaneWriter.ElectionFence {
                private int authoritativeChecks;

                @Override
                public boolean stillElected() {
                    return true;
                }

                @Override
                public String writerId() {
                    return "writer";
                }

                @Override
                public boolean isFencedBy(String ringWriterId) {
                    return ringWriterId != null && !writerId().equals(ringWriterId);
                }

                @Override
                public boolean ownsElectionAuthoritative() {
                    // The publish path on a not-yet-existing ring spends three authoritative-ownership
                    // checks before the post-append compaction, and we want the APPEND to land but the
                    // compaction to be fenced:
                    //   #1 publish pre-loop guard                 -> true
                    //   #2 lazyCreateRing (ring does not exist)   -> true  (creates the empty ring)
                    //   #3 publish pre-CAS guard                  -> true  -> delta APPENDS (size == 1)
                    //   #4 compaction work guard (post-append)    -> false -> compaction FENCED
                    // so the ring keeps its single delta and no snapshot znode is ever written.
                    return ++authoritativeChecks <= 3;
                }
            }

            StatePlaneWriter w = new StatePlaneWriter(zkClient, new Fence());
            RuntimeException fenced = null;
            try {
                w.publish("c1", "s1", COLL_ID, promo(10, ACTIVE), java.util.Collections.emptyList());
                fail("expected compaction snapshot write to be fenced");
            } catch (RuntimeException e) {
                fenced = e;
            }
            assertEquals(
                "org.apache.solr.common.cloud.StatePlaneWriter$FencedException",
                fenced.getClass().getName());

            ShardStateLog ring = readRing("c1", "s1");
            assertEquals(1, ring.entries.size());
            assertFalse("lost overseer ownership must not write a compaction snapshot",
                zkClient.exists(StatePlanePaths.shardSnapshot(StatePlanePaths.collectionPath("c1"), "s1"), true));
        } finally {
            if (oldCount == null) {
                System.clearProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP);
            } else {
                System.setProperty(StateDeltaConfig.COMPACT_AFTER_COUNT_SYSPROP, oldCount);
            }
            if (oldMillis == null) {
                System.clearProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP);
            } else {
                System.setProperty(StateDeltaConfig.COMPACT_AFTER_MILLIS_SYSPROP, oldMillis);
            }
        }
    }

}
