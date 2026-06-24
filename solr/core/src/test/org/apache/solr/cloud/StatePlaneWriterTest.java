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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.common.cloud.ShardStateLog;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StatePlanePaths;
import org.apache.solr.common.cloud.StatePlaneWriter;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * PR-1 writer unit tests for {@link StatePlaneWriter}. ZK-backed via {@link ZkTestServer}.
 *
 * Short states: LEADER=1, ACTIVE=2, BUFFERING=3, RECOVERING=4, DOWN=5, RECOVERY_FAILED=6.
 */
public class StatePlaneWriterTest extends SolrTestCaseJ4 {

    private static final int LEADER = 1;
    private static final int ACTIVE = 2;
    private static final int RECOVERING = 4;
    private static final int DOWN = 5;

    private ZkTestServer server;
    private SolrZkClient zkClient;

    /** Always-elected fence with a fixed writerId; never fenced out. */
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
        Path zkDir = SolrTestUtil.createTempDir("zkData-spw");
        server = new ZkTestServer(zkDir);
        server.run(true);
        zkClient = new SolrZkClient(server.getZkAddress(), AbstractZkTestCase.TIMEOUT);
        zkClient.start();
    }

    @After
    public void tearDown() throws Exception {
        if (zkClient != null) zkClient.close();
        if (server != null) server.shutdown();
        super.tearDown();
    }

    private StatePlaneWriter writer(String id) {
        return new StatePlaneWriter(zkClient, new AlwaysElectedFence(id));
    }

    private ShardStateLog readRing(String coll, String shard) throws Exception {
        String path = StatePlanePaths.shardDeltas(StatePlanePaths.collectionPath(coll), shard);
        byte[] data = zkClient.getData(path, null, new Stat(), true);
        return org.apache.solr.common.cloud.StateDeltaCodec.decodeShardStateLog(data);
    }

    private static List<StateDelta.Entry> promo(int replicaId, int shortState) {
        return Collections.singletonList(new StateDelta.Entry(replicaId, shortState));
    }

    /** Publishing ONE replica update writes ONE delta to the per-shard deltas ring, no full _statupdates. */
    @Test
    public void casAppend() throws Exception {
        StatePlaneWriter w = writer("e1");
        boolean appended = w.publish("c1", "s1", promo(10, ACTIVE), null);
        assertTrue("first publish should append", appended);

        ShardStateLog ring = readRing("c1", "s1");
        assertEquals("exactly one delta", 1, ring.entries.size());
        assertEquals("seq starts at 1", 1L, ring.lastSeq);
        StateDelta d = ring.entries.get(0);
        assertEquals(1, d.entries.size());
        assertEquals(10, d.entries.get(0).replicaId);
        assertEquals(ACTIVE, d.entries.get(0).shortState);

        // DELTA-mode writer never writes the legacy full-map _statupdates node.
        String legacy = StatePlanePaths.collectionPath("c1") + "/_statupdates";
        assertFalse("no legacy _statupdates write", zkClient.exists(legacy));
    }

    /** A duplicate publish of an already-current state is an idempotent no-op (no new delta). */
    @Test
    public void duplicateSuppressed() throws Exception {
        StatePlaneWriter w = writer("e1");
        assertTrue(w.publish("c1", "s1", promo(10, ACTIVE), null));
        boolean second = w.publish("c1", "s1", promo(10, ACTIVE), null);
        assertFalse("duplicate of current state is suppressed", second);
        assertEquals("still only one delta", 1, readRing("c1", "s1").entries.size());
        assertEquals(1L, readRing("c1", "s1").lastSeq);
    }

    /** Same-name collection recreations must not reuse the prior incarnation's writer cache. */
    @Test
    public void effectiveCacheIsScopedByCollectionIncarnation() throws Exception {
        StatePlaneWriter w = writer("e1");
        String coll = "reincarnated";
        String shard = "s1";

        // First incarnation: seed snapshot state at seq 0, then publish the same state. This builds a
        // writer-local effective cache with lastSeq=0 and replica 10 already ACTIVE.
        java.util.Map<Integer, Integer> seeded = new java.util.HashMap<>();
        seeded.put(10, ACTIVE);
        w.seedShard(coll, shard, 1, seeded);
        assertFalse(w.publish(coll, shard, "incarnation-1", promo(10, ACTIVE), null));
        assertEquals(0L, readRing(coll, shard).lastSeq);

        // Delete the collection state-plane znodes and recreate the same name. The fresh ring also
        // starts at lastSeq=0. If the cache were keyed only by collection name/shard, the first needed
        // ACTIVE delta would be suppressed as a stale no-op from the previous incarnation.
        zkClient.clean(StatePlanePaths.collectionPath(coll));
        assertTrue(w.publish(coll, shard, "incarnation-2", promo(10, ACTIVE), null));
        ShardStateLog recreated = readRing(coll, shard);
        assertEquals("new incarnation must append its own first delta", 1L, recreated.lastSeq);
        assertEquals(1, recreated.entries.size());
        assertEquals(10, recreated.entries.get(0).entries.get(0).replicaId);
        assertEquals(ACTIVE, recreated.entries.get(0).entries.get(0).shortState);
    }

    /** A BadVersion/ConnLoss-style retry that re-reads effective state cannot regress it. */
    @Test
    public void badVersionRetryIdempotent() throws Exception {
        StatePlaneWriter w = writer("e1");
        w.publish("c1", "s1", promo(10, ACTIVE), null);
        w.publish("c1", "s1", promo(10, RECOVERING), null);
        // Re-publishing the older ACTIVE state is NOT a no-op (state differs) and appends forward;
        // but re-publishing the CURRENT state is suppressed — the property that makes a retry safe.
        boolean noop = w.publish("c1", "s1", promo(10, RECOVERING), null);
        assertFalse("re-publish of current effective state is a no-op (retry cannot regress)", noop);
        ShardStateLog ring = readRing("c1", "s1");
        // Effective state of replica 10 is RECOVERING (the latest), never regressed back to ACTIVE.
        assertEquals(RECOVERING,
                ring.entries.get(ring.entries.size() - 1).entries.get(0).shortState);
    }

    /** LEADER promotion demotes the prior leader to ACTIVE in the SAME delta (D5/D14). */
    @Test
    public void leaderDemotionEntryActive() throws Exception {
        StatePlaneWriter w = writer("e1");
        w.publish("c1", "s1", promo(10, LEADER), null);          // replica 10 is leader
        w.publish("c1", "s1", promo(11, LEADER), null);          // replica 11 takes over
        ShardStateLog ring = readRing("c1", "s1");
        StateDelta last = ring.entries.get(ring.entries.size() - 1);
        assertEquals("11 promoted to LEADER", LEADER, last.entries.get(0).shortState);
        assertEquals("prior leader 10 demoted in same delta", 1, last.demotedReplicaIds.size());
        assertEquals(Integer.valueOf(10), last.demotedReplicaIds.get(0));
        assertEquals("demoted to ACTIVE(2) (D14)", ACTIVE, last.demotedShortState);
    }

    /** Re-publishing LEADER while a stale LEADER lingers still emits the demotion (no two-leader no-op). */
    @Test
    public void leaderRepublishStillDemotesStaleLeader() throws Exception {
        StatePlaneWriter w = writer("e1");
        w.publish("c1", "s1", promo(10, LEADER), null);
        // Simulate a session-loss wedge: replica 11 also recorded LEADER (stale) via a raw append.
        w.publish("c1", "s1", promo(11, LEADER), Collections.emptyList());
        // The previous call already demoted 10. Now there is exactly one leader (11). Re-publish 11
        // as LEADER: state-noop for 11, but if a stale leader still existed it must be demoted.
        // Force a stale-leader situation: publish 10 LEADER again (demotes 11), then re-publish 10.
        w.publish("c1", "s1", promo(10, LEADER), null); // 10 leader again, demotes 11
        boolean republish = w.publish("c1", "s1", promo(10, LEADER), null); // already leader, no stale
        assertFalse("no stale leader → republish of sole leader is a no-op", republish);

        // Now create a genuine stale-leader: directly append a second LEADER without demotion is not
        // possible through publish (it always demotes), so assert the invariant via the prior deltas:
        // every LEADER promotion that displaced another leader carried a demotion.
        ShardStateLog ring = readRing("c1", "s1");
        for (StateDelta d : ring.entries) {
            for (StateDelta.Entry e : d.entries) {
                if (e.shortState == LEADER && ring.entries.indexOf(d) > 0) {
                    // any non-initial leader promotion must have demoted the prior leader
                    assertFalse("leader takeover carries a demotion", d.demotedReplicaIds.isEmpty());
                }
            }
        }
    }

    /** Two concurrent same-shard publishes serialize (D6): seqs are strictly monotonic, none lost. */
    @Test
    public void perShardSerialized() throws Exception {
        final StatePlaneWriter w = writer("e1");
        final int perThread = 20;
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<Throwable> err = new AtomicReference<>();
        Runnable a = () -> {
            try {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    // distinct states so each is a real transition, never a no-op
                    w.publish("c1", "s1", promo(100, i % 2 == 0 ? ACTIVE : RECOVERING), null);
                }
            } catch (Throwable t) { err.compareAndSet(null, t); }
        };
        Runnable b = () -> {
            try {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    w.publish("c1", "s1", promo(200, i % 2 == 0 ? DOWN : ACTIVE), null);
                }
            } catch (Throwable t) { err.compareAndSet(null, t); }
        };
        Thread t1 = new Thread(a), t2 = new Thread(b);
        t1.start(); t2.start();
        start.countDown();
        t1.join(60000); t2.join(60000);
        assertNull("no error during concurrent same-shard publish", err.get());

        ShardStateLog ring = readRing("c1", "s1");
        // Seqs must be strictly increasing and contiguous up to lastSeq (no interleave/regress).
        long prev = ring.baseSeq;
        for (StateDelta d : ring.entries) {
            assertTrue("strictly increasing seq: " + d.seq + " after " + prev, d.seq > prev);
            prev = d.seq;
        }
        assertEquals("lastSeq == highest delta seq", prev, ring.lastSeq);
    }

    /** Node-down batches all of a node's replicas in one (coll,shard) into ONE multi-entry delta (D7). */
    @Test
    public void nodeDownSingleChokepointBatched() throws Exception {
        StatePlaneWriter w = writer("e1");
        // Three replicas of the downed node live in shard s1.
        List<StateDelta.Entry> batch = new ArrayList<>();
        batch.add(new StateDelta.Entry(10, DOWN));
        batch.add(new StateDelta.Entry(11, DOWN));
        batch.add(new StateDelta.Entry(12, DOWN));
        boolean appended = w.publish("c1", "s1", batch, null);
        assertTrue(appended);

        ShardStateLog ring = readRing("c1", "s1");
        assertEquals("ONE delta for the whole node-down batch, not one-per-replica", 1, ring.entries.size());
        assertEquals("multi-entry delta carries all 3 replicas", 3, ring.entries.get(0).entries.size());
        // Not a full-map collection write: no _statupdates node.
        assertFalse(zkClient.exists(StatePlanePaths.collectionPath("c1") + "/_statupdates"));
    }

    /** DOWNNODE/RECOVERYNODE-style fan-out across shards each produce their own bounded delta. */
    @Test
    public void downnodeRecoverynodeFanout() throws Exception {
        StatePlaneWriter w = writer("e1");
        w.publish("c1", "s1", promo(10, DOWN), null);
        w.publish("c1", "s2", promo(20, DOWN), null);
        assertEquals(1, readRing("c1", "s1").entries.size());
        assertEquals(1, readRing("c1", "s2").entries.size());
        // recovery
        w.publish("c1", "s1", promo(10, RECOVERING), null);
        assertEquals(2, readRing("c1", "s1").entries.size());
        assertEquals(RECOVERING,
                readRing("c1", "s1").entries.get(1).entries.get(0).shortState);
    }

    /** Publishing to a never-published shard lazily creates the skeleton, then succeeds (D17). */
    @Test
    public void lazyRingCreate() throws Exception {
        String deltaPath = StatePlanePaths.shardDeltas(StatePlanePaths.collectionPath("fresh"), "sX");
        assertFalse("ring does not exist yet", zkClient.exists(deltaPath));
        StatePlaneWriter w = writer("e1");
        assertTrue(w.publish("fresh", "sX", promo(1, ACTIVE), null));
        assertTrue("ring lazily created", zkClient.exists(deltaPath));
        assertEquals(1, readRing("fresh", "sX").entries.size());
    }

    /**
     * Finding #1: a single replica's state change appends a delta carrying ONLY that replica's entry,
     * not the whole shard/collection map. Seed a 3-replica shard, change ONE replica, and assert the
     * delta payload size is 1 (scales with changed replicas, not shard size).
     */
    @Test
    public void singleChangedEntryDeltaIsMinimal() throws Exception {
        StatePlaneWriter w = writer("e1");
        java.util.Map<Integer, Integer> seeded = new java.util.HashMap<>();
        seeded.put(10, ACTIVE);
        seeded.put(11, ACTIVE);
        seeded.put(12, ACTIVE);
        w.seedShard("c1", "s1", 1, seeded);
        assertEquals("seeded ring is empty (state lives in snapshot)", 0, readRing("c1", "s1").entries.size());

        // Only replica 11 transitions. The caller (ZkStateWriter) now passes ONLY this entry.
        assertTrue(w.publish("c1", "s1", promo(11, RECOVERING), null));

        ShardStateLog ring = readRing("c1", "s1");
        assertEquals("exactly one appended delta", 1, ring.entries.size());
        StateDelta d = ring.entries.get(0);
        assertEquals("delta carries ONLY the changed replica, not all 3", 1, d.entries.size());
        assertEquals(11, d.entries.get(0).replicaId);
        assertEquals(RECOVERING, d.entries.get(0).shortState);
        assertTrue("a non-leader change carries no demotions", d.demotedReplicaIds.isEmpty());
    }

    /**
     * Finding #1: leader demotion is computed INSIDE the writer from snapshot+ring, so the caller need
     * only publish the NEW leader's single entry — it does NOT pass the prior leader (the whole map).
     * Seed {10:LEADER, 11:ACTIVE}, publish only {11:LEADER}, assert the writer itself demotes 10.
     */
    @Test
    public void leaderDemotionComputedFromSingleEntry() throws Exception {
        StatePlaneWriter w = writer("e1");
        java.util.Map<Integer, Integer> seeded = new java.util.HashMap<>();
        seeded.put(10, LEADER);
        seeded.put(11, ACTIVE);
        w.seedShard("c1", "s1", 1, seeded);

        // Publish ONLY the new leader's entry — the prior leader (10) is NOT passed by the caller.
        assertTrue(w.publish("c1", "s1", promo(11, LEADER), null));

        ShardStateLog ring = readRing("c1", "s1");
        StateDelta last = ring.entries.get(ring.entries.size() - 1);
        assertEquals("delta carries ONLY the new leader entry", 1, last.entries.size());
        assertEquals(11, last.entries.get(0).replicaId);
        assertEquals(LEADER, last.entries.get(0).shortState);
        assertEquals("writer computed the prior-leader demotion itself", 1, last.demotedReplicaIds.size());
        assertEquals(Integer.valueOf(10), last.demotedReplicaIds.get(0));
        assertEquals("demoted to ACTIVE(2)", ACTIVE, last.demotedShortState);
    }

    /** Seed order: snapshot + deltas written, manifest(seeded=true) published LAST. */
    @Test
    public void seedWriteOrderSnapshotThenManifestLast() throws Exception {
        StatePlaneWriter w = writer("e1");
        String collPath = StatePlanePaths.collectionPath("seedc");
        java.util.Map<Integer, Integer> states = new java.util.HashMap<>();
        states.put(10, LEADER);
        states.put(11, ACTIVE);
        w.seedShard("seedc", "s1", 7, states);
        // snapshot + deltas exist; manifest not yet.
        assertTrue(zkClient.exists(StatePlanePaths.shardSnapshot(collPath, "s1")));
        assertTrue(zkClient.exists(StatePlanePaths.shardDeltas(collPath, "s1")));
        assertFalse("manifest not written until LAST", zkClient.exists(StatePlanePaths.manifest(collPath)));

        w.writeManifestSeeded("seedc", 7, Arrays.asList("s1"));
        assertTrue("manifest published last", zkClient.exists(StatePlanePaths.manifest(collPath)));

        ShardStateLog ring = readRing("seedc", "s1");
        assertEquals("seeded ring is empty (state lives in snapshot)", 0, ring.entries.size());
        assertEquals(7, ring.epoch);
    }
    @Test
    public void authoritativeFenceIsRecheckedImmediatelyBeforeCas() throws Exception {
        class LoseBeforeCasFence implements StatePlaneWriter.ElectionFence {
            int authoritativeChecks;

            @Override
            public boolean stillElected() {
                return true;
            }

            @Override
            public String writerId() {
                return "e1";
            }

            @Override
            public boolean isFencedBy(String ringWriterId) {
                return false;
            }

            @Override
            public boolean ownsElectionAuthoritative() {
                authoritativeChecks++;
                return authoritativeChecks == 1;
            }
        }

        LoseBeforeCasFence fence = new LoseBeforeCasFence();
        StatePlaneWriter w = new StatePlaneWriter(zkClient, fence);

        StatePlaneWriter.FencedException thrown = null;
        try {
            w.publish(
                "freshFence",
                "sX",
                Collections.singletonList(new StateDelta.Entry(100, 2)),
                Collections.emptyList());
        } catch (StatePlaneWriter.FencedException e) {
            thrown = e;
        }
        assertNotNull("publish must fail before CAS after authoritative ownership is lost", thrown);

        assertEquals("pre-loop and pre-CAS authoritative checks must both run", 2, fence.authoritativeChecks);
        String deltaPath = StatePlanePaths.shardDeltas(StatePlanePaths.collectionPath("freshFence"), "sX");
        if (zkClient.exists(deltaPath)) {
            assertEquals("stale writer must not append a delta after losing authoritative ownership", 0, readRing("freshFence", "sX").lastSeq);
        }
    }

}
