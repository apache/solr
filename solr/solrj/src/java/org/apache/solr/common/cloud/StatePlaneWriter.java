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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.solr.common.SolrException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PR-1 WRITER-ONLY primitive for the StateUpdates delta plane.
 *
 * <p>{@link #publish} is the single, <b>synchronous, per-(coll,shard)-serialized</b> append
 * primitive (D6) that replaces the async fire-and-forget {@code setData(...,-1,...)} for replica
 * state updates. It runs a CAS loop on {@code state/shards/<shard>/deltas}:
 *
 * <ol>
 *   <li>{@code getData} the ring (decode {@link ShardStateLog} via the {@link List} interface);
 *   <li><b>fence</b> (D4): re-check "am I still the elected overseer" AND reject a ring owned by a
 *       higher election id ({@code writerId} — election id, NOT epoch). {@code writerId} is a
 *       <b>writer-side fence + diagnostics only</b>; readers MUST NEVER validate it;
 *   <li>rebase the local epoch cursor if {@code ring.epoch > localEpoch};
 *   <li><b>compute pending demotions first</b> (D5) — a stale leader is demoted to {@code ACTIVE(2)}
 *       (D14) in the SAME delta as the promotion, so reader-visible state shows exactly one leader;
 *   <li>no-op ONLY when the promotions change no effective state AND there are no pending demotions
 *       (idempotent — a duplicate publish of an already-current state is skipped);
 *   <li>append one {@link StateDelta} ({@code seq = lastSeq + 1}, D2) and CAS
 *       {@code setData(expectedVersion = stat.getVersion())}; then PR-5 compaction folds the
 *       committed delta prefix into the per-shard snapshot (snapshot durable FIRST, then a
 *       CAS-guarded ring trim) once {@link StateDeltaConfig#compactAfterCount()} (or the time
 *       trigger) is reached. {@code ringCap} remains a blind-trim hard safety backstop only.
 * </ol>
 *
 * Error handling: {@code BadVersion} → re-getData + retry; {@code ConnectionLoss}/{@code
 * SessionExpired} → retry, idempotent via effective-state re-read so a retry cannot regress state
 * (D4/AC#5); {@code NoNode} → lazy-create the {@code state/shards/<shard>/} skeleton, then retry
 * (D17).
 *
 * <p>This class does the per-shard delta-plane work only. It never writes live state into
 * {@code state.json}; structure writes stay in {@code enqueueStructureChange}.
 */
public class StatePlaneWriter {

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /** ACTIVE short state — a demoted (deposed) leader becomes ACTIVE (D14). */
    public static final int ACTIVE = 2;
    /** LEADER raw short state on the wire (preserved; LEADER→ACTIVE collapse is read-side only). */
    public static final int LEADER = 1;

    /**
     * Hard safety cap on ring depth (a blind-trim backstop). Real compaction (snapshot fold, PR-5)
     * is driven by {@link StateDeltaConfig#compactAfterCount()} and normally fires first; this cap
     * only bounds the ring if compaction is misconfigured ({@code compactAfterCount > ringCap}).
     */
    public static final String RING_CAP_SYSPROP = "solr.statePlane.ringCap";
    private static final int DEFAULT_RING_CAP = 64;

    /** Max number of CAS attempts before giving up a single publish. */
    private static final int MAX_CAS_ATTEMPTS = 50;

    /**
     * Writer-side fence (D4). {@code writerId} is the overseer election id; it is a writer-side
     * fence + diagnostics ONLY. Readers must never validate it.
     */
    public interface ElectionFence {
        /** True iff this node is still the elected overseer right now (re-checked immediately before CAS). */
        boolean stillElected();

        /** This writer's election id, stamped into {@link ShardStateLog#writerId} for diagnostics. */
        String writerId();

        /**
         * True iff a ring already owned by {@code ringWriterId} outranks this writer (a newer
         * election) and this writer must therefore NOT append — it has been fenced out (D4).
         * {@code null}/equal ring writerId is never fencing.
         */
        boolean isFencedBy(String ringWriterId);
    }

    /** Thrown when this writer is no longer elected or has been fenced out by a newer election (D4). */
    public static class FencedException extends RuntimeException {
        public FencedException(String message) {
            super(message);
        }
    }

    private final SolrZkClient zkClient;
    private final ElectionFence fence;
    private final int ringCap;
    /** PR-5 compaction trigger: fold a shard's prefix into its snapshot at this live-delta count. */
    private final int compactAfterCount;
    /** PR-5 time-based compaction trigger (millis); {@code 0} disables. */
    private final long compactAfterMillis;

    /** Per-(coll,shard) serialization locks (D6) — two appends to the same shard never race. */
    private final ConcurrentHashMap<String, ReentrantLock> shardLocks = new ConcurrentHashMap<>();

    /** Per-(coll,shard) last-compaction wall clock (millis) for the time-based trigger. */
    private final ConcurrentHashMap<String, Long> lastCompactionMillis = new ConcurrentHashMap<>();

    /** Local epoch cursor; rebased up when a ring is seen at a higher epoch. */
    private volatile int localEpoch = 0;

    public StatePlaneWriter(SolrZkClient zkClient, ElectionFence fence) {
        this.zkClient = zkClient;
        this.fence = fence;
        this.ringCap = Integer.getInteger(RING_CAP_SYSPROP, DEFAULT_RING_CAP);
        this.compactAfterCount = StateDeltaConfig.compactAfterCount();
        this.compactAfterMillis = StateDeltaConfig.compactAfterMillis();
    }

    /** Allows the takeover path to seed the local epoch cursor (e.g. after an overseer bump). */
    public void setLocalEpoch(int epoch) {
        if (epoch > localEpoch) localEpoch = epoch;
    }

    public int getLocalEpoch() {
        return localEpoch;
    }

    /**
     * Publish replica state for one shard. Synchronous and serialized per (coll,shard).
     *
     * @param coll       collection name
     * @param shard      shard (slice) name
     * @param promotions (replicaId → shortState) entries to apply; e.g. a single transition, or a
     *                   batched set for a node-down chokepoint (D7). May be empty.
     * @param demotions  explicit replica ids to demote to ACTIVE(2) (e.g. node-down handling); the
     *                   stale-leader demotion for a LEADER promotion is computed automatically (D5).
     * @return true if a delta was appended; false if the publish was an idempotent no-op.
     */
    public boolean publish(String coll, String shard, List<StateDelta.Entry> promotions,
                           List<Integer> demotions) {
        final String collPath = StatePlanePaths.collectionPath(coll);
        final String deltaPath = StatePlanePaths.shardDeltas(collPath, shard);
        final List<StateDelta.Entry> promos =
                promotions == null ? Collections.emptyList() : promotions;
        final List<Integer> explicitDemotions =
                demotions == null ? Collections.emptyList() : demotions;

        final ReentrantLock lock = shardLocks.computeIfAbsent(coll + "/" + shard, k -> new ReentrantLock());
        lock.lock();
        try {
            for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
                try {
                    Stat stat = new Stat();
                    byte[] data;
                    try {
                        data = zkClient.getData(deltaPath, null, stat, true);
                    } catch (KeeperException.NoNodeException nne) {
                        // D17: lazy-create the skeleton + empty ring, then retry.
                        lazyCreateRing(collPath, shard);
                        continue;
                    }

                    ShardStateLog ring = StateDeltaCodec.decodeShardStateLog(data);

                    // ---- Fence (D4): writerId/election-id, NOT epoch. ----
                    if (!fence.stillElected()) {
                        throw new FencedException(
                                "no longer elected overseer; refusing to append " + deltaPath);
                    }
                    if (ring.writerId != null && fence.isFencedBy(ring.writerId)) {
                        throw new FencedException("ring " + deltaPath + " owned by newer election '"
                                + ring.writerId + "'; fenced out");
                    }

                    // ---- Rebase epoch cursor if the ring is ahead. epoch never bumps per-write. ----
                    int epoch = Math.max(localEpoch, ring.epoch);
                    if (ring.epoch > localEpoch) localEpoch = ring.epoch;

                    // ---- Effective state = snapshot baseline + ring deltas. ----
                    Map<Integer, Integer> effective = effectiveState(coll, collPath, shard, ring);

                    // ---- Compute pending demotions FIRST (D5). ----
                    List<Integer> pendingDemotions = computePendingDemotions(
                            effective, promos, explicitDemotions);

                    // ---- No-op ONLY when promotions change nothing AND no pending demotions. ----
                    if (pendingDemotions.isEmpty() && isStateNoop(effective, promos)) {
                        if (log.isDebugEnabled()) {
                            log.debug("publish no-op (idempotent) for {}/{}: {}", coll, shard, promos);
                        }
                        return false;
                    }

                    // ---- Append one delta (seq = lastSeq + 1, D2; demoted→ACTIVE, D14). ----
                    long seq = ring.lastSeq + 1;
                    StateDelta delta = new StateDelta(epoch, seq, promos, pendingDemotions, ACTIVE);

                    List<StateDelta> newEntries = new ArrayList<>(ring.entries);
                    newEntries.add(delta);
                    long newBaseSeq = ring.baseSeq;
                    // Hard-safety BACKSTOP only — PR-5 compaction (below, post-append) folds the prefix
                    // into the snapshot well before ringCap is reached. This fires solely on a
                    // misconfiguration where compactAfterCount > ringCap. SAFETY (finding #8): a reader
                    // past the cap rebases from the snapshot, so baseSeq must NEVER advance past a delta
                    // the snapshot does not durably cover — that would leave an unrecoverable gap between
                    // snapshot.upToSeq and ring.baseSeq. So fold the committed ring into the snapshot
                    // FIRST (durable), then clamp the trim to the covered seq; if the fold cannot be made
                    // durable, retain ALL deltas (no trim) — fail-safe, bounded by the misconfiguration.
                    if (newEntries.size() > ringCap) {
                        int intendedDrop = newEntries.size() - ringCap;
                        long intendedBaseSeq = newEntries.get(intendedDrop - 1).seq;
                        long coveredUpToSeq = foldCommittedRingIntoSnapshot(collPath, coll, shard, ring);
                        // Never past the snapshot coverage, never past the intended trim point.
                        long safeBaseSeq = Math.min(intendedBaseSeq, Math.max(ring.baseSeq, coveredUpToSeq));
                        int safeDrop = 0;
                        while (safeDrop < intendedDrop && newEntries.get(safeDrop).seq <= safeBaseSeq) {
                            safeDrop++;
                        }
                        if (safeDrop > 0) {
                            newBaseSeq = newEntries.get(safeDrop - 1).seq;
                            newEntries = new ArrayList<>(newEntries.subList(safeDrop, newEntries.size()));
                            log.warn("ring {} exceeded hard cap {} (compactAfterCount {} should be <= ringCap); "
                                            + "safely trimmed {} snapshot-covered entries (baseSeq->{}).",
                                    deltaPath, ringCap, compactAfterCount, safeDrop, newBaseSeq);
                        } else {
                            log.warn("ring {} exceeded hard cap {} but the snapshot covers no droppable "
                                            + "prefix (coveredUpToSeq={}); retaining all {} deltas to preserve "
                                            + "reconstruct-ability.",
                                    deltaPath, ringCap, coveredUpToSeq, newEntries.size());
                        }
                    }

                    ShardStateLog newRing = new ShardStateLog(
                            epoch, newBaseSeq, seq, fence.writerId(), newEntries);
                    byte[] out = StateDeltaCodec.encodeShardStateLog(newRing);

                    // ---- CAS: setData with expectedVersion. retryOnConnLoss=false so we own the retry. ----
                    zkClient.setData(deltaPath, out, stat.getVersion(), false);

                    if (log.isDebugEnabled()) {
                        log.debug("publish appended delta for {}/{}: epoch={} seq={} promos={} demotions={} baseSeq={} entries={} ver={}->{}",
                                coll, shard, epoch, seq, promos, pendingDemotions, newBaseSeq, newEntries.size(),
                                stat.getVersion(), stat.getVersion() + 1);
                    }

                    // ---- PR-5: compaction (snapshot fold) — snapshot durable FIRST, THEN CAS-trim
                    //      the ring (D2/D3). Runs under the per-shard lock so it never races this
                    //      writer's own next publish; cross-writer safety is the CAS + fence. ----
                    maybeCompact(collPath, coll, shard, newEntries.size());
                    return true;

                } catch (KeeperException.BadVersionException bve) {
                    // Lost the CAS race — re-read and retry. Effective-state re-read makes it idempotent.
                    if (log.isDebugEnabled()) log.debug("BadVersion on {}; retry {}", deltaPath, attempt);
                } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException ce) {
                    // Reconnect+retry. The post-write re-read of effective state cannot regress (AC#5):
                    // if the write actually landed, the retry sees it already present and no-ops.
                    if (log.isDebugEnabled()) log.debug("ConnLoss/SessionExpired on {}; retry {}", deltaPath, attempt, ce);
                } catch (FencedException fe) {
                    throw fe;
                } catch (KeeperException | InterruptedException | IOException e) {
                    if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                            "StatePlaneWriter.publish failed for " + deltaPath, e);
                }
            }
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                    "StatePlaneWriter.publish exhausted " + MAX_CAS_ATTEMPTS + " CAS attempts for " + deltaPath);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reconstruct the current effective (replicaId → shortState) for a shard from the per-shard
     * snapshot baseline + ring deltas. A missing snapshot is treated as empty.
     */
    private Map<Integer, Integer> effectiveState(String coll, String collPath, String shard, ShardStateLog ring)
            throws KeeperException, InterruptedException, IOException {
        StateSnapshot snapshot = readSnapshot(collPath, shard);
        if (snapshot == null) {
            snapshot = new StateSnapshot(-1L, coll, ring.epoch, shard, ring.baseSeq, Collections.emptyMap());
        }
        return new LinkedHashMap<>(snapshot.reconstruct(ring.entries));
    }

    /**
     * Decide whether to compact this shard and, if so, fold its committed delta prefix into the
     * per-shard snapshot. Compaction is an optimization: any failure leaves state fully intact and is
     * retried on the next publish (snapshot-first ordering means a partial fold loses NO state).
     */
    private void maybeCompact(String collPath, String coll, String shard, int liveDeltaCount) {
        if (!shouldCompact(coll, shard, liveDeltaCount)) {
            return;
        }
        compactShard(collPath, coll, shard);
    }

    /** True when the live delta count hit {@link #compactAfterCount} OR the time trigger elapsed. */
    private boolean shouldCompact(String coll, String shard, int liveDeltaCount) {
        if (liveDeltaCount <= 0) {
            return false;
        }
        if (compactAfterCount > 0 && liveDeltaCount >= compactAfterCount) {
            return true;
        }
        if (compactAfterMillis > 0) {
            String key = coll + "/" + shard;
            Long last = lastCompactionMillis.get(key);
            long now = System.currentTimeMillis();
            if (last == null) {
                // First observation — start the timer now (don't compact a brand-new shard immediately).
                lastCompactionMillis.putIfAbsent(key, now);
                return false;
            }
            return now - last >= compactAfterMillis;
        }
        return false;
    }

    /**
     * Fold the shard's <b>already-committed</b> delta ring into its snapshot, then CAS-trim the ring.
     *
     * <p>Ordering (D3): snapshot durable FIRST (single atomic op), THEN the ring CAS that removes the
     * folded deltas. {@code upToSeq = ring.lastSeq}: every delta currently in the ring has
     * {@code seq <= lastSeq} and is captured by the fold; any concurrent {@link #publish} append lands
     * at {@code seq > lastSeq}, so it is NOT folded — it survives and wins. The ring CAS is guarded by
     * {@code stat.getVersion()}: if a concurrent append bumped the version the CAS fails and we retry
     * (the new delta is never dropped). A crash between the snapshot write and the ring CAS leaves the
     * folded deltas in place with {@code seq <= upToSeq}, so {@link StateSnapshot#reconstruct} skips
     * them as stale — idempotent, no state loss.
     */
    private void compactShard(String collPath, String coll, String shard) {
        final String deltaPath = StatePlanePaths.shardDeltas(collPath, shard);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            try {
                Stat stat = new Stat();
                byte[] data;
                try {
                    data = zkClient.getData(deltaPath, null, stat, true);
                } catch (KeeperException.NoNodeException nne) {
                    return; // nothing to compact
                }
                ShardStateLog ring = StateDeltaCodec.decodeShardStateLog(data);
                if (ring.entries.isEmpty()) {
                    return; // already compacted (or nothing to fold)
                }

                // Fence (D4): only the elected, un-fenced writer may compact.
                if (!fence.stillElected()) {
                    throw new FencedException("no longer elected; refusing to compact " + deltaPath);
                }
                if (ring.writerId != null && fence.isFencedBy(ring.writerId)) {
                    throw new FencedException(
                            "ring " + deltaPath + " owned by newer election; refusing to compact");
                }

                long upToSeq = ring.lastSeq;
                StateSnapshot oldSnap = readSnapshot(collPath, shard);
                if (oldSnap == null) {
                    oldSnap = new StateSnapshot(-1L, coll, ring.epoch, shard, ring.baseSeq,
                            Collections.emptyMap());
                }
                Map<Integer, Integer> folded = oldSnap.reconstruct(ring.entries);
                StateSnapshot newSnap = new StateSnapshot(
                        oldSnap.collectionId, coll, ring.epoch, shard, upToSeq, folded);

                // (1) snapshot durable FIRST — single atomic op.
                writeOrCreate(StatePlanePaths.shardSnapshot(collPath, shard),
                        StateDeltaCodec.encodeStateSnapshot(newSnap));

                // (2) THEN CAS-trim the ring: baseSeq advances to upToSeq, folded deltas removed.
                ShardStateLog trimmed = new ShardStateLog(
                        ring.epoch, upToSeq, ring.lastSeq, fence.writerId(), Collections.emptyList());
                zkClient.setData(deltaPath, StateDeltaCodec.encodeShardStateLog(trimmed),
                        stat.getVersion(), false);

                lastCompactionMillis.put(coll + "/" + shard, System.currentTimeMillis());
                if (log.isDebugEnabled()) {
                    log.debug("statePlane compacted {} up to seq {} ({} deltas folded into snapshot)",
                            deltaPath, upToSeq, ring.entries.size());
                }
                return;

            } catch (KeeperException.BadVersionException bve) {
                // Concurrent append between our getData and CAS — retry. The new delta has
                // seq > upToSeq, so it survives the next fold; nothing is lost.
                if (log.isDebugEnabled()) log.debug("compaction BadVersion on {}; retry {}", deltaPath, attempt);
            } catch (KeeperException.ConnectionLossException | KeeperException.SessionExpiredException ce) {
                if (log.isDebugEnabled()) log.debug("compaction ConnLoss on {}; retry {}", deltaPath, attempt, ce);
            } catch (FencedException fe) {
                throw fe;
            } catch (KeeperException | InterruptedException | IOException e) {
                if (e instanceof InterruptedException) Thread.currentThread().interrupt();
                // Compaction is best-effort: never fail the publish for it. State is intact (snapshot
                // is written before any delta delete); the next publish retries compaction.
                log.warn("statePlane compaction failed for {} (state intact; retried on next publish)",
                        deltaPath, e);
                return;
            }
        }
        log.warn("statePlane compaction exhausted {} CAS attempts for {}", MAX_CAS_ATTEMPTS, deltaPath);
    }

    private StateSnapshot readSnapshot(String collPath, String shard)
            throws KeeperException, InterruptedException, IOException {
        String snapPath = StatePlanePaths.shardSnapshot(collPath, shard);
        try {
            byte[] data = zkClient.getData(snapPath, null, new Stat(), true);
            return StateDeltaCodec.decodeStateSnapshot(data);
        } catch (KeeperException.NoNodeException nne) {
            return null;
        }
    }

    /**
     * Durably fold the given <b>committed</b> ring's deltas into the shard snapshot and return the
     * highest seq the snapshot now covers ({@code ring.lastSeq}), or {@code -1} if the fold could not
     * be made durable. Unlike {@link #compactShard}, this does NOT trim the ring — the caller (the
     * hard-cap path in {@link #publish}) owns the single ring write, and must not advance baseSeq past
     * a delta the snapshot does not cover (finding #8). Best-effort: any ZK failure returns -1 so the
     * caller retains all deltas rather than risk an unrecoverable gap.
     *
     * <p>{@code protected} as a test seam: a subclass returning {@code -1} exercises the fail-safe
     * (no-trim) branch deterministically without inducing a ZK fault.
     */
    protected long foldCommittedRingIntoSnapshot(String collPath, String coll, String shard, ShardStateLog ring) {
        try {
            StateSnapshot oldSnap = readSnapshot(collPath, shard);
            if (oldSnap == null) {
                oldSnap = new StateSnapshot(-1L, coll, ring.epoch, shard, ring.baseSeq, Collections.emptyMap());
            }
            long upToSeq = ring.lastSeq;
            Map<Integer, Integer> folded = oldSnap.reconstruct(ring.entries);
            StateSnapshot newSnap = new StateSnapshot(
                    oldSnap.collectionId, coll, ring.epoch, shard, upToSeq, folded);
            // Snapshot durable FIRST (single atomic op), exactly as compactShard does, so the caller's
            // subsequent ring trim can never expose a gap even on a crash between the two writes.
            writeOrCreate(StatePlanePaths.shardSnapshot(collPath, shard),
                    StateDeltaCodec.encodeStateSnapshot(newSnap));
            return upToSeq;
        } catch (KeeperException | InterruptedException | IOException e) {
            if (e instanceof InterruptedException) Thread.currentThread().interrupt();
            log.warn("hard-cap snapshot fold failed for {}/{}; retaining un-snapshotted deltas (no trim)",
                    coll, shard, e);
            return -1L;
        }
    }

    /**
     * Pending demotions = explicit demotions (whose effective state is not already ACTIVE) UNION
     * stale leaders displaced by a LEADER promotion in this delta (D5). A re-publish of LEADER while
     * a stale LEADER lingers still emits the demotion — never a two-leaders no-op.
     */
    private List<Integer> computePendingDemotions(Map<Integer, Integer> effective,
                                                  List<StateDelta.Entry> promotions,
                                                  List<Integer> explicitDemotions) {
        // Use a set-like ordered structure to dedup while preserving order.
        LinkedHashMap<Integer, Boolean> demoted = new LinkedHashMap<>();
        for (Integer id : explicitDemotions) {
            Integer cur = effective.get(id);
            if (cur == null || cur != ACTIVE) demoted.put(id, Boolean.TRUE);
        }
        for (StateDelta.Entry promo : promotions) {
            if (promo.shortState != LEADER) continue;
            // Demote any OTHER replica in this shard currently at LEADER (raw==1).
            for (Map.Entry<Integer, Integer> e : effective.entrySet()) {
                if (e.getKey() == promo.replicaId) continue;
                if (e.getValue() != null && e.getValue() == LEADER) {
                    demoted.put(e.getKey(), Boolean.TRUE);
                }
            }
        }
        // A replica being promoted in this same delta is never simultaneously demoted.
        for (StateDelta.Entry promo : promotions) {
            demoted.remove(promo.replicaId);
        }
        return new ArrayList<>(demoted.keySet());
    }

    /** True iff every promotion already matches the effective state (nothing would change). */
    private boolean isStateNoop(Map<Integer, Integer> effective, List<StateDelta.Entry> promotions) {
        if (promotions.isEmpty()) return true;
        for (StateDelta.Entry promo : promotions) {
            Integer cur = effective.get(promo.replicaId);
            if (cur == null || cur != promo.shortState) return false;
        }
        return true;
    }

    /**
     * Lazy-create the {@code state/shards/<shard>/deltas} skeleton with an empty ring (D17).
     * Idempotent — concurrent creators tolerate NodeExists.
     */
    private void lazyCreateRing(String collPath, String shard)
            throws KeeperException, InterruptedException, IOException {
        String deltaPath = StatePlanePaths.shardDeltas(collPath, shard);
        ShardStateLog empty = new ShardStateLog(localEpoch, 0L, 0L, fence.writerId(),
                Collections.emptyList());
        byte[] bytes = StateDeltaCodec.encodeShardStateLog(empty);
        try {
            // makePath creates intermediate nodes (state, shards, shards/<shard>) and the deltas leaf.
            zkClient.makePath(deltaPath, bytes, CreateMode.PERSISTENT, true);
        } catch (KeeperException.NodeExistsException nee) {
            // Another writer raced us; fine.
        }
    }

    // ---- Seed (overseer takeover / collection bring-up). ----

    /**
     * Seed a shard's delta plane from a known state map (e.g. rebuilt on overseer takeover, or an
     * empty seed at collection create). Write order is <b>snapshot → deltas(empty) → manifest LAST</b>;
     * {@code manifest.seeded==true} is the reader's switch onto the delta plane and is published last.
     */
    public void seedShard(String coll, String shard, int epoch, Map<Integer, Integer> states)
            throws KeeperException, InterruptedException, IOException {
        String collPath = StatePlanePaths.collectionPath(coll);
        setLocalEpoch(epoch);
        // 1) snapshot (carry the collection name as identity; numeric id unknown at seed → -1)
        StateSnapshot snap = new StateSnapshot(-1L, coll, epoch, shard, 0L,
                states == null ? Collections.emptyMap() : states);
        writeOrCreate(StatePlanePaths.shardSnapshot(collPath, shard),
                StateDeltaCodec.encodeStateSnapshot(snap));
        // 2) deltas (empty)
        ShardStateLog empty = new ShardStateLog(epoch, 0L, 0L, fence.writerId(), Collections.emptyList());
        writeOrCreate(StatePlanePaths.shardDeltas(collPath, shard),
                StateDeltaCodec.encodeShardStateLog(empty));
    }

    /**
     * Publish the manifest LAST (the reader's switch onto the delta plane). Callers invoke this after
     * every shard of a collection has been seeded via {@link #seedShard}.
     */
    public void writeManifestSeeded(String coll, int epoch, List<String> shardHints)
            throws KeeperException, InterruptedException, IOException {
        String collPath = StatePlanePaths.collectionPath(coll);
        List<Object> manifest = new ArrayList<>(3);
        manifest.add(epoch);
        manifest.add(Boolean.TRUE); // seeded — published LAST, the reader's switch onto the delta plane
        manifest.add(shardHints == null ? Collections.emptyList() : new ArrayList<>(shardHints));
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        new org.apache.solr.common.util.JavaBinCodec()
                .marshal(manifest, new org.apache.solr.common.util.FastOutputStream(baos));
        writeOrCreate(StatePlanePaths.manifest(collPath), baos.toByteArray());
    }

    private void writeOrCreate(String path, byte[] bytes)
            throws KeeperException, InterruptedException {
        try {
            zkClient.setData(path, bytes, -1, true);
        } catch (KeeperException.NoNodeException nne) {
            try {
                zkClient.makePath(path, bytes, CreateMode.PERSISTENT, true);
            } catch (KeeperException.NodeExistsException nee) {
                // raced; overwrite
                zkClient.setData(path, bytes, -1, true);
            }
        }
    }
}
