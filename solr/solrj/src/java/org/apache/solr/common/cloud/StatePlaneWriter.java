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
import org.apache.solr.common.util.Utils;
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
 *   <li>reject the append if {@code ring.epoch > localEpoch};
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

        /**
         * ZK-authoritative ownership check (review P1 #5): true iff this writer still owns the overseer
         * election in ZooKeeper right now. Stronger than {@link #stillElected()} (a local liveness flag):
         * it rejects a stale writer that lost leadership but is not yet marked closed, even when the new
         * owner has not yet written this shard's ring (so {@code ring.writerId} cannot reveal the
         * staleness). Default delegates to {@link #stillElected()} for fences with no ZK election
         * (e.g. unit-test fences).
         */
        default boolean ownsElectionAuthoritative() {
            return stillElected();
        }
    }

    /** Thrown when this writer is no longer elected or has been fenced out by a newer election (D4). */
    public static class FencedException extends RuntimeException {
        public FencedException(String message) {
            super(message);
        }
    }

    /**
     * Writer-local cached effective state for one shard (review P1 #3). The hot write path used to
     * decode the per-shard snapshot and {@code reconstruct} the WHOLE replica map on every publish —
     * O(replicas-in-shard) per single-replica transition. Instead the elected writer (the only one that
     * may append, per the per-shard lock + election fence) keeps the effective map in memory and updates
     * it incrementally as it appends. The cache key includes the collection incarnation id when the
     * caller has one; {@code lastSeq} alone is not incarnation-safe because a same-name recreation can
     * start a fresh ring at the same sequence. {@code lastSeq} is the ring {@code lastSeq} this map
     * reflects; the cache is reused only when the freshly-read ring still has that {@code lastSeq} for
     * the same incarnation — otherwise (first publish on this shard after takeover, a collection
     * recreation, or a concurrent append seen as a changed lastSeq) it is rebuilt from snapshot + ring
     * exactly as before. {@code currentLeaderId} is the O(1) stale-leader-demotion index (the single
     * replica at raw {@code LEADER}, or {@code null}). All fields are read/written only under the owning
     * shard's lock.
     */
    private static final class ShardEffective {
        long lastSeq;
        final Map<Integer, Integer> states;
        Integer currentLeaderId;
        ShardEffective(long lastSeq, Map<Integer, Integer> states, Integer currentLeaderId) {
            this.lastSeq = lastSeq;
            this.states = states;
            this.currentLeaderId = currentLeaderId;
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

    /**
     * Per-(coll,shard) writer-local effective-state cache (review P1 #3). Lazily built on first publish
     * to a shard (or after a detected divergence) and advanced incrementally per append, so steady-state
     * publishes skip the snapshot decode + full-map reconstruct. Bounded by the shards this overseer
     * incarnation publishes to; rebuilt fresh on takeover (a new overseer constructs a new writer).
     * Keys are collection-incarnation scoped when the caller has a collection id, so a same-name
     * collection recreation cannot reuse the prior incarnation's effective state when its fresh ring
     * restarts at the same sequence number.
     */
    private final ConcurrentHashMap<String, ShardEffective> effectiveCache = new ConcurrentHashMap<>();

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

    private static String cacheKey(String coll, String shard, String collectionIncarnation) {
        String incarnation =
                (collectionIncarnation == null || collectionIncarnation.isEmpty())
                        ? coll
                        : collectionIncarnation;
        return coll + "#" + incarnation + "/" + shard;
    }

    private static boolean collectionKey(String key, String coll) {
        return key.startsWith(coll + "#") || key.startsWith(coll + "/");
    }

    /** Drop writer-local cache/timer state for a removed collection name. */
    public void clearCollection(String coll) {
        effectiveCache.keySet().removeIf(key -> collectionKey(key, coll));
        lastCompactionMillis.keySet().removeIf(key -> collectionKey(key, coll));
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
    /**
     * Publish replica state for one shard with a caller-supplied collection incarnation id.
     */
    public boolean publish(String coll, String shard, String collectionIncarnation,
                           List<StateDelta.Entry> promotions, List<Integer> demotions) {
        if (collectionIncarnation == null) {
            throw new IllegalArgumentException("collectionIncarnation is required for state-plane publish");
        }
        final String collPath = StatePlanePaths.collectionPath(coll);
        final String deltaPath = StatePlanePaths.shardDeltas(collPath, shard);
        final List<StateDelta.Entry> promos =
                promotions == null ? Collections.emptyList() : promotions;
        final List<Integer> explicitDemotions =
                demotions == null ? Collections.emptyList() : demotions;

        final ReentrantLock lock = shardLocks.computeIfAbsent(coll + "/" + shard, k -> new ReentrantLock());
        lock.lock();
        try {
            // Authoritative election fence (review P1 #5): confirm we still OWN the overseer election in
            // ZK before doing publish work. The per-attempt fence.stillElected() below is only a local
            // liveness flag, and ring.writerId fencing cannot reveal a stale writer when the new overseer
            // has not yet written THIS shard's ring. The authoritative check rejects a stale overseer
            // that lost leadership but is not yet marked closed; the check is repeated immediately
            // before each mutating CAS below.
            if (!fence.ownsElectionAuthoritative()) {
                throw new FencedException("no longer the elected overseer (authoritative); refusing to append "
                        + deltaPath);
            }
            for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
                try {
                    Stat stat = new Stat();
                    byte[] data;
                    try {
                        data = zkClient.getData(deltaPath, null, stat, true);
                    } catch (KeeperException.NoNodeException nne) {
                        // D17: lazy-create the skeleton + empty ring, then retry.
                        lazyCreateRing(collPath, shard, collectionIncarnation);
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
                    if (ring.epoch > localEpoch) {
                        throw new FencedException(
                                "ring " + deltaPath + " is at epoch " + ring.epoch
                                        + " ahead of local writer epoch " + localEpoch + "; refusing to append");
                    }

                    int epoch = Math.max(localEpoch, ring.epoch);
                    if (ring.epoch > localEpoch) localEpoch = ring.epoch;

                    // ---- Effective state (review P1 #3): served from the writer-local per-shard cache so
                    //      a steady-state publish does NOT decode the snapshot and reconstruct the whole
                    //      shard map every time. Valid iff it reflects the same collection incarnation
                    //      AND the ring we just read (ce.lastSeq == ring.lastSeq); otherwise (first
                    //      publish on this shard after takeover, same-name collection recreation, or a
                    //      concurrent append detected via a changed lastSeq) rebuild it from snapshot +
                    //      ring exactly as before. Held under the per-shard lock. ----
                    final String cacheKey = cacheKey(coll, shard, collectionIncarnation);
                    ShardEffective ce = effectiveCache.get(cacheKey);
                    if (ce == null || ce.lastSeq != ring.lastSeq) {
                        Map<Integer, Integer> rebuilt = effectiveState(coll, collPath, shard, ring);
                        ce = new ShardEffective(ring.lastSeq, rebuilt, findLeader(coll, shard, rebuilt));
                        effectiveCache.put(cacheKey, ce);
                    }
                    final Map<Integer, Integer> effective = ce.states;

                    // ---- Compute pending demotions FIRST (D5). Stale-leader demotion uses the O(1)
                    //      per-shard current-leader index, not a full-map scan (review P1 #3). ----
                    List<Integer> pendingDemotions = computePendingDemotions(
                            effective, ce.currentLeaderId, promos, explicitDemotions);

                    // ---- No-op ONLY when promotions change nothing AND no pending demotions. ----
                    if (pendingDemotions.isEmpty() && isStateNoop(effective, promos)) {
                        if (log.isDebugEnabled()) {
                            log.debug("publish no-op (idempotent) for {}/{}: {}", coll, shard, promos);
                        }
                        return false;
                    }

                    // ---- Append one delta (seq = lastSeq + 1, D2; demoted→ACTIVE, D14). ----
                    long seq = ring.lastSeq + 1;
                    StateDelta delta = new StateDelta(collectionIncarnation, shard, epoch, seq, promos, pendingDemotions, ACTIVE);

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
                            ring.collectionId != null ? ring.collectionId : collectionIncarnation,
                            ring.shardId != null ? ring.shardId : shard,
                            epoch, newBaseSeq, seq, fence.writerId(), newEntries);
                    byte[] out = StateDeltaCodec.encodeShardStateLog(newRing);

                    // ---- CAS: setData with expectedVersion. retryOnConnLoss=false so we own the retry. ----
                    // Re-check authoritative overseer ownership immediately before the mutating CAS. The
          // pre-loop check bounds ordinary work, but leadership may transfer after the ring read and
          // before setData; in that window ring.writerId may still name this writer until the new
          // overseer publishes to this shard. Do not append from that stale window.
          if (!fence.ownsElectionAuthoritative()) {
            throw new FencedException(
                "no longer the elected overseer (authoritative); refusing to append " + deltaPath);
          }
          zkClient.setData(deltaPath, out, stat.getVersion(), false);

                    // Append succeeded — advance the writer-local effective cache incrementally (review
                    // P1 #3) so the next publish reuses it with no snapshot read or full reconstruct.
                    // Compaction (below) preserves both lastSeq and effective state, so the cache stays
                    // valid across it. Demotions land as ACTIVE; promotions overwrite; the LEADER index is
                    // re-derived from the applied changes (single-leader-per-shard invariant).
                    for (Integer demoteId : pendingDemotions) {
                        ce.states.put(demoteId, ACTIVE);
                    }
                    for (StateDelta.Entry promo : promos) {
                        ce.states.put(promo.replicaId, promo.shortState);
                        if (promo.shortState == LEADER) ce.currentLeaderId = promo.replicaId;
                    }
                    if (ce.currentLeaderId != null) {
                        Integer ls = ce.states.get(ce.currentLeaderId);
                        if (ls == null || ls != LEADER) ce.currentLeaderId = null;
                    }
                    ce.lastSeq = seq;

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
            snapshot = new StateSnapshot(Long.parseLong(ring.collectionId), coll, ring.epoch, shard,
                    ring.baseSeq, Collections.emptyMap());
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
                    oldSnap = new StateSnapshot(Long.parseLong(ring.collectionId), coll, ring.epoch, shard, ring.baseSeq,
                            Collections.emptyMap());
                }
                Map<Integer, Integer> folded = oldSnap.reconstruct(ring.entries);
                StateSnapshot newSnap = new StateSnapshot(
                        oldSnap.collectionId, coll, ring.epoch, shard, upToSeq, folded);

                // (1) snapshot durable FIRST — single atomic op, monotonic (never regresses a newer
                //     writer's higher-coverage snapshot; see writeSnapshotMonotonic).
                writeSnapshotMonotonic(collPath, shard, newSnap);

                // (2) THEN CAS-trim the ring: baseSeq advances to upToSeq, folded deltas removed.
                ShardStateLog trimmed = new ShardStateLog(
                        ring.collectionId, ring.shardId, ring.epoch, upToSeq, ring.lastSeq,
                        fence.writerId(), Collections.emptyList());
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

    /**
     * Writes a shard snapshot only when it advances durable coverage. The snapshot is the
     * reconstruction base for the bounded CAS ring, so regressive stale-writer overwrites must
     * be rejected even if a later ring CAS would fail.
     */
    private void writeSnapshotMonotonic(String collPath, String shard, StateSnapshot newSnap)
            throws KeeperException, InterruptedException, IOException {
        String snapPath = StatePlanePaths.shardSnapshot(collPath, shard);
        byte[] newBytes = StateDeltaCodec.encodeStateSnapshot(newSnap);

        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            Stat stat = new Stat();
            try {
                byte[] currentBytes = zkClient.getData(snapPath, null, stat, true);
                StateSnapshot current = StateDeltaCodec.decodeStateSnapshot(currentBytes);
                if (current != null && current.upToSeq() >= newSnap.upToSeq()) {
                    return;
                }
                zkClient.setData(snapPath, newBytes, stat.getVersion(), true);
                return;
            } catch (KeeperException.NoNodeException nne) {
                try {
                    zkClient.makePath(snapPath, newBytes, CreateMode.PERSISTENT, true);
                    return;
                } catch (KeeperException.NodeExistsException exists) {
                    // Lost the create race; loop and apply the monotonic version check.
                }
            } catch (KeeperException.BadVersionException bve) {
                // Concurrent snapshot writer; loop and re-check coverage.
            }
        }

        throw new KeeperException.BadVersionException(snapPath);
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
                oldSnap = new StateSnapshot(Long.parseLong(ring.collectionId), coll, ring.epoch, shard, ring.baseSeq,
                        Collections.emptyMap());
            }
            long upToSeq = ring.lastSeq;
            Map<Integer, Integer> folded = oldSnap.reconstruct(ring.entries);
            StateSnapshot newSnap = new StateSnapshot(
                    oldSnap.collectionId, coll, ring.epoch, shard, upToSeq, folded);
            // Snapshot durable FIRST (single atomic op), exactly as compactShard does, so the caller's
            // subsequent ring trim can never expose a gap even on a crash between the two writes. The
            // write is monotonic: if a newer writer already folded further, this skips and the snapshot
            // still covers >= upToSeq, so returning upToSeq remains a safe (conservative) lower bound.
            writeSnapshotMonotonic(collPath, shard, newSnap);
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
                                                  Integer currentLeaderId,
                                                  List<StateDelta.Entry> promotions,
                                                  List<Integer> explicitDemotions) {
        // Use a set-like ordered structure to dedup while preserving order.
        LinkedHashMap<Integer, Boolean> demoted = new LinkedHashMap<>();
        for (Integer id : explicitDemotions) {
            Integer cur = effective.get(id);
            if (cur == null || cur != ACTIVE) demoted.put(id, Boolean.TRUE);
        }
        // Stale-leader demotion (D5): if this delta promotes a LEADER, demote the shard's CURRENT leader
        // via the O(1) per-shard current-leader index (review P1 #3) instead of scanning the whole
        // effective map. The single-leader-per-shard invariant (enforced by this very demotion under the
        // per-shard lock + election fence) makes the index sufficient.
        boolean leaderPromo = false;
        for (StateDelta.Entry promo : promotions) {
            if (promo.shortState == LEADER) { leaderPromo = true; break; }
        }
        if (leaderPromo && currentLeaderId != null) {
            demoted.put(currentLeaderId, Boolean.TRUE);
        }
        // A replica being promoted in this same delta is never simultaneously demoted.
        for (StateDelta.Entry promo : promotions) {
            demoted.remove(promo.replicaId);
        }
        return new ArrayList<>(demoted.keySet());
    }

    /**
     * Find the single replica at raw {@code LEADER} in a freshly-reconstructed effective map (review
     * P1 #3 rebuild path). O(replicas-in-shard), but only on a cache miss (takeover / divergence), never
     * on the steady-state hot path. Warns if the single-leader-per-shard invariant is violated.
     */
    private Integer findLeader(String coll, String shard, Map<Integer, Integer> effective) {
        Integer leader = null;
        for (Map.Entry<Integer, Integer> e : effective.entrySet()) {
            if (e.getValue() != null && e.getValue() == LEADER) {
                if (leader != null) {
                    log.warn("statePlane {}/{}: more than one replica at LEADER in effective state "
                            + "({} and {}); using the latter as the current-leader index",
                            coll, shard, leader, e.getKey());
                }
                leader = e.getKey();
            }
        }
        return leader;
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
    private void lazyCreateRing(String collPath, String shard, String collectionIncarnation)
            throws KeeperException, InterruptedException, IOException {
        String deltaPath = StatePlanePaths.shardDeltas(collPath, shard);
        ShardStateLog empty = new ShardStateLog(collectionIncarnation, shard, localEpoch, 0L, 0L, fence.writerId(),
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
    /**
     * Publishes the state-plane manifest after all shard snapshots/rings are durable. The manifest is
     * intentionally small metadata: readers use its existence as the switch onto this fork's state
     * plane and discover the authoritative shard set from state.json.
     */
    public void writeManifestSeeded(String coll, int epoch, List<String> shards)
            throws KeeperException, InterruptedException, IOException {
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("epoch", epoch);
        manifest.put("seeded", Boolean.TRUE);
        manifest.put("shards", new ArrayList<>(shards));
        createIfAbsent(StatePlanePaths.manifest(StatePlanePaths.collectionPath(coll)), Utils.toJSON(manifest));
    }

    public void seedShard(String coll, String shard, String collectionIncarnation, int epoch, Map<Integer, Integer> states)
            throws IOException, KeeperException, InterruptedException {
        if (collectionIncarnation == null) {
            throw new IllegalArgumentException("collectionIncarnation is required for state-plane seed");
        }
        setLocalEpoch(epoch);
        String collPath = StatePlanePaths.collectionPath(coll);
        StateSnapshot snap = new StateSnapshot(Long.parseLong(collectionIncarnation), coll, epoch, shard, 0L, states);
        createIfAbsent(StatePlanePaths.shardSnapshot(collPath, shard), StateDeltaCodec.encodeStateSnapshot(snap));
        ShardStateLog empty = new ShardStateLog(collectionIncarnation, shard, epoch, 0L, 0L, fence.writerId(), Collections.emptyList());
        createIfAbsent(StatePlanePaths.shardDeltas(collPath, shard), StateDeltaCodec.encodeShardStateLog(empty));
    }

    private void createIfAbsent(String path, byte[] bytes)
            throws KeeperException, InterruptedException {
        try {
            zkClient.makePath(path, bytes, CreateMode.PERSISTENT, true);
        } catch (KeeperException.NodeExistsException nee) {
            if (log.isDebugEnabled()) {
                log.debug("seed: {} already exists; keeping existing state (no empty-baseline overwrite)", path);
            }
        }
    }
}
