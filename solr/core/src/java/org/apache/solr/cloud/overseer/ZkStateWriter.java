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
package org.apache.solr.cloud.overseer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;

import com.codahale.metrics.Meter;
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap;
import org.apache.solr.cloud.ActionThrottle;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.Stats;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cloud.LeaderElector;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.StateDelta;
import org.apache.solr.common.cloud.StatePlanePaths;
import org.apache.solr.common.cloud.StatePlaneWriter;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.singletonMap;


// TODO: live node listener to clear states
public class ZkStateWriter {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Meter stateUpdateWrites = Metrics.MARKS_METRICS.meter("zkstatewriter_stateupdates");
  private static final Meter structureWrites = Metrics.MARKS_METRICS.meter("zkstatewriter_structureupdates");

  private final ZkStateReader reader;
  private final Overseer overseer;

  /**
   * Represents a no-op {@link ZkWriteCommand} which will result in no modification to cluster state
   */

  protected volatile Stats stats;

  private final Map<Integer, Map<Integer,Integer>> stateUpdates = new ConcurrentHashMap<>(64);

  /**
   * collectionId -&gt; the set of replica internalIds whose state changed since the last delta-plane
   * publish for that collection (finding #1). Every path that mutates {@link #stateUpdates} records the
   * touched ids here; {@link #writeStateUpdatesInternal} drains the set and publishes a delta carrying
   * ONLY those replicas (plus writer-computed demotions) instead of the whole collection map. A drain
   * that finds no entry (e.g. {@link #stop()} / takeover republish) falls back to publishing the full
   * map, which is always safe — just larger.
   */
  private final Map<Integer, Set<Integer>> pendingChangedIds = new ConcurrentHashMap<>(64);

  /**
   * Bounded record of collection ids whose structure was REMOVED (review P0 #2, option 3). A replica
   * state update whose collection id is unknown is normally retained — its overseer queue item is held
   * for reprocess — on the assumption the structure simply has not landed yet. But if the collection
   * was deleted, that id can never resolve, so the update is dropped cleanly instead of poisoning the
   * queue. Collection ids are monotonic and never reused, so only the most recent removals need to be
   * remembered to tell "deleted" from "not yet landed". synchronizedSet: written under collLock in
   * {@link #removeCollection}, read from the taskZkWriterExecutor in {@link #writeStateUpdatesInternal}.
   */
  private static final int REMOVED_COLL_IDS_CAP =
      Integer.getInteger("solr.zkStateWriter.removedCollIdsCap", 8192);
  private final Set<Integer> removedCollIds = Collections.synchronizedSet(
      Collections.newSetFromMap(new java.util.LinkedHashMap<Integer, Boolean>(256, 0.75f, false) {
        @Override protected boolean removeEldestEntry(Map.Entry<Integer, Boolean> eldest) {
          return size() > REMOVED_COLL_IDS_CAP;
        }
      }));

  /**
   * Bounded backstop for review P0 #2: how many times a state update for an unknown collection id is
   * retained for reprocess until the collection id is proven removed (a stale item from a long-dead incarnation
   * this overseer never observed being removed, so it is in neither idToCollection nor removedCollIds).
   * Cleared the moment the id resolves (structure lands) or is removed. Self-bounding — entries are
   * removed on resolve/remove/drop.
   */
  private final Map<Integer, Integer> unknownIdAttempts = new ConcurrentHashMap<>();

//  Map<Long,List<ZkStateWriter.StateUpdate>> sliceStates = new ConcurrentHashMap<>();

  private final Map<Integer,String> idToCollection = new NonBlockingHashMap<>(64);

  private final Map<String,DocAssign> assignMap = new ConcurrentHashMap<>(64);

  private final Map<String,ReentrantLock> collLocks = new ConcurrentHashMap<>(64);

  private final Map<String,ActionThrottle> stateWriteThrottles = new ConcurrentHashMap<>(64);

  private final Map<String,DocCollection> cs = new ConcurrentHashMap<>(64);

  // finding #6: node-down/recovery must scale O(replicas_on_node), not O(all collections * all replicas).
  // Primary query index: nodeName -> (collectionId -> immutable snapshot of replica internal ids that the
  // node hosts for that collection). Maintained only on structure changes (the points where a DocCollection
  // lands in or leaves {@link #cs}), so it can never drift from cluster state.
  private final Map<String,Map<Integer,Set<Integer>>> nodeReplicas = new ConcurrentHashMap<>(64);
  // Reverse index: collectionId -> set of node names currently hosting a replica of it. Lets a structure
  // change find which node entries to refresh/drop for a collection without scanning every node.
  private final Map<Integer,Set<String>> collectionNodes = new ConcurrentHashMap<>(64);

  private final AtomicInteger ID = new AtomicInteger();

  private final Set<String> dirtyStructure = ConcurrentHashMap.newKeySet();

  /**
   * collectionId -&gt; replica internalIds published LEADER while that collection's DocCollection was
   * absent (overseer takeover before cs repopulated, or a LEADER update landing before its
   * structure-change). enqueueStateUpdates cannot run the single-leader-per-slice demotion without
   * slice membership, so it defers the ids here; enqueueStructureChange drains them and demotes against
   * the authoritative slice membership the instant the structure lands. Cleared in removeCollection.
   */
  private final Map<Integer,Set<Integer>> pendingLeaderDemotions = new ConcurrentHashMap<>(16);

  private volatile ExecutorService workerExec;
  private volatile long start;

  /** Lazily built on first publish to the per-shard delta plane (the only state-update representation). */
  private volatile StatePlaneWriter statePlaneWriter;

  /**
   * D2 (PR-4): collections whose delta-plane snapshot+manifest this writer has already ensured during
   * this overseer lifetime. The manifest is the reader's switch onto the delta plane (written LAST); we seed it
   * lazily on first publish per collection — never an eager cluster-wide sweep. ZK is the source of
   * truth (we re-check {@code state/manifest} existence before seeding), so this set is only a fast skip.
   */
  private final Set<String> manifestEnsured = ConcurrentHashMap.newKeySet();

  public ZkStateWriter(ZkStateReader zkStateReader, Stats stats, Overseer overseer) {
    this.overseer = overseer;
    this.reader = zkStateReader;
    this.stats = stats;

  }

  /**
   * Writer-side election fence (D4) for the delta plane. {@code writerId} is the overseer's monotonic
   * ZK election sequence ({@link Overseer#getElectionSeq()}) — NOT {@link Overseer#getId()}, which is
   * the node port and is not ordered by election recency (a lower-port node can be elected after a
   * higher-port one, which would falsely fence the legitimate new overseer out). A ring owned by a
   * strictly-higher election sequence fences this writer out. Readers never validate it. Uses
   * {@code overseer} directly (never {@code cc.getZkController()}, the null-trap in minimal test setups).
   */
  private static final class OverseerElectionFence implements StatePlaneWriter.ElectionFence {
    private final Overseer overseer;
    OverseerElectionFence(Overseer overseer) { this.overseer = overseer; }

    @Override public boolean stillElected() { return !overseer.isClosed(); }

    @Override public String writerId() {
      Integer seq = overseer.getElectionSeq();
      return seq == null ? null : seq.toString();
    }

    @Override public boolean isFencedBy(String ringWriterId) {
      if (ringWriterId == null) return false;
      Integer mine = overseer.getElectionSeq();
      if (mine == null) return false;
      try {
        return Long.parseLong(ringWriterId) > mine.longValue();
      } catch (NumberFormatException nfe) {
        return false; // unparseable ring writerId is never fencing
      }
    }

    /**
     * ZK-authoritative election ownership (review P1 #5). The overseer leader is the LOWEST live
     * candidate sequence under {@code /overseer/overseer_elect/election}; {@link Overseer#getElectionSeq()}
     * is this overseer's candidate sequence. If the current minimum live candidate sequence is not ours,
     * a newer overseer has taken over (our candidate node is gone, or a re-election occurred) and this
     * writer is stale — fence it out, even though it may not yet be marked closed and even if the new
     * overseer has not yet written this shard's ring. Degrades to the local liveness check in minimal
     * setups (no election seq / no ZkController) and falls back to it on a transient ZK read error — a
     * new overseer cannot have been elected while ZK is unreachable, so the local check suffices there.
     */
    @Override public boolean ownsElectionAuthoritative() {
      if (overseer.isClosed()) return false;
      Integer mySeq = overseer.getElectionSeq();
      if (mySeq == null) return true; // minimal/test setup — fence effectively disabled.
      ZkController zkc = overseer.getZkController();
      if (zkc == null) return true;
      try {
        List<String> children = zkc.getZkClient()
            .getChildren(Overseer.OVERSEER_ELECT + LeaderElector.ELECTION_NODE, null, true);
        int min = Integer.MAX_VALUE;
        for (String c : children) {
          try {
            int s = LeaderElector.getSeq(c);
            if (s < min) min = s;
          } catch (RuntimeException malformed) {
            // A child whose name has no parseable sequence (shouldn't happen) — ignore it.
          }
        }
        // No live candidates visible -> cannot prove staleness; don't fence on absence.
        return min == Integer.MAX_VALUE || min == mySeq.intValue();
      } catch (KeeperException | InterruptedException e) {
        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        log.warn("overseer election validation read failed; falling back to local liveness check", e);
        return !overseer.isClosed();
      }
    }
  }

  /** Lazy accessor for the per-shard delta-plane writer. */
  private StatePlaneWriter statePlaneWriter() {
    StatePlaneWriter w = statePlaneWriter;
    if (w == null) {
      synchronized (this) {
        w = statePlaneWriter;
        if (w == null) {
          w = new StatePlaneWriter(reader.getZkClient(), new OverseerElectionFence(overseer));
          statePlaneWriter = w;
        }
      }
    }
    return w;
  }

  /**
   * Publish for one collection. Groups the collection's flat {@code internalId -> shortState} map by
   * shard (via the structure {@link DocCollection}) and appends ONE per-shard delta through {@link
   * StatePlaneWriter#publish} — never a full-collection map write.
   *
   * <p>Runs on the {@code taskZkWriterExecutor}, so it never holds a {@code stateUpdates.compute()} bin
   * lock during the synchronous ZK round trip. When the collection's structure is not yet known the
   * state stays in the in-memory map and is published in full on a later write once structure exists.
   */
  private void publishToStatePlane(String collection, Map<Integer,Integer> fullMap) {
    DocCollection dc = cs.get(collection);
    if (dc == null) {
      if (fullMap.isEmpty()) {
        // Nothing to publish — no structure AND no pending replica states. Safe to report success.
        log.debug("statePlane: no DocCollection for {} and nothing to publish", collection);
        return;
      }
      // Structure not yet known but there ARE replica-state changes to persist. Do NOT report success:
      // the caller already drained these ids from pendingChangedIds, so a normal return would let the
      // durability future complete and WorkQueueWatcher delete the queue item before anything was
      // appended — a durable loss (finding #1). Fail the publish so the future completes exceptionally;
      // writeStateUpdatesInternal re-arms the drained ids and the queue item is held for reprocess until
      // the DocCollection lands (at which point this collection's states publish in full).
      throw new RuntimeException("statePlane: structure for " + collection
          + " not yet known; deferring " + fullMap.size() + " replica-state update(s) for retry");
    }
    // Ensure the snapshot+manifest exist BEFORE any delta is appended. The manifest is the reader's
    // switch onto the delta plane; without it the reader never sees this collection's state. Throws if
    // seeding fails, aborting this publish before any delta is appended so we never leave a durable but
    // unreachable delta (the failure propagates to the publish future and the queue item is reprocessed).
    ensureManifestSeeded(collection, dc);
    // Group changed-replica states by shard. The shard for each changed internalId is resolved in O(1)
    // through DocCollection's idToReplica index (getReplicaById) rather than scanning every slice/replica.
    // The old nested scan made a single-replica publish O(replicas_in_collection) and a node-down publish
    // O(changed_ids x replicas_in_collection); this is O(changed_ids). (finding #3)
    Map<String,List<StateDelta.Entry>> byShard = new LinkedHashMap<>();
    for (Map.Entry<Integer,Integer> e : fullMap.entrySet()) {
      Integer internalId = e.getKey();
      Integer shortState = e.getValue();
      if (internalId == null || shortState == null) continue;
      Replica r = dc.getReplicaById(internalId);
      if (r == null) continue; // replica not in structure (e.g. just removed)
      byShard.computeIfAbsent(r.getSlice(), k -> new ArrayList<>())
          .add(new StateDelta.Entry(internalId, shortState));
    }
    if (byShard.isEmpty()) return;
    StatePlaneWriter w = statePlaneWriter();
    for (Map.Entry<String,List<StateDelta.Entry>> e : byShard.entrySet()) {
      // One multi-entry delta per (coll,shard) — node-down batching (D7) falls out for free here,
      // because all of a downed node's replicas in a shard arrive in fullMap together.
      w.publish(collection, e.getKey(), e.getValue(), null);
    }
  }

  /**
   * Lazy per-collection delta-plane bootstrap. On the first publish to a collection whose delta
   * {@code state/manifest} node does not yet exist, seed an empty per-shard snapshot for every shard
   * and write the manifest LAST (the reader's switch onto the delta plane).
   *
   * <p>Ordering guarantee: the seed snapshot is stamped at the takeover {@code epoch} with
   * {@code baseSeq=0} and the seeded ring starts at {@code seq=0}. The next real {@link
   * StatePlaneWriter#publish} appends at {@code seq>=1} (rebasing epoch up if a newer ring exists), so a
   * real delta strictly out-orders the seeded baseline.
   *
   * <p>Idempotent and bounded to one collection per call — there is no startup sweep. ZK manifest
   * existence is the source of truth; {@link #manifestEnsured} is only a fast in-process skip.
   */
  private void ensureManifestSeeded(String collection, DocCollection dc) {
    if (manifestEnsured.contains(collection)) return;
    final String collPath = StatePlanePaths.collectionPath(collection);
    try {
      if (reader.getZkClient().exists(StatePlanePaths.manifest(collPath), true)) {
        manifestEnsured.add(collection);
        return;
      }
      final int epoch = seedEpoch();
      final StatePlaneWriter w = statePlaneWriter();
      final List<String> shards = new ArrayList<>();
      for (Slice s : dc.getSlices()) {
        final String shard = s.getName();
        shards.add(shard);
        w.seedShard(collection, shard, epoch, Collections.emptyMap());
      }
      // Manifest LAST — the authoritative switch the reader gates on.
      w.writeManifestSeeded(collection, epoch, shards);
      manifestEnsured.add(collection);
      if (log.isDebugEnabled()) {
        log.debug("statePlane: seeded delta manifest for {} at epoch {} ({} shards)",
            collection, epoch, shards.size());
      }
    } catch (Exception e) {
      // Do NOT mark ensured AND do NOT swallow. There is no _statupdates fallback by design: the manifest
      // is the reader's ONLY authoritative switch onto the delta plane, so a delta appended while the manifest
      // is absent is durable-but-unreachable (readers treat a missing manifest as "plane not seeded" and
      // no-op). Propagate so publishToStatePlane aborts BEFORE appending any delta, completes its publish
      // future exceptionally, and WorkQueueWatcher leaves the queue item for reprocess — the seed (and the
      // delta it gates) is retried on the next publish. (finding #1)
      log.warn("statePlane: could not seed delta manifest for {} (publish held back; will retry)", collection, e);
      throw new RuntimeException("Failed to seed delta-plane manifest for " + collection, e);
    }
  }

  /**
   * Takeover-monotonic seed epoch: the overseer's monotonic ZK election sequence
   * ({@link Overseer#getElectionSeq()}) — NOT {@link Overseer#getId()} (the node port, which is not
   * ordered by election recency, so a lower-port node elected later would seed a lower epoch and
   * order behind a prior overseer's ring). Mirrors {@link OverseerElectionFence#writerId()}, which
   * already fences on the election sequence. getElectionSeq() is null in minimal test setups (no
   * election path); fall back to 1 so a seed is still positive and monotonic-enough to win over an
   * empty plane.
   */
  private int seedEpoch() {
    Integer seq = overseer.getElectionSeq();
    return (seq != null && seq > 0) ? seq : 1;
  }

  /**
   * True if the replica with the given internal id currently has an effective live state of ACTIVE
   * (or LEADER, which is published as ACTIVE) in the StateUpdates channel. The structure DocCollection
   * returned by {@link #getClusterstate} carries each replica's registration-time state (often DOWN);
   * live replica state lives only in this channel, so completion/'all active' checks must consult it.
   */
  public boolean isReplicaActive(int collId, Integer replicaId) {
    if (replicaId == null) return false;
    Map<Integer,Integer> map = stateUpdates.get(collId);
    if (map == null) return false;
    Integer shortState = map.get(replicaId);
    if (shortState == null) return false;
    return Replica.State.shortStateToState(shortState, true) == Replica.State.ACTIVE;
  }

  /**
   * H1: demote any replica still holding a stale LEADER short-state that was published while this
   * collection's structure was absent (recorded in {@link #pendingLeaderDemotions}). Called from
   * enqueueStructureChange just before the DocCollection is published to {@code cs}, so the
   * single-leader-per-slice invariant holds atomically at the moment slice membership becomes
   * available. Only ever touches replicas in the SAME slice as the deferred leader, so it can never
   * demote a legitimate leader of a different slice.
   */
  private void drainPendingLeaderDemotions(DocCollection landed) {
    if (landed == null) {
      return;
    }
    Set<Integer> pending = pendingLeaderDemotions.remove(landed.getId());
    if (pending == null || pending.isEmpty()) {
      return;
    }
    Map<Integer,Integer> curMap = this.stateUpdates.get(landed.getId());
    if (curMap == null) {
      return;
    }
    final int LEADER_SHORT = Replica.State.getShortState(Replica.State.LEADER);
    final int ACTIVE_SHORT = Replica.State.getShortState(Replica.State.ACTIVE);
    for (Integer rid : pending) {
      // The deferred leader may since have gone DOWN; only act if it still holds LEADER.
      Integer ridState = curMap.get(rid);
      if (ridState == null || ridState.intValue() != LEADER_SHORT) {
        continue;
      }
      Slice mySlice = null;
      for (Slice s : landed.getSlices()) {
        for (Replica r : s.getReplicas()) {
          if (rid.equals(r.getInternalId())) { mySlice = s; break; }
        }
        if (mySlice != null) break;
      }
      if (mySlice == null) {
        continue;
      }
      for (Replica r : mySlice.getReplicas()) {
        Integer otherId = r.getInternalId();
        if (otherId == null || otherId.equals(rid)) continue;
        Integer st = curMap.get(otherId);
        if (st != null && st.intValue() == LEADER_SHORT) {
          curMap.put(otherId, ACTIVE_SHORT);
          log.info("Demoting stale leader replicaId={} -> ACTIVE in collection={} slice={} (deferred; replicaId={} published LEADER before structure landed)",
              otherId, landed.getName(), mySlice.getName(), rid);
        }
      }
    }
  }

  /**
   * Apply replica-state and slice-state updates to the in-memory cluster state. Replica-state updates are
   * carried by the changed-id set and published to the delta plane by the caller via {@link
   * #writeStateUpdates}; slice-state (UPDATESHARDSTATE) updates are hard-state structure changes written
   * here via {@link #writeStructureUpdates}. Returns a future that completes once ALL structure writes
   * this call scheduled have completed (an already-complete future when none were scheduled). The caller
   * MUST gate any queue-item delete on BOTH this future and the {@link #writeStateUpdates} append future:
   * a pure UPDATESHARDSTATE item adds no collection id to the replica-state set, so its append future is
   * trivially complete and, without this gate, the item could be deleted before its slice-state write is
   * durable. (finding #2)
   */
  public CompletableFuture<Void> enqueueStateUpdates(Map<Integer,Map<Integer,Integer>> replicaStates,  Map<Integer,List<ZkStateWriter.StateUpdate>> sliceStates) {    log.debug("enqueue state updates");

    replicaStates.forEach((collectionId, idToStateMap) -> {
      // A newly-elected overseer's ZkStateWriter starts with an EMPTY in-memory stateUpdates map. The
      // durable per-shard delta plane retains prior replica states (most importantly the LEADER) across
      // the overseer handoff, and reconcileLeadersFromZk re-asserts the ephemeral shard-leader nodes into
      // the map at takeover, so a fresh map built from just this incoming delta is repaired without
      // reading any collection-wide node.
      this.stateUpdates.compute(collectionId, (id, map) -> {

        if (map == null) {
          Map<Integer,Integer> newMap = new ConcurrentHashMap<>(idToStateMap.size() + 4);
          newMap.putAll(idToStateMap);
          return newMap;
        }
        idToStateMap.forEach((integer, newState) -> {
          if (map.containsKey(integer)) {
            map.replace(integer, newState);
          } else {
            map.put(integer, newState);
          }
        });

        return map;
      });

      // Record the touched ids so the next publish carries only these entries (finding #1). Accumulate
      // INSIDE compute so the add is atomic against writeStateUpdatesInternal's remove() drain — a
      // computeIfAbsent()+addAll() could add to an already-drained (orphaned) set and lose the id.
      pendingChangedIds.compute(collectionId, (k, set) -> {
        Set<Integer> s = (set == null) ? ConcurrentHashMap.newKeySet() : set;
        s.addAll(idToStateMap.keySet());
        return s;
      });

      // Single-leader-per-slice invariant. The StateUpdates channel is a flat replicaId->shortState
      // map with no ephemeral semantics, so a stale LEADER entry from a previous leader can linger after
      // that replica dies or relinquishes leadership. When a replica is published LEADER for a slice,
      // demote any OTHER replica in the SAME slice that still holds LEADER down to ACTIVE, so getLeader()
      // never sees two leaders for one slice. Two LEADER entries are indistinguishable to getLeader()
      // (it returns whichever iterates first) and, because the entry is not ephemeral, a stale one
      // survives the old leader's ZK-session loss and permanently wedges leader migration
      // (HttpPartitionTest.testLeaderZkSessionLoss and the tlog/ForceLeader variants).
      final int LEADER_SHORT = Replica.State.getShortState(Replica.State.LEADER);
      final int ACTIVE_SHORT = Replica.State.getShortState(Replica.State.ACTIVE);
      String collName = idToCollection.get(collectionId);
      DocCollection dc = collName == null ? null : cs.get(collName);
      Map<Integer,Integer> curMap = this.stateUpdates.get(collectionId);
      if (dc != null && curMap != null) {
        idToStateMap.forEach((rid, newState) -> {
          if (newState == null || newState.intValue() != LEADER_SHORT) return;
          Slice mySlice = null;
          for (Slice s : dc.getSlices()) {
            for (Replica r : s.getReplicas()) {
              if (rid.equals(r.getInternalId())) { mySlice = s; break; }
            }
            if (mySlice != null) break;
          }
          if (mySlice == null) return;
          for (Replica r : mySlice.getReplicas()) {
            Integer otherId = r.getInternalId();
            if (otherId == null || otherId.equals(rid)) continue;
            Integer st = curMap.get(otherId);
            if (st != null && st.intValue() == LEADER_SHORT) {
              curMap.put(otherId, ACTIVE_SHORT);
              log.info("Demoting stale leader replicaId={} -> ACTIVE in collection={} slice={} because replicaId={} published LEADER", otherId, collName, mySlice.getName(), rid);
            }
          }
        });
      } else {
        // Structure not yet known (dc == null): slice peers are unidentifiable, so the demotion above
        // cannot run now. A blanket demotion against curMap would be wrong — it spans all slices of the
        // collection and would demote legitimate leaders of other slices. Defer the LEADER ids so
        // enqueueStructureChange demotes against authoritative membership once the structure lands.
        idToStateMap.forEach((rid, newState) -> {
          if (newState != null && newState.intValue() == LEADER_SHORT) {
            pendingLeaderDemotions.computeIfAbsent(collectionId, k -> ConcurrentHashMap.newKeySet()).add(rid);
          }
        });
      }

      // log.debug("enqueue state updates result {} {}", replicaStatesEntry.getKey(), stateUpdates.get(replicaStatesEntry.getKey()));
    });

    // Structure (slice-state / UPDATESHARDSTATE) writes scheduled below are async; collect their futures
    // so the returned aggregate lets the caller gate queue-item deletion on their durability. (finding #2)
    final List<Future<?>> structureWrites = new ArrayList<>();

    sliceStates.forEach((collectionId, updates) -> {
      String collection = idToCollection.get(collectionId);

      DocCollection docColl = collection == null ? null : cs.get(collection);
      if (docColl == null) {
        // Structure for this collection id is not (yet) loaded — e.g. a freshly elected overseer
        // applying a slice-state (UPDATESHARDSTATE) update before its DocCollection is populated.
        // Skip rather than NPE, which would abort the whole batch (dropping co-batched replica updates).
        return;
      }

      boolean didUpdate = false;

      for (StateUpdate update : updates) {
        Slice slice = docColl.getSlice(update.sliceName);
        if (slice != null) {
          didUpdate = true;
          slice.setState(update.state);
        }
      }

      if (didUpdate) {
        dirtyStructure.add(collection);
        structureWrites.add(writeStructureUpdates(collection));
      }
    });

    // Re-home the classic ReplicaMutator.checkAndCompleteShardSplit completion for the StateUpdates
    // overseer. Trigger only on replica-state changes (never on the slice-state updates this method
    // emits for the completion itself, which would recurse). Replica ACTIVE publishes land here via
    // StatePublisher, bypassing WorkQueueWatcher.processQueueItems, so this is the correct chokepoint.
    if (!replicaStates.isEmpty()) {
      checkAndCompleteShardSplits(replicaStates.keySet());
    }

    return awaitAll(structureWrites);
  }

  /**
   * Aggregate the given async structure-write futures into one {@link CompletableFuture} that completes
   * when ALL of them complete and completes exceptionally if any fails. Lets {@link WorkQueueWatcher}
   * gate a slice-state (UPDATESHARDSTATE) queue-item delete on the durability of the structure write it
   * triggered. Runs the (brief) join on the zkWriter executor so the calling thread is never blocked; an
   * empty list short-circuits to an already-complete future so the common replica-state-only path never
   * touches the executor. (finding #2)
   */
  private CompletableFuture<Void> awaitAll(List<Future<?>> futures) {
    if (futures.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }
    return CompletableFuture.runAsync(() -> {
      for (Future<?> f : futures) {
        try {
          f.get();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        } catch (ExecutionException ee) {
          throw new RuntimeException(ee.getCause() != null ? ee.getCause() : ee);
        }
      }
    }, overseer.getTaskZkWriterExecutor());
  }

  /**
   * When every replica of all RECOVERY sub-shards of a parent shard is ACTIVE, flip the parent to
   * INACTIVE and the sub-shards to ACTIVE -- the slice-state transition the repFactor==1 SPLITSHARD
   * path performs inline. With the queue-based ClusterStateUpdater removed, repFactor>1 splits had no
   * code to complete this transition, so the parent stayed ACTIVE forever (ShardSplitTest).
   */
  private void checkAndCompleteShardSplits(Set<Integer> collIds) {
    try {
      Map<Integer,List<StateUpdate>> sliceStates = new HashMap<>();
      Set<Integer> affected = new HashSet<>();
      for (Integer collId : collIds) {
        String collection = idToCollection.get(collId);
        if (collection == null) continue;
        DocCollection docCollection = cs.get(collection);
        if (docCollection == null) continue;

        Map<String,List<Slice>> byParent = new HashMap<>();
        for (Slice slice : docCollection.getSlices()) {
          if (slice.getState() == Slice.State.RECOVERY && slice.getParent() != null) {
            byParent.computeIfAbsent(slice.getParent(), k -> new ArrayList<>()).add(slice);
          }
        }

        for (Map.Entry<String,List<Slice>> e : byParent.entrySet()) {
          Slice parentSlice = docCollection.getSlice(e.getKey());
          if (parentSlice == null) continue;
          boolean allActive = true;
          for (Slice sub : e.getValue()) {
            Collection<Replica> reps = sub.getReplicas();
            if (reps.isEmpty()) { allActive = false; break; }
            for (Replica r : reps) {
              if (!isReplicaActive(collId, r.getInternalId())) { allActive = false; break; }
            }
            if (!allActive) break;
          }
          if (!allActive) continue;

          log.info("Shard split complete for collection={} parent={} subShards={}: switching parent INACTIVE and sub-shards ACTIVE",
              collection, e.getKey(), e.getValue());
          List<StateUpdate> updates = sliceStates.computeIfAbsent(collId, k -> new ArrayList<>());
          StateUpdate pu = new StateUpdate();
          pu.sliceName = e.getKey();
          pu.state = Slice.State.INACTIVE;
          updates.add(pu);
          for (Slice sub : e.getValue()) {
            StateUpdate su = new StateUpdate();
            su.sliceName = sub.getName();
            su.state = Slice.State.ACTIVE;
            updates.add(su);
          }
          affected.add(collId);
        }
      }
      if (!sliceStates.isEmpty()) {
        enqueueStateUpdates(new HashMap<>(), sliceStates);
      }
    } catch (Exception e) {
      log.error("Error while checking for completed shard splits", e);
    }
  }

  public void enqueueStructureChange(DocCollection docCollection) {

    try {

      log.debug("enqueue structure change docCollection={} replicas={}", docCollection, docCollection.getReplicas());

      String collectionName = docCollection.getName();

      idToCollection.put(docCollection.getId(), docCollection.getName());

      ReentrantLock collLock = collLocks.compute(collectionName, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          return new ReentrantLock();
        }
        return reentrantLock;
      });
      collLock.lock();
      try {

        docCollection = docCollection.copy();

        DocCollection currentCollection = cs.get(docCollection.getName());
        log.trace("zkwriter collection={}", docCollection);
        log.trace("zkwriter currentCollection={}", currentCollection);
        dirtyStructure.add(docCollection.getName());


        if (currentCollection != null) {
          List<String> removeSlices = new ArrayList<>();

          Map<String,Slice> newSlices = currentCollection.getSlicesCopy();
          for (Slice docCollectionSlice : docCollection.getSlices()) {
            if (docCollectionSlice.get("remove") != null) {
              removeSlices.add(docCollectionSlice.getName());
              continue;
            }

            // Merge the incoming replicas INTO the current slice's replica set rather than replacing
            // it wholesale. enqueueStructureChange is invoked concurrently (e.g. parallel addReplica to
            // different shards, as the legacy FullDistrib test base does); each caller builds its
            // DocCollection from a possibly-stale cluster-state snapshot, so replacing every incoming
            // slice's replicas wholesale clobbered replicas that a concurrent change had just committed
            // to ANOTHER shard (the stale caller's view of that shard was still empty/older). Starting
            // from the current slice keeps already-committed replicas; incoming replicas are still
            // added/updated (put overwrites by name), and removals continue to work via the "remove"
            // tombstone because SliceMutator.removeReplica keeps the replica in the slice flagged
            // remove=true (it does not omit it), so the merge below drops it. This is also why the
            // tombstone pass is order-independent: surviving replicas are carried over first, then the
            // tombstoned ones are dropped.
            Slice currentSlice = currentCollection.getSlice(docCollectionSlice.getName());
            Map<String,Replica> newReplicaMap = (currentSlice != null)
                ? currentSlice.getReplicasCopy() : docCollectionSlice.getReplicasCopy();
            for (Replica replica : docCollectionSlice.getReplicas()) {
              if (replica.get("remove") != null) {
                newReplicaMap.remove(replica.getName());
                continue;
              }
              Replica newReplica = replica.copyWithProps(new HashMap(2));
              newReplicaMap.put(replica.getName(), newReplica);
            }

            Slice newDocCollectionSlice = docCollectionSlice.copyWithReplicas(newReplicaMap);
            // Preserve the CURRENT slice state across structure merges. Slice state transitions
            // (CONSTRUCTION -> RECOVERY -> ACTIVE/INACTIVE) flow exclusively through UPDATESHARDSTATE
            // state-updates (enqueueStateUpdates -> slice.setState on the canonical cs). A structure
            // change (e.g. the per-replica addReplica enqueueStructureChange during SPLITSHARD) is built
            // from a possibly-stale cluster-state snapshot and carries an OLD slice state; applying it
            // wholesale reverts e.g. a sub-shard that was just moved to RECOVERY back to CONSTRUCTION, so
            // WorkQueueWatcher.checkAndCompleteShardSplits never sees RECOVERY and the split never
            // completes (parent stuck ACTIVE -> ShardSplitTest.testSplitMixedReplicaTypes). A brand-new
            // slice (currentSlice == null, e.g. createShard) still takes its incoming state.
            if (currentSlice != null) {
              newDocCollectionSlice.setState(currentSlice.getState());
              // Likewise preserve the immutable sub-shard "parent" linkage. It is set once at sub-shard
              // creation and must survive structure merges: a stale incoming slice (e.g. from addReplica
              // building off an older snapshot) can lack it, and copyWithReplicas would then drop it,
              // leaving the sub-shard with getParent()==null so split completion never recognizes it.
              Object curParent = currentSlice.get(Slice.PARENT);
              if (curParent != null && newDocCollectionSlice.get(Slice.PARENT) == null) {
                Object2ObjectLinkedOpenHashMap<String,Object> pProps =
                    new Object2ObjectLinkedOpenHashMap<>(newDocCollectionSlice.getProperties(), 0.5f);
                pProps.put(Slice.PARENT, curParent);
                Object spn = currentSlice.get("shard_parent_node");
                if (spn != null && pProps.get("shard_parent_node") == null) pProps.put("shard_parent_node", spn);
                Object spz = currentSlice.get("shard_parent_zk_session");
                if (spz != null && pProps.get("shard_parent_zk_session") == null) pProps.put("shard_parent_zk_session", spz);
                newDocCollectionSlice = new Slice(newDocCollectionSlice.getName(), newReplicaMap, pProps,
                    collectionName, currentCollection.getId());
                newDocCollectionSlice.setState(currentSlice.getState());
              }
            }
            // Preserve slice-level routing rules across structure merges. A structure change built from a
            // cluster-state snapshot that predates an ADDROUTINGRULE carries no routingRules on its incoming
            // slice; copyWithReplicas would then overwrite (drop) the rule that SliceMutator.addRoutingRule
            // committed to the current slice, breaking MIGRATE's forwarding of in-flight updates to the
            // target collection. Only carry the current rules forward when the incoming slice does not itself
            // specify routingRules (REMOVEROUTINGRULE leaves a non-null, possibly empty, map so it still wins).
            if (currentSlice != null && docCollectionSlice.getRoutingRules() == null
                && currentSlice.getRoutingRules() != null) {
              Object2ObjectLinkedOpenHashMap<String,Object> mergedProps =
                  new Object2ObjectLinkedOpenHashMap<>(newDocCollectionSlice.getProperties(), 0.5f);
              mergedProps.put("routingRules", currentSlice.getProperties().get("routingRules"));
              newDocCollectionSlice = new Slice(newDocCollectionSlice.getName(), newReplicaMap, mergedProps,
                  collectionName, currentCollection.getId());
            }
            newSlices.put(newDocCollectionSlice.getName(), newDocCollectionSlice);
          }
          for (String removeSlice : removeSlices) {
            newSlices.remove(removeSlice);
          }
          // Use the INCOMING collection's props as the authoritative prop set. The mutators always derive
          // the new DocCollection from current cluster state, so its props are complete; taking them here
          // lets structure changes that also modify collection-level properties actually take effect
          // (e.g. MODIFYCOLLECTION setting/unsetting readOnly). Previously this seeded from
          // currentCollection.getProps() and the incoming props were dropped, so MODIFYCOLLECTION was a
          // silent no-op.
          Map newDocProps = new HashMap(docCollection.getProps());
          newDocProps.remove("pullReplicas");
          newDocProps.remove("replicationFactor");
          newDocProps.remove("maxShardsPerNode");
          newDocProps.remove("nrtReplicas");
          newDocProps.remove("tlogReplicas");
          newDocProps.remove("numShards");
          DocCollection newCollection = currentCollection.copyWithSlices(newSlices, newDocProps);
          // Carry forward live replica states (ACTIVE/LEADER/DOWN from the StateUpdates channel) across
          // the structure merge. copyWithSlices builds a fresh DocCollection whose live StateUpdates map is
          // reseeded from the state.json baseline (DocCollection.setStates -> Replica.getPublishedState), so
          // without this overlay a structure-change reload clobbers a live ACTIVE/LEADER replica back to its
          // (often DOWN) baseline. Overlaying via updateState mutates each surviving replica's already-linked
          // AtomicInteger in place (preserving Replica.linkState): ids no longer present in the new structure
          // are skipped (updateState no-ops on absent ids, so tombstone-dropped replicas stay dropped), and
          // newly-added replicas keep their fresh baseline seed (absent from the current map). The
          // delta-plane cursors are carried forward too so the delta-plane version gate in
          // StatePlaneReader.carryForwardStateUpdates keeps ordering correctly across the merge.
          currentCollection.getStateUpdates().forEach((id, st) ->
              newCollection.updateState((Integer) id, ((AtomicInteger) st).get()));
          newCollection.adoptStatePlaneCursors(currentCollection);
          log.debug("zkwriter newCollection={} replicas={}", newCollection, newCollection.getReplicas());
          // Demote any leader published before this structure landed (H1), before the collection becomes
          // visible to readers, so the single-leader-per-slice invariant holds atomically at publication.
          drainPendingLeaderDemotions(newCollection);
          cs.put(currentCollection.getName(), newCollection);
          indexCollection(newCollection);

        } else {
          Map<String,Object> newDocProps = new HashMap<>(docCollection.getProps());

          Map<String,Slice> newSlices = docCollection.getSlicesCopy();
          List<String> removeSlices = new ArrayList<>();
          for (Slice slice : docCollection) {

            if (slice.get("remove") != null) {
              removeSlices.add(slice.getName());
            }

            for (Replica replica : slice.getReplicas()) {

              Map<String,Replica> newReplicaMap = slice.getReplicasCopy();
              Map<Object,Object> newProps = new HashMap<>(2);

              Replica newReplica = replica.copyWithProps(newProps);
              newReplicaMap.put(newReplica.getName(), newReplica);
              Slice newSlice = slice.copyWithReplicas(newReplicaMap);
              newSlices.put(newSlice.getName(), newSlice);
            }

          }
          for (String removeSlice : removeSlices) {
            newSlices.remove(removeSlice);
          }

          newDocProps.remove("pullReplicas");
          newDocProps.remove("replicationFactor");
          newDocProps.remove("maxShardsPerNode");
          newDocProps.remove("nrtReplicas");
          newDocProps.remove("tlogReplicas");
          newDocProps.remove("numShards");
          DocCollection landedCollection = docCollection.copyWithSlices(newSlices, newDocProps);
          // Demote any leader published before this structure landed (H1), before publication to cs.
          drainPendingLeaderDemotions(landedCollection);
          cs.put(docCollection.getName(), landedCollection);
          indexCollection(landedCollection);
        }

      } finally {
        collLock.unlock();
      }

      // Structure for this id has now landed in cs (review P0 #2, option 2). Flush any replica-state
      // updates that arrived BEFORE the structure and were held armed in pendingChangedIds /
      // stateUpdates: a single-collection republish appends them to the delta plane NOW rather than
      // waiting for an unrelated later write or relying solely on queue-item reprocess. This must run
      // after cs.put (publishToStatePlane needs the collection present in cs to map replicas to shards),
      // i.e. after the locked block above. Best-effort + async (taskZkWriterExecutor); the retained
      // queue item is the durable backstop and publishToStatePlane is idempotent.
      final int landedId = docCollection.getId();
      unknownIdAttempts.remove(landedId);
      if (pendingChangedIds.containsKey(landedId) || stateUpdates.containsKey(landedId)) {
        try {
          writeStateUpdatesInternal(Collections.singleton(landedId), 1);
        } catch (RuntimeException republishEx) {
          log.warn("structure-landing republish for collection id {} failed; the retained queue item "
              + "will reprocess it", landedId, republishEx);
        }
      }

    } catch (Exception e) {
      log.error("Exception while queuing update", e);
      throw e;
    }
  }

  public Integer lastWrittenVersion(String collection) {
    DocCollection col = cs.get(collection);
    if (col == null) {
      return 0;
    }
    return col.getZNodeVersion();
  }

  /**
   * Writes all pending updates to ZooKeeper and returns the modified cluster state
   *
   */

  public CompletableFuture<Void> writeStructureUpdates(String collection) {
    // The state.json write is async (setData with a ZK callback), so the returned future must be
    // completed FROM that callback — completing it when this submit body returns would signal
    // durability before ZK has acked the write, letting WorkQueueWatcher delete the queue item too
    // early (finding #2). write() completes `result` on the state.json callback (success) or chains a
    // rescheduled attempt's future into it (on a retriable ZK rc), so the future tracks real durability.
    CompletableFuture<Void> result = new CompletableFuture<>();
    ParWork.submit("zkStateWriter#writePendingUpdates", () -> {
      MDCLoggingContext.setNode(overseer.getZkController().getNodeName());
      try {
        write(collection, result);
      } catch (Exception e) {
        log.error("write pending failed", e);
        result.completeExceptionally(e);
      }
    });
    return result;
  }

  private void write(String coll, CompletableFuture<Void> result) {

    if (log.isDebugEnabled()) {
      log.debug("writePendingUpdates {}", coll);
    }

    log.debug("process collection {}", coll);
    // Serialize all writes for a single collection through the same per-collection lock that
    // enqueueStructureChange takes when it mutates cs. setData() below is async with version -1, and the
    // ZK client is a single FIFO session: the request is queued in the order setData() is invoked. Two
    // concurrent writers for the same collection could otherwise read cs, race into setData(), and have an
    // older cs land after a newer one -- BadVersion can never reject it (version -1) so the stale state.json
    // would silently persist. Holding the lock across the cs read + setData invocation makes the last writer
    // read the freshest cs and queue its send last, so ZK applies the newest state.json last.
    ReentrantLock collLock = collLocks.compute(coll, (s, reentrantLock) ->
        reentrantLock == null ? new ReentrantLock() : reentrantLock);
    collLock.lock();
    try {

      DocCollection collection = cs.get(coll);

      if (collection == null) {
        // Nothing to persist (collection not present in cs) — no durable write needed.
        result.complete(null);
        return;
      }


      if (log.isTraceEnabled()) log.trace("check collection {} {}", collection, dirtyStructure);

      //  collState.throttle.minimumWaitBetweenActions();
      //  collState.throttle.markAttemptingAction();
      String name = collection.getName();
      String path = ZkStateReader.getCollectionPath(collection.getName());
      String pathSCN = ZkStateReader.getCollectionSCNPath(collection.getName());

      if (log.isTraceEnabled()) log.trace("process {}", collection);
      try {

        if (dirtyStructure.contains(name)) {
          if (log.isDebugEnabled()) log.debug("structure change in {}", collection.getName());

          byte[] data = Utils.toJSON(singletonMap(name, collection));

          if (log.isDebugEnabled()) log.debug("Write state.json prevVersion={} bytes={} col={} ", collection.getZNodeVersion(), data.length, collection);

          if (reader == null) {
            log.error("read not initialized in zkstatewriter");
            result.completeExceptionally(new IllegalStateException("reader not initialized in ZkStateWriter"));
            return;
          }
          if (reader.getZkClient() == null) {
            log.error("zkclient not initialized in zkstatewriter");
            result.completeExceptionally(new IllegalStateException("zkclient not initialized in ZkStateWriter"));
            return;
          }

          structureWrites.mark();
          reader.getZkClient().setData(path, data, -1, (rc, path1, ctx, stateJsonStat) -> {
            if (rc != 0) {
              KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path1);
              log.error("Exception writing state.json path={}", path1, e);

              // dirtyStructure is cleared only on success (below), so the collection is still marked
              // dirty here. Reschedule for any non-success rc (ConnectionLoss, SessionExpired, NoNode,
              // ...) so the in-memory cs does not stay ahead of the persisted state.json with no retry.
              // setData uses version -1, so BadVersion cannot occur. Guard against a reschedule storm
              // during shutdown (mirrors the writer-loop close guard above).
              if (!overseer.isClosed() && !reader.getZkClient().isClosed()) {
                // Retriable rc — reschedule and propagate THAT attempt's durability into this future,
                // so `result` completes only when state.json is actually persisted (finding #2).
                overseer.getZkStateWriter().writeStructureUpdates(coll).whenComplete((v, ex) -> {
                  if (ex == null) {
                    result.complete(null);
                  } else {
                    result.completeExceptionally(ex);
                  }
                });
              } else {
                // Shutting down — no retry will run; do not falsely claim durability.
                result.completeExceptionally(e);
              }

            } else {
              dirtyStructure.remove(name);
              try {
                reader.getZkClient().setData(pathSCN, null, -1, (rc2, path2, ctx2, scnStat) -> {
                  if (rc2 != 0) {
                    KeeperException e = KeeperException.create(KeeperException.Code.get(rc2), path2);
                    log.error("Exception on trigger scn znode path={}", path2, e);
                  }
                }, "pathSCN");
              } catch (Exception e) {
                log.error("Exception triggering SCN node");
              }
              // state.json is durable here. The _scn node is a best-effort change-notification trigger
              // whose failure does NOT affect state.json durability, so the durability future completes
              // on state.json success regardless of the SCN outcome (finding #2).
              result.complete(null);
            }
          }, "state.json");

        } else {
          // No structure change pending for this collection — already clean and durable.
          result.complete(null);
        }

      } catch (Exception e) {
        log.error("Failed processing update={}", collection, e);
        // setData (or a surrounding synchronous step) threw before any callback could fire; do not let
        // the durability future hang. completeExceptionally is a no-op if already completed.
        result.completeExceptionally(e);
      }

    } finally {
      collLock.unlock();
    }

  }

  public ClusterState getClusterstate(String collection) {

    Map<String,DocCollection> map;
    if (collection != null) {

      map = new Object2ObjectLinkedOpenHashMap<>(1, 0.25f);
      DocCollection coll = cs.get(collection);
      if (coll != null) {
        map.put(collection, coll.copy());
      }

    } else {
      map = new Object2ObjectLinkedOpenHashMap<>(cs.keySet().size(), 0.25f);
      cs.forEach((s, docCollection) -> map.put(s, docCollection.copy()));
    }

    return ClusterState.getRefCS(map, -2);

  }

//  public String getCollectionForId(Integer id) {
//    AtomicReference<String> collectionName = new AtomicReference<>();
//
//    for (DocCollection docCollection : cs.values()) {
//      if (docCollection.getId().equals(id)) {
//        collectionName.set(docCollection.getName());
//        break;
//      }
//    }
//
//    String name = collectionName.get();
//    if (name == null) {
//      Collection<DocCollection> watchedCollectionStates = reader.getClusterState().getWatchedCollectionStates();
//
//      for (DocCollection docCollection : watchedCollectionStates) {
//        if (docCollection.getId().equals(id)) {
//          collectionName.set(docCollection.getName());
//          break;
//        }
//      }
//    }
//
//    name = collectionName.get();
//    if (name == null) {
//      Collection<ClusterState.CollectionRef> lazydCollectionStates = reader.getClusterState().getLazyCollectionStates();
//      for (ClusterState.CollectionRef docCollection : lazydCollectionStates) {
//        DocCollection docColl = docCollection.get().join();
//        if (docColl != null) {
//          if (docColl.getId().equals(id)) {
//            collectionName.set(docColl.getName());
//            break;
//          }
//        }
//      }
//    }
//
//    return collectionName.get();
//  }

  public Set<String> getDirtyStructureCollections() {
    return dirtyStructure;
  }


  public void removeCollection(String collection) {
    log.debug("Removing collection from zk state {}", collection);
    try {
      ReentrantLock collLock = collLocks.compute(collection, (s, reentrantLock) -> {
        if (reentrantLock == null) {
          return new ReentrantLock();
        }
        return reentrantLock;
      });
      collLock.lock();
      try {
        assignMap.remove(collection);
        dirtyStructure.remove(collection);
        // Invalidate the delta-plane manifest skip cache: the collection's state/manifest znode is
        // deleted with the collection, so a later same-named recreate must re-seed it. Without this,
        // ensureManifestSeeded() short-circuits on the stale name and the reader never switches onto
        // the delta plane for the new incarnation (sees no manifest and treats the plane as un-seeded).
        manifestEnsured.remove(collection);

        DocCollection removed = cs.remove(collection);
        if (removed != null) {
          deindexCollection(removed.getId());
          stateUpdates.remove(removed.getId());
          idToCollection.remove(removed.getId());
          // Record the removed incarnation so a late replica-state update for this id is dropped cleanly
          // rather than retained-for-reprocess forever (review P0 #2, option 3).
          removedCollIds.add(removed.getId());
          pendingChangedIds.remove(removed.getId());
          unknownIdAttempts.remove(removed.getId());
          // The structure for this id will never land now; drop any deferred leader-demotion ids so the
          // map does not leak an entry keyed by a dead collection id.
          pendingLeaderDemotions.remove(removed.getId());
          List<Replica> replicas = removed.getReplicas();
          for (Replica replica : replicas) {
            overseer.getZkController().clearCachedState(replica.getName());
          }

        }

      } finally {
        collLock.unlock();
        collLocks.compute(collection, (s, reentrantLock) -> null);
      }
    } catch (Exception e) {
      log.error("Exception removing collection", e);

    }
  }

  /**
   * Rebuilds the nodeName-&gt;placement index for a single collection from its authoritative DocCollection.
   * Called under the per-collection lock right where the collection lands in {@link #cs}, so the index can
   * never drift from cluster state. Cost is O(replicas in this collection). Per-node value sets are
   * immutable snapshots and are always replaced wholesale, so concurrent {@link #getReplicasOnNode} reads
   * are lock-free and consistent. Empty per-node maps are intentionally left in place (a bounded set of
   * known nodes) to avoid a remove/reinsert race with a concurrent structure change on the same node.
   */
  private void indexCollection(DocCollection dc) {
    final int collId = dc.getId();
    Map<String,Set<Integer>> newByNode = new HashMap<>();
    for (Slice slice : dc.getSlices()) {
      for (Replica r : slice.getReplicas()) {
        newByNode.computeIfAbsent(r.getNodeName(), k -> new HashSet<>()).add(r.getInternalId());
      }
    }
    Set<String> oldNodes = collectionNodes.get(collId);
    if (oldNodes != null) {
      for (String node : oldNodes) {
        if (!newByNode.containsKey(node)) {
          Map<Integer,Set<Integer>> perColl = nodeReplicas.get(node);
          if (perColl != null) {
            perColl.remove(collId);
          }
        }
      }
    }
    for (Map.Entry<String,Set<Integer>> e : newByNode.entrySet()) {
      nodeReplicas.computeIfAbsent(e.getKey(), k -> new ConcurrentHashMap<>())
          .put(collId, Set.copyOf(e.getValue()));
    }
    collectionNodes.put(collId, Set.copyOf(newByNode.keySet()));
  }

  /** Drops all node-index entries for a removed collection. Cost is O(nodes that hosted it). */
  private void deindexCollection(int collId) {
    Set<String> oldNodes = collectionNodes.remove(collId);
    if (oldNodes != null) {
      for (String node : oldNodes) {
        Map<Integer,Set<Integer>> perColl = nodeReplicas.get(node);
        if (perColl != null) {
          perColl.remove(collId);
        }
      }
    }
  }

  /**
   * Returns, for a node, the (collectionId -&gt; replica internal ids) it hosts, so node-down/recovery can
   * mark exactly that node's replicas without scanning every collection (finding #6). The returned map and
   * its value sets are live/immutable snapshots; callers must not mutate them. Iteration is weakly
   * consistent (matches the prior cluster-state-snapshot scan's eventual-consistency semantics).
   */
  public Map<Integer,Set<Integer>> getReplicasOnNode(String nodeName) {
    Map<Integer,Set<Integer>> perColl = nodeReplicas.get(nodeName);
    return perColl == null ? Collections.emptyMap() : perColl;
  }

  public Integer getHighestId(String collection) {
    Integer id = ID.incrementAndGet();
    idToCollection.put(id, collection);
    return id;
  }

  public int getReplicaAssignCnt(String collection, String shard, String namePrefix) {

    DocAssign docAssign = assignMap.computeIfAbsent(collection, c -> new DocAssign());

    int id = docAssign.replicaAssignCnt.incrementAndGet();
    log.debug("assign id={} for collection={} slice={} namePrefix={}", id, collection, shard, namePrefix);
    return id;
  }

  public void init(boolean weAreReplacement) {
    log.info("ZkStateWriter Init - A new Overseer in charge or we are back baby replacement={}", weAreReplacement);
    start = System.nanoTime();
    try {

      overseer.getZkController().clearStatePublisher();

      // This ZkStateWriter is a per-node singleton (one Overseer per ZkController, one final ZkStateWriter
      // per Overseer), so init() runs again every time this node re-wins the overseer election. The Worker
      // run loop guards on the instance field `terminated`, and stop() enqueues a shared static TERMINATED
      // sentinel onto the instance `workQueue`. Neither was reset on re-init, so under overseer churn a
      // stale TERMINATED left in workQueue by a previous incarnation's stop() was polled by the NEW Worker,
      // which then terminated itself immediately -- leaving a freshly-elected overseer with no running
      // writer. Replica state updates (e.g. a just-elected shard LEADER) were then enqueued via
      // workQueue.put but never drained to the delta plane, so readers hung in getLeaderRetry until the
      // suite timeout (LeaderElectionIntegrationTest under load). Re-establish a clean runnable state
      // before submitting the new Worker: a new overseer rebuilds all durable state from ZK
      // (reconcileLeadersFromZk + WorkQueueWatcher onStart + the delta plane), so the in-process hand-off
      // queue legitimately starts empty.
      terminated = false;
      workQueue.clear();

      Worker worker = new Worker();
      workerExec = Executors.newSingleThreadExecutor(new SolrNamedThreadFactory("ZKStateWriter", true));
      workerExec.submit(worker);

      int[] highId = new int[1];
      // This ZkStateWriter is a per-node singleton re-init()'d on every overseer re-election; rebuild the
      // node-placement index from scratch so a prior incarnation's placements cannot linger (finding #6).
      nodeReplicas.clear();
      collectionNodes.clear();

      Map<String,ClusterState.CollectionRef> collectionRefs = reader.getCollectionRefs();

      collectionRefs.forEach((collectionName, docStateRef) -> {
        final DocCollection docState;
        try {
          docState = docStateRef.get(false).join();
        } catch (Exception e) {
          // getCollectionRefs() is a point-in-time snapshot that can include a lazy ref whose
          // state.json no longer exists: a collection deleted in the window between the snapshot and
          // this resolve, or a doomed lazy ref left behind by a timed-out waitForState on a
          // non-existent collection name (e.g. a stale request routed to a deleted core via
          // SolrCall.getRemoteCoreUrl). A newly-elected overseer must seed and republish the leader of
          // every *resolvable* collection; aborting the entire init on one unresolvable ref previously
          // bricked the takeover -- no LEADER was republished, so a just-killed shard stayed
          // leaderless until the next overseer change (root cause of the
          // TestTlogReplica.testKillLeader "Expect new leader" flake). Skip the bad ref, keep seeding.
          log.warn("ZkStateWriter init skipping collection={} whose state could not be resolved", collectionName, e);
          return;
        }
        idToCollection.put(docState.getId(), collectionName);
        // Register the structure BEFORE reconcileLeadersFromZk/writeStateUpdatesInternal. The latter
        // submits publishToStatePlane onto the taskZkWriterExecutor, which reads cs.get(collection) to
        // map replicas to shards. If cs.put ran afterwards the async publish could race ahead, find no
        // DocCollection, and silently defer the reconciled LEADER (see publishToStatePlane). In a
        // quiescent cluster (e.g. every other replica just killed) there is no later write to flush that
        // deferred state, so the just-reconciled leader never reaches the delta plane and readers hang in
        // getLeaderRetry. submit-happens-before-run on the executor guarantees the async publish observes
        // this put.
        cs.put(collectionName, docState);
        indexCollection(docState);
        if (weAreReplacement) {
          // Overseer-takeover leader reconciliation. A leader-promotion StateUpdate routed to the
          // previous overseer can be lost if that overseer (frequently the same node as the outgoing
          // leader) dies after the queue node is consumed but before the delta is durable. The shard
          // leader registration node (.../leaders/<shard>/leader/<internalId>) is EPHEMERAL on the live
          // winning replica's session, so it survives the overseer handoff and is the authoritative
          // record of who holds leadership. Re-assert it into the state-update map so the write below
          // republishes LEADER into the delta plane, repairing any promotion lost in the handoff window.
          reconcileLeadersFromZk(collectionName, docState);

          writeStateUpdatesInternal(Collections.singleton(docState.getId()), 1);
        } else {
//          Map<Integer,Integer> su = stateUpdates.get(docState.getId());
//          if (su != null) {
//            su.clear();
//            writeStateUpdatesInternal(Collections.singleton(docState.getId()), 1);
//          }
        }

        if (docState.getId() > highId[0]) {
          highId[0] = docState.getId();
        }

        DocAssign docAssign = new DocAssign();
        assignMap.put(collectionName, docAssign);
        int max = 1;
        Collection<Slice> slices = docState.getSlices();
        for (Slice slice : slices) {
          Collection<Replica> replicas = slice.getReplicas();

          for (Replica replica : replicas) {
            // Seed from the authoritative internal id (the value getReplicaAssignCnt()
            // hands out), not just the core-name suffix. Split sub-shard replicas can have
            // an internal id that differs from their core-name suffix (buildSolrCoreName
            // mints a paired core-name+id but the split path discards the id, so AddReplicaCmd
            // mints a separate id). Seeding only from the suffix can leave the counter below
            // an in-use id; a later incrementAndGet() then re-mints a live id and two replicas
            // collide on the same (collId,id) delta-plane key.
            Integer internalId = replica.getInternalId();
            if (internalId != null) {
              max = Math.max(max, internalId);
            }
            Matcher matcher = Assign.pattern.matcher(replica.getName());
            if (matcher.matches()) {
              int val = Integer.parseInt(matcher.group(1));
              max = Math.max(max, val);
            }
          }
        }
        docAssign.replicaAssignCnt.set(max);

      });

      ID.set(highId[0]);

      reader.registerLiveNodesListener((oldLiveNodes, newLiveNodes) -> {

        Set<String> lostLiveNodes = new HashSet<>(oldLiveNodes);
        lostLiveNodes.removeAll(newLiveNodes);
        log.info("Detected nodes that went down, removing states for nodes=[{}]...", lostLiveNodes);
        if (lostLiveNodes.isEmpty()) {
          return false;
        }
        // finding #6: resolve the affected replicas through the nodeName->placement index so this scales
        // O(replicas on the lost nodes), not O(all collections * all replicas). Aggregate per collection so
        // each collection's stateUpdates entry is computed exactly once.
        Map<Integer,Set<Integer>> downByColl = new HashMap<>();
        for (String node : lostLiveNodes) {
          for (Map.Entry<Integer,Set<Integer>> e : getReplicasOnNode(node).entrySet()) {
            downByColl.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue());
          }
        }
        for (Map.Entry<Integer,Set<Integer>> e : downByColl.entrySet()) {
          final Integer collId = e.getKey();
          final Set<Integer> downIds = e.getValue();
          stateUpdates.compute(collId, (integer, integerIntegerMap) -> {
            if (integerIntegerMap == null) {
              return null;
            }
            boolean write = false;
            for (Integer internalId : downIds) {
              if (log.isDebugEnabled()) {
                log.debug("Set an inactive state for replica id={} on a lost node for collection {} ...", internalId, collId);
              }
              // Mark the replica DOWN rather than removing its entry. Removing it left readers with the
              // last applied state (often ACTIVE/LEADER), and fresh DocCollections fall back to the stale
              // state.json baseline — so the instant the node rejoined live nodes, the replica looked
              // ACTIVE to every reader until the RECOVERYNODE update landed, letting waitForState-style
              // checks race past an in-progress recovery.
              integerIntegerMap.put(internalId, Replica.State.getShortState(Replica.State.DOWN));
              // Record the downed replica so the publish carries only these DOWN entries (finding #1),
              // not the whole collection map. Drained by writeStateUpdatesInternal.
              pendingChangedIds.compute(collId, (k, set) -> {
                Set<Integer> s = (set == null) ? ConcurrentHashMap.newKeySet() : set;
                s.add(internalId);
                return s;
              });
              write = true;
            }
            if (write) {
              // Write only this collection's id; a shared growing union across collections re-submitted
              // already-written collections and could write a collection whose DOWN compute() had not run yet.
              writeStateUpdatesInternal(Collections.singleton(collId), 1);
            }
            return integerIntegerMap;
          });
        }

        return false;
      });

      if (log.isDebugEnabled()) log.debug("zkStateWriter starting with cs {}", cs);
    } catch (Exception e) {
      log.error("Exception in ZkStateWriter init", e);
    }
  }

  // Overseer-takeover leader reconciliation (delta plane only). When a new overseer takes over,
  // a leader promotion that was published to the in-memory stateUpdates map but not yet made
  // durable in the delta plane can be lost if the prior overseer died mid-write. The EPHEMERAL
  // shard-leader registration node lives on the winning replica's ZK session and survives the
  // handoff, so it is the authoritative source of the current leader. Re-assert that replica as
  // LEADER into stateUpdates so the subsequent writeStateUpdatesInternal republishes it. Idempotent.
  private void reconcileLeadersFromZk(String collectionName, DocCollection docState) {
    int leaderShort = Replica.State.getShortState(Replica.State.LEADER);
    for (Slice slice : docState.getSlices()) {
      String leaderPath = ZkStateReader.getShardLeadersPath(collectionName, slice.getName());
      List<String> children;
      try {
        children = reader.getZkClient().getChildren(leaderPath, null, true);
      } catch (KeeperException.NoNodeException e) {
        continue;
      } catch (Exception e) {
        log.warn("Leader reconciliation: failed reading leader node {}", leaderPath, e);
        continue;
      }
      if (children == null || children.isEmpty()) {
        continue;
      }
      // Normally exactly one EPHEMERAL leader-registration child exists. A contested election or a
      // not-yet-expired stale session can transiently leave more than one, and getChildren order is
      // undefined — so picking children.get(0) could re-assert a STALE leader. Select the most recently
      // created registration (highest czxid), i.e. the latest election winner, skipping non-numeric or
      // raced-away children defensively (finding #9).
      if (children.size() > 1) {
        log.warn("Leader reconciliation: {} leader-registration children under {} {}; choosing most recent",
            children.size(), leaderPath, children);
      }
      Integer chosenId = null;
      long bestCzxid = Long.MIN_VALUE;
      for (String child : children) {
        final int parsedId;
        try {
          parsedId = Integer.parseInt(child);
        } catch (NumberFormatException e) {
          log.warn("Leader reconciliation: unexpected leader node child {} under {}", child, leaderPath);
          continue;
        }
        try {
          Stat st = reader.getZkClient().exists(leaderPath + "/" + child, null, true);
          if (st == null) {
            continue; // raced away between getChildren and exists
          }
          if (st.getCzxid() > bestCzxid) {
            bestCzxid = st.getCzxid();
            chosenId = parsedId;
          }
        } catch (Exception e) {
          log.warn("Leader reconciliation: failed reading leader child {} under {}", child, leaderPath, e);
        }
      }
      if (chosenId == null) {
        continue;
      }
      final Integer leaderInternalId = chosenId;
      stateUpdates.compute(docState.getId(), (k, v) -> {
        Map<Integer,Integer> m = (v == null) ? new ConcurrentHashMap<>() : v;
        m.put(leaderInternalId, leaderShort);
        return m;
      });
      // Record the re-asserted leader so the subsequent publish carries only this entry (finding #1).
      pendingChangedIds.compute(docState.getId(), (k, set) -> {
        Set<Integer> s = (set == null) ? ConcurrentHashMap.newKeySet() : set;
        s.add(leaderInternalId);
        return s;
      });
      if (log.isDebugEnabled()) {
        log.debug("Leader reconciliation: re-asserted LEADER collection={} shard={} internalId={}",
            collectionName, slice.getName(), leaderInternalId);
      }
    }
  }

  public Set<String> getCollections() {
    return cs.keySet();
  }

  /**
   * Enqueue a delta-plane publish for {@code collIds} and return a future that completes once that
   * publish is DURABLE in ZooKeeper (finding #5). The Worker coalesces queued units into batched
   * {@link #writeStateUpdatesInternal} calls and completes every unit's future when its batch's
   * publish finishes (exceptionally if the publish failed). {@link WorkQueueWatcher} gates the overseer
   * queue-item delete on this future so an item is never removed before its state is durably appended.
   */
  public CompletableFuture<Void> writeStateUpdates(Set<Integer> collIds) throws InterruptedException {
    Set<Integer> workSet = ConcurrentHashMap.newKeySet(collIds.size());
    workSet.addAll(collIds);
    CompletableFuture<Void> done = new CompletableFuture<>();
    workQueue.put(new WriteUnit(workSet, done));
    return done;
  }

  private static final byte VERSION = 2;

  /**
   * A queued batch of collection ids to publish to the delta plane, paired with a {@code done} future
   * completed once the publish is durable. The {@link #TERMINATED} sentinel carries null fields.
   */
  private static final class WriteUnit {
    final Set<Integer> collIds;
    final CompletableFuture<Void> done;

    WriteUnit(Set<Integer> collIds, CompletableFuture<Void> done) {
      this.collIds = collIds;
      this.done = done;
    }
  }

  private final static WriteUnit TERMINATED = new WriteUnit(null, null);

  public void stop() throws InterruptedException {


    //if (!workQueue.tryTransfer(TERMINATED)) {
    workQueue.put(TERMINATED);
    //  }

    if (workerExec != null) {
      workerExec.shutdown();
      workerExec.awaitTermination(5000, TimeUnit.MILLISECONDS);
    }

    // Final flush: publish whatever state is still buffered in the in-memory map and wait (bounded) for
    // it to be durable, so a clean overseer handoff does not drop the last updates.
    try {
      writeStateUpdatesInternal(stateUpdates.keySet(), 1).get(10000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | TimeoutException e) {
      log.warn("ZkStateWriter.stop final state flush did not complete cleanly", e);
    }
  }

  private static class DocAssign {

    DocAssign() {
    }

    private final AtomicInteger replicaAssignCnt = new AtomicInteger();
  }

  public static class StateUpdate {
    int id;
    Slice.State state;
    String sliceName;
    String nodeName;

    /** Build a slice-state update (used by inline callers outside this package, e.g. split cleanup). */
    public static StateUpdate forSlice(String sliceName, Slice.State state) {
      StateUpdate su = new StateUpdate();
      su.sliceName = sliceName;
      su.state = state;
      return su;
    }
  }

  /**
   * Schedule a per-collection delta-plane publish for each id in {@code collIds} and return a future
   * that completes once ALL of those publishes have durably appended to ZK (finding #5). Each publish
   * runs as a {@link CompletableFuture#runAsync} task on the {@code taskZkWriterExecutor}; a task that
   * throws completes its future exceptionally, which propagates through the aggregate so the queue-item
   * delete is held back and the item is reprocessed.
   */
  private CompletableFuture<Void> writeStateUpdatesInternal(Set<Integer> collIds, int tryCnt) {
    log.debug("writeStateUpdates for {}", collIds);
    List<CompletableFuture<Void>> futures = new ArrayList<>(collIds.size());
    for (Integer collId : collIds) {
      String collection = idToCollection.get(collId);

      futures.add(CompletableFuture.runAsync(() -> {

        if (collection == null) {
          // Unknown collection id (review P0 #2). Two cases:
          //  (a) the collection was REMOVED (in removedCollIds) — its id will never resolve. Drop the
          //      armed state and complete NORMALLY so WorkQueueWatcher deletes the queue item (option 3).
          //      Same disposition once an id has been retained past the bounded removed-collection proof, which guards
          //      a stale item from a long-dead incarnation this overseer never observed being removed.
          //  (b) the structure simply has not LANDED yet — keep pendingChangedIds / stateUpdates armed
          //      and complete EXCEPTIONALLY so WorkQueueWatcher RETAINS the queue item for reprocess
          //      (option 1). enqueueStructureChange republishes the armed ids the moment the structure
          //      lands (option 2), and the reprocessed item also resolves then. Both are idempotent.
          // The previous code always returned normally here, letting the durable queue item be deleted
          // though no delta was ever appended — a silent lost update if the overseer failed over before a
          // later write flushed the in-memory state.
          int attempts = unknownIdAttempts.merge(collId, 1, Integer::sum);
          if (removedCollIds.contains(collId)) {
            unknownIdAttempts.remove(collId);
            pendingChangedIds.remove(collId);
            stateUpdates.remove(collId);
            log.warn("writeStateUpdates: dropping state update for unknown collection id {} ({}); "
                    + "structure will never land",
                collId, removedCollIds.contains(collId) ? "collection removed" : "not yet proven removed");
            return;
          }
          log.debug("writeStateUpdates: structure for collection id {} not yet known (attempt {}); "
              + "retaining queue item for reprocess", collId, attempts);
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
              "structure for collection id " + collId + " not yet known; retaining for reprocess");
        }
        // Structure resolved — clear any unknown-id retry accounting for this id.
        unknownIdAttempts.remove(collId);

        ActionThrottle writeThrottle = stateWriteThrottles.compute(collection, (s, throttle) -> {
          if (throttle == null) {
            return new ActionThrottle("zkstatewriter", Integer.getInteger("solr.zkstateWriteThrottle", 0));
          }
          return throttle;
        });

        writeThrottle.minimumWaitBetweenActions();
        writeThrottle.markAttemptingAction();

        // Drain the changed-id set for this collection (finding #1). Non-null -> publish only those
        // replicas' current states (the writer computes any stale-leader demotions itself, D5). Null ->
        // no per-id tracking for this write (stop()/takeover or any untracked caller) -> full-map
        // republish, which is always safe, just larger.
        final Set<Integer> changed = pendingChangedIds.remove(collId);
        HashMap<Integer,Integer> javaBinMap = new HashMap<>(16);
        stateUpdates.compute(collId, (id, idToStateMap) -> {
          log.debug("writeStateUpdates for collection {} updates={} changed={}", collId, idToStateMap, changed);
          if (idToStateMap == null) {
            idToStateMap = new ConcurrentHashMap<>(16);
          }

          if (changed == null) {
            javaBinMap.putAll(idToStateMap);
          } else {
            for (Integer cid : changed) {
              Integer st = idToStateMap.get(cid);
              if (st != null) {
                javaBinMap.put(cid, st);
              }
            }
          }

          return idToStateMap;
        });

        // Route replica state updates through the per-shard delta plane. Runs on the
        // taskZkWriterExecutor, outside any stateUpdates.compute() bin lock.
        stateUpdateWrites.mark();
        try {
          publishToStatePlane(collection, javaBinMap);
        } catch (RuntimeException ex) {
          // The publish failed AFTER we drained `changed` (e.g. structure not yet known, or the manifest
          // seed failed). Re-arm the drained ids so a subsequent write re-publishes them — the in-memory
          // stateUpdates map still holds their states. The exception also completes this future
          // exceptionally so WorkQueueWatcher holds the queue item for reprocess. Without re-arming, a
          // post-drain failure would permanently lose these ids from the delta plane. (finding #1)
          if (changed != null && !changed.isEmpty()) {
            pendingChangedIds.compute(collId, (k, set) -> {
              Set<Integer> s = (set == null) ? ConcurrentHashMap.newKeySet() : set;
              s.addAll(changed);
              return s;
            });
          }
          throw ex;
        }
      }, overseer.getTaskZkWriterExecutor()));
    }

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
  }

  private final LinkedTransferQueue<WriteUnit> workQueue = new LinkedTransferQueue<>();

  private volatile boolean terminated;
  private class Worker implements Runnable {

    Worker() {

    }

    @Override public void run() {

      while (!terminated) {
        try {
          WriteUnit message = null;
          try {
            log.debug("ZkStateWriter worker will poll for 5 seconds");
            message = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            message = TERMINATED;
            terminated = true;
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          Set<Integer> bulkMessage = ConcurrentHashMap.newKeySet();
          // Futures of every coalesced unit in this batch; completed (or failed) together once the
          // batch's publish is durable. WorkQueueWatcher gates queue-item deletes on these.
          List<CompletableFuture<Void>> pendingDone = new ArrayList<>();
          if (message != null) {
            log.debug("Got state message {}", message);

            int pollTime;
            if (message == TERMINATED) {
              log.debug("State publish is terminated");
              terminated = true;
              pollTime = 0;
            } else {
              pollTime = bulkMessage(message, bulkMessage, pendingDone);
            }

            while (true) {
              try {
                message = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);
              } catch (InterruptedException e) {
                message = TERMINATED;
                terminated = true;
              } catch (Exception e) {
                log.warn("zkstate writer hit exception polling", e);
              }
              if (message != null) {
                if (message == TERMINATED) {
                  terminated = true;
                  pollTime = 0;
                } else {
                  pollTime = bulkMessage(message, bulkMessage, pendingDone);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage.size() > 0 || !pendingDone.isEmpty()) {
            CompletableFuture<Void> batch;
            try {
              batch = writeStateUpdatesInternal(bulkMessage, 1);
            } catch (Throwable t) {
              log.error("Exception scheduling state-plane publish for {}", bulkMessage, t);
              batch = new CompletableFuture<>();
              batch.completeExceptionally(t);
            }
            final List<CompletableFuture<Void>> toComplete = pendingDone;
            batch.whenComplete((v, ex) -> {
              for (CompletableFuture<Void> d : toComplete) {
                if (ex == null) {
                  d.complete(null);
                } else {
                  d.completeExceptionally(ex);
                }
              }
            });
          }

        } catch (Exception e) {
          log.error("Exception in ZkStateWriter Worker run loop", e);
        }
      }
    }

    private int bulkMessage(WriteUnit unit, Set<Integer> bulkColIds, List<CompletableFuture<Void>> pendingDone) {
      bulkColIds.addAll(unit.collIds);
      if (unit.done != null) {
        pendingDone.add(unit.done);
      }
      return 50;
    }
  }

}

