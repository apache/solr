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

import com.codahale.metrics.Meter;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.metrics.Metrics;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ZkStateReaderQueue implements Closeable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Meter stateUpdateRequests = Metrics.MARKS_METRICS.meter("zkreader_stateupdates_requests");
  private static final Meter docCollUpdateRequests = Metrics.MARKS_METRICS.meter("zkreader_doccollupdates_requests");

  private final static FetchStateUpdatesRequest TERMINATED = new FetchStateUpdatesRequest(null, null, null, false);
  public static final byte[] EMPTY_BYTES = new byte[0];

  private final LinkedTransferQueue<FetchStateUpdatesRequest> workQueue = new LinkedTransferQueue<>();

  /**
   * Bounds how many times a single collection's state fetch is automatically re-enqueued after a
   * transient ZooKeeper failure before we give up and rely on the next watch event / session
   * reconnect resync. The counter is cleared on the first successful fetch, so a steady stream of
   * isolated flaps never exhausts the budget.
   */
  private static final int MAX_FETCH_RETRIES = 5;

  private final ConcurrentHashMap<String, AtomicInteger> fetchRetries = new ConcurrentHashMap<>();

  private final SolrZkClient zkClient;
  private final ZkStateReader reader;

  private volatile Worker worker;

  private volatile boolean terminated;
  private volatile boolean closed;

  private volatile ExecutorService workerExec;


  public ZkStateReaderQueue(ZkStateReader reader) {
   this.zkClient = reader.getZkClient();
   this.reader = reader;
  }

  public void close() {
    this.closed = true;

    //    if (!workQueue.tryTransfer(TERMINATED)) {
    workQueue.put(TERMINATED);
    //    }
    if (workerExec != null) {
      workerExec.shutdown();
      try {
        workerExec.awaitTermination(5000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {

      }
    }
  }

  private static class BulkMessage extends ConcurrentHashMap<String, Set<FetchStateUpdatesRequest>> {

  }

  /**
   * Retry-budget key for {@link #fetchRetries}. A full fetch ({@code justStates == false}) rebuilds
   * the whole collection, so it uses the bare collection name as its single scope. A delta apply
   * keys by collection + the targeted shard scope so one shard's run of transient failures neither
   * exhausts nor resets another shard's budget; a null/empty scope (a manifest / full-apply event)
   * is its own collection-level delta scope. Collection names cannot contain {@code "::"} (enforced by
   * {@link org.apache.solr.common.SolrIdentifierValidator} at every collection-creation entry point),
   * so the scopes never collide with the bare full-fetch key.
   */
  private static String retryKey(String collection, boolean justStates, Set<String> targetShards) {
    if (!justStates) {
      return collection;
    }
    if (targetShards == null || targetShards.isEmpty()) {
      return collection + "::*";
    }
    return collection + "::" + new java.util.TreeSet<>(targetShards);
  }

  /**
   * Drop every retry-budget entry for a collection: its full-fetch key and all per-shard delta
   * scopes. A successful full fetch supersedes all outstanding shard state, so it bounds counter
   * leaks (e.g. a shard scope that stopped firing under bulk-message coalescing) rather than letting
   * orphaned per-shard counters accumulate.
   */
  private void clearCollectionRetries(String collection) {
    fetchRetries.remove(collection);
    final String prefix = collection + "::";
    fetchRetries.keySet().removeIf(k -> k.startsWith(prefix));
  }

  private class Worker implements Runnable {

    public static final int POLL_TIME_ON_PUBLISH_NODE = 1;
    public static final int POLL_TIME = 250;

    Worker() {

    }

    // section worker run
    @Override public void run() {
      MDCLoggingContext.setNode(reader.node);
      while (!terminated && !closed) {
        try {

          FetchStateUpdatesRequest updateRequest = null;
          try {
            log.debug("ZkStateReaderQueue worker will poll for 5 seconds");
            updateRequest = workQueue.poll(5000, TimeUnit.MILLISECONDS);
          } catch (InterruptedException e) {
            updateRequest = TERMINATED;
            terminated = true;
          } catch (Exception e) {
            log.warn("state publisher hit exception polling", e);
          }
          BulkMessage bulkMessage = null;
          if (updateRequest != null) {
            log.debug("got state update request collection={} structureUpdateToo={}", updateRequest.collection, !updateRequest.justStates);
            bulkMessage = new BulkMessage();

            int pollTime;
            if (updateRequest == TERMINATED) {
              terminated = true;
              pollTime = 0;
            } else {
              pollTime = bulkMessage(updateRequest, bulkMessage);
            }

            while (true) {
              try {
                updateRequest = workQueue.poll(pollTime, TimeUnit.MILLISECONDS);

              } catch (InterruptedException e) {
                updateRequest = TERMINATED;
                terminated = true;
              } catch (Exception e) {
                log.warn("ZkStateReaderQueue hit exception polling", e);
              }
              if (updateRequest != null) {
                log.debug("got state update request collection={}", updateRequest.collection);
                if (updateRequest == TERMINATED) {
                  terminated = true;
                  pollTime = 0;
                } else {
                  pollTime = bulkMessage(updateRequest, bulkMessage);
                }
              } else {
                break;
              }
            }
          }

          if (bulkMessage != null && bulkMessage.size() > 0) {
            process(bulkMessage);
          }

        } catch (Exception e) {
          log.error("Exception in ZkStateReaderQueue Worker run loop", e);
        }

      }
      log.info("ZkStateReaderQueue has terminated");
    }

    private void process(BulkMessage bulkMessage) {
      try {
        log.debug("process state update requests {}", bulkMessage);

        bulkMessage.forEach((collection, fetchStateUpdatesRequests) -> {
          boolean justStates = true;
          for (FetchStateUpdatesRequest fetch : fetchStateUpdatesRequests) {
            if (!fetch.justStates) {
              justStates = false;
              break;
            }
          }

          try {
            if (!justStates) {
              log.debug("fetchCollectionState {}", collection);
              fetchCollectionState(collection).thenCompose(docCollection1 -> {
                // PR-3 delta plane: re-apply the live state plane onto the freshly-fetched structure
                // before installing it. The structure (state.json) and the delta plane are watched on
                // two independent ZK nodes, so a leader/active delta can arrive (and be processed
                // against a structure that does not yet contain that replica id) BEFORE the structure
                // that defines the replica. The justStates path then skips the un-seeded id but still
                // advances the per-shard cursor past it (StatePlaneReader logs "skipping un-seeded
                // replica"), so the raced-ahead state is lost forever. A full fetch builds a fresh
                // DocCollection with fresh (epoch=0) cursors that now seeds the replica, so re-running
                // the delta apply reconstructs the current state from the snapshot+ring and recovers
                // the lost leader/active state — self-healing convergence independent of watch order.
                // ZkStateReader.updateWatchedCollection's carryForward already assumes the full-fetch
                // path delivers freshly-applied updates and preserves whichever side is newer.
                if (docCollection1 != null) {
                  return getAndProcessDeltaUpdates(docCollection1);
                }
                return CompletableFuture.completedFuture(docCollection1);
              }).thenAcceptAsync(docCollection1 -> {
                clearCollectionRetries(collection);
                reader.updateWatchedCollection(collection, new ClusterState.CollectionRef(docCollection1));
              }).exceptionally(t -> {
                // A failed full fetch must not be silently swallowed: the triggering watch event has
                // already been consumed (the recursive ZK watch does not replay events missed during
                // a disconnect) and a transient ConnectionLoss does not fire OnReconnect resync, so
                // without re-fetching the node's view of the collection can go permanently stale and
                // getLeaderRetry will spin until timeout. Re-enqueue on transient ZK failures.
                log.error("fetchCollectionState failed coll={}", collection, t);
                scheduleRetryOnTransientFailure(collection, false, null, t);
                return null;
              });

            } else {
              // Targeted-shard delta apply (finding #2): fold ONLY the shards whose delta plane fired,
              // not every shard of the collection. If any coalesced request carries a null shard (a
              // manifest event, which is a collection-level switch onto the plane), fall back to the
              // full delta apply so a freshly-seeded collection still folds all shards.
              Set<String> targetShards = new HashSet<>();
              boolean allShardsApply = false;
              for (FetchStateUpdatesRequest fetch : fetchStateUpdatesRequests) {
                if (fetch.shard == null) {
                  allShardsApply = true;
                  break;
                }
                targetShards.add(fetch.shard);
              }
              final Set<String> shardsArg = allShardsApply ? null : targetShards;
              log.debug("getAndProcessStateUpdates {} shards={}", collection, shardsArg);
              getAndProcessStateUpdates(reader.watchedCollectionStates.get(collection), shardsArg).thenAcceptAsync(docCollection1 -> {
                // The collection can be demoted to lazy between the watch event and this drain, in
                // which case watchedCollectionStates.get returns null and getAndProcessStateUpdates
                // yields null; passing a null-wrapped ref through is a no-op, so guard it explicitly.
                if (docCollection1 == null) {
                  return;
                }
                fetchRetries.remove(retryKey(collection, true, shardsArg));
                reader.updateWatchedCollection(collection, new ClusterState.CollectionRef(docCollection1));
              }).exceptionally(t -> {
                // Mirror the full-fetch branch: re-enqueue on transient ZK failures so a missed
                // delta apply does not leave the node's view permanently stale.
                log.error("getAndProcessStateUpdates failed coll={}", collection, t);
                // Carry the target shards through so a transient one-shard failure retries scoped to
                // those shards instead of degrading to a full-collection fold. shardsArg == null (a
                // manifest/full-apply event) correctly retries as a full apply. (finding #5)
                scheduleRetryOnTransientFailure(collection, true, shardsArg, t);
                return null;
              });
            }
          } catch (Exception e) {
            log.error("Exception processing state update request", e);
            fetchStateUpdatesRequests.forEach(fetchStateUpdatesRequest -> fetchStateUpdatesRequest.future.completeExceptionally(e));
          }
        });

      } catch (Exception e) {
        log.error("Exception processing state update request", e);
      }
    }

    /**
     * Re-enqueue a failed state fetch when the failure looks transient (a ZooKeeper connection-level
     * error rather than a structural one like NoNode). The re-fetch is deferred onto a shared
     * executor that first waits for the ZK connection to be re-established, so we never hot-spin while
     * the ensemble is unreachable. Retries are bounded per collection and the counter resets on the
     * next successful fetch.
     */
    private void scheduleRetryOnTransientFailure(String collection, boolean justStates, Set<String> targetShards, Throwable t) {
      // Bound retries per (collection, shard-scope) rather than per collection, so one shard's run of
      // persistent transient failures neither exhausts nor resets the retry budget of the
      // collection's other shards. A full fetch supersedes all shard scopes and clears them on
      // success via clearCollectionRetries.
      final String key = retryKey(collection, justStates, targetShards);
      if (closed || terminated) {
        fetchRetries.remove(key);
        return;
      }
      if (!isTransientZkFailure(t)) {
        // Structural failures (e.g. NoNode for a deleted collection) are not recoverable by retry;
        // drop any accumulated retry budget for this scope.
        fetchRetries.remove(key);
        return;
      }

      int attempt = fetchRetries.computeIfAbsent(key, k -> new AtomicInteger()).incrementAndGet();
      if (attempt > MAX_FETCH_RETRIES) {
        log.error("Giving up re-fetching state for collection={} scope={} after {} transient ZK failures; the next watch event or session reconnect will resync", collection, key, MAX_FETCH_RETRIES);
        fetchRetries.remove(key);
        return;
      }

      log.warn("Re-enqueuing state fetch for collection={} justStates={} targetShards={} after transient ZK failure (attempt {}/{})", collection, justStates, targetShards, attempt, MAX_FETCH_RETRIES);
      ParWork.getRootSharedExecutor().submit(() -> {
        try {
          int waitMs = zkClient.getZkClientTimeout();
          zkClient.getConnectionManager().waitForConnected(waitMs > 0 ? waitMs : 30000);
        } catch (Exception e) {
          // Fall through and re-enqueue anyway; if ZK is still unreachable the worker's fetch will
          // fail again and either retry or exhaust the bounded budget.
          log.debug("waitForConnected before state re-fetch interrupted/failed coll={}", collection, e);
        }
        if (!closed && !terminated) {
          // Preserve shard targeting on retry (finding #5): a non-null targetShards re-enqueues a
          // scoped per-shard fetch; null (full-fetch or manifest event) re-enqueues the full apply.
          if (justStates && targetShards != null) {
            for (String shard : targetShards) {
              fetchStateUpdates(collection, shard, true);
            }
          } else {
            fetchStateUpdates(collection, justStates);
          }
        }
      });
    }

    private int bulkMessage(FetchStateUpdatesRequest fetchStateUpdatesRequest, BulkMessage bulkMessage) {
      if (fetchStateUpdatesRequest == TERMINATED) {
        return 1;
      }

      Set<FetchStateUpdatesRequest> currentSet = bulkMessage.computeIfAbsent(fetchStateUpdatesRequest.collection, integer -> ConcurrentHashMap.newKeySet());
      currentSet.add(fetchStateUpdatesRequest);
      return 20;
    }
  }

  public void start() {
    if (worker != null) {
      return;
    }
    this.worker = new Worker();

    workerExec = Executors.newSingleThreadExecutor(new SolrNamedThreadFactory("ZkStateReaderQueue", true));

    workerExec.submit(this.worker);
  }

  /**
   * Entry point for applying live replica-state updates onto a {@link DocCollection}. Routes to the
   * per-shard delta plane ({@link #getAndProcessDeltaUpdates}), which consumes per-shard
   * {@code (epoch, seq)} delta rings + snapshots.
   */
  public CompletableFuture<DocCollection> getAndProcessStateUpdates(DocCollection docCollection) {
    return getAndProcessStateUpdates(docCollection, null);
  }

  /**
   * Targeted-shard variant (finding #2). {@code targetShards == null} folds every shard (structure
   * refresh or manifest switch); a non-null set folds only those shards, skipping the per-shard ring
   * read for every other shard.
   */
  public CompletableFuture<DocCollection> getAndProcessStateUpdates(DocCollection docCollection, Set<String> targetShards) {
    return getAndProcessDeltaUpdates(docCollection, targetShards);
  }

  /**
   * Delta-plane apply path. Reads each shard's delta ring (and, when the reader has fallen behind the
   * ring, the per-shard snapshot) and folds them onto {@code docCollection} via {@link
   * StatePlaneReader}, advancing the per-shard {@code (epoch, seq)} cursors.
   *
   * <p>If the collection's {@code state/manifest} node does not yet exist, the delta plane has not been
   * seeded for this collection — there is no live state to apply, so the structure is returned
   * unchanged. The manifest is published LAST by the writer, so its presence is the authoritative
   * switch onto the plane.
   */
  public CompletableFuture<DocCollection> getAndProcessDeltaUpdates(DocCollection docCollection) {
    return getAndProcessDeltaUpdates(docCollection, null);
  }

  /**
   * Targeted-shard delta-plane apply (finding #2). When {@code targetShards} is non-null, only those
   * shards' rings are read and folded — every other shard's ring read is skipped — so a delta event on
   * one shard costs O(1) ZK reads instead of O(shards). A null {@code targetShards} folds all shards
   * (structure refresh / manifest switch / first seed).
   */
  public CompletableFuture<DocCollection> getAndProcessDeltaUpdates(DocCollection docCollection, Set<String> targetShards) {
    stateUpdateRequests.mark();
    try {
      if (docCollection == null) {
        log.debug("Null docCollection as argument (delta)");
        return CompletableFuture.completedFuture(null);
      }

      String collectionPath = StatePlanePaths.collectionPath(docCollection.getName());
      String manifestPath = StatePlanePaths.manifest(collectionPath);

      Stat manifestStat = zkClient.exists(manifestPath, null, true);
      if (manifestStat == null) {
        // Plane not seeded for this collection yet — no live state to apply. The writer seeds the
        // manifest LAST on first publish, so its absence means there are no deltas to fold.
        log.debug("No state plane manifest at {}; no-op", manifestPath);
        return CompletableFuture.completedFuture(docCollection);
      }

      StatePlaneCursors cursors = docCollection.getOrCreateStatePlaneCursors();

      for (Slice slice : docCollection.getSlices()) {
        String shard = slice.getName();
        if (targetShards != null && !targetShards.contains(shard)) {
          // finding #2: this shard's delta plane did not fire — skip its ring read entirely.
          continue;
        }
        String deltaPath = StatePlanePaths.shardDeltas(collectionPath, shard);

        byte[] ringBytes;
        try {
          ringBytes = zkClient.getData(deltaPath, null, new Stat(), true);
        } catch (KeeperException.NoNodeException nne) {
          // No deltas written for this shard yet.
          continue;
        }
        if (ringBytes == null || ringBytes.length == 0) {
          continue;
        }

        ShardStateLog ring = StateDeltaCodec.decodeShardStateLog(ringBytes);
        long[] cursor = cursors.get(shard);

        // PR-5 (D5): force a snapshot catch-up when far behind the ring head (readerCatchupLimit) or
        // when the ring carries more deltas than maxDeltaFetch — rebasing from the snapshot is cheaper
        // than folding an unbounded incremental tail. Compaction advances ring.baseSeq, which already
        // flips needsSnapshotCatchup() true for any reader that missed the folded prefix.
        int catchupLimit = StateDeltaConfig.readerCatchupLimit();
        int maxDeltaFetch = StateDeltaConfig.maxDeltaFetch();
        boolean tooManyDeltas = maxDeltaFetch > 0 && ring.entries.size() > maxDeltaFetch;
        if (tooManyDeltas || StatePlaneReader.needsSnapshotCatchup(ring, cursor, catchupLimit)) {
          StateSnapshot snapshot = readSnapshot(collectionPath, shard);
          StatePlaneReader.applySnapshotAndDeltas(docCollection, shard, snapshot, ring, cursors);
        } else {
          StatePlaneReader.applyRing(docCollection, shard, ring, cursors);
        }
      }

      return CompletableFuture.completedFuture(docCollection);

    } catch (Exception e) {
      log.error("{} exception trying to process delta state updates",
              docCollection == null ? "(null)" : docCollection.getName(), e);
      return CompletableFuture.failedFuture(new SolrException(SolrException.ErrorCode.SERVER_ERROR, e));
    }
  }

  /** Reads and decodes a shard's {@link StateSnapshot}; returns null if no snapshot has been written. */
  private StateSnapshot readSnapshot(String collectionPath, String shard) throws Exception {
    String snapshotPath = StatePlanePaths.shardSnapshot(collectionPath, shard);
    byte[] data;
    try {
      data = zkClient.getData(snapshotPath, null, new Stat(), true);
    } catch (KeeperException.NoNodeException nne) {
      return null;
    }
    if (data == null || data.length == 0) {
      return null;
    }
    return StateDeltaCodec.decodeStateSnapshot(data);
  }

  /**
   * Whether a failed state fetch should be retried. True only for ZooKeeper connection-level
   * failures (ConnectionLoss / OperationTimeout / Session{Expired,Moved}), which are recoverable by
   * re-fetching once the connection is back. Structural failures such as {@code NoNode} are not
   * retryable — retrying them would spin without ever succeeding. Unwraps the cause chain because the
   * fetch futures wrap the original {@link KeeperException} in {@link SolrException}.
   */
  static boolean isTransientZkFailure(Throwable t) {
    Throwable c = t;
    while (c != null) {
      if (c instanceof KeeperException.ConnectionLossException
          || c instanceof KeeperException.OperationTimeoutException
          || c instanceof KeeperException.SessionExpiredException
          || c instanceof KeeperException.SessionMovedException) {
        return true;
      }
      c = c.getCause();
    }
    return false;
  }

  public void fetchStateUpdates(String collection, boolean justStates) {
    fetchStateUpdates(collection, null, justStates);
  }

  /**
   * Enqueue a state fetch scoped to a single {@code shard} (finding #2): a delta event on
   * {@code /state/shards/<shard>/deltas} folds ONLY that shard's ring instead of every shard of the
   * collection. A null {@code shard} (a manifest event or a structure refresh) folds all shards.
   */
  public void fetchStateUpdates(String collection, String shard, boolean justStates) {
    log.debug("add update request to queue for collection={} shard={}", collection, shard);
//    if (closed) {
//      throw new AlreadyClosedException();
//    }

    if (collection == null) {
      log.error("null collection name passed to fetchStateUpdates");
      throw new IllegalArgumentException();
    }

    FetchStateUpdatesRequest request = new FetchStateUpdatesRequest(collection, shard, null, justStates);

   // if (!workQueue.tryTransfer(request)) {
      workQueue.put(request);
  //  }

  }

  // region fetch
  CompletableFuture<DocCollection> fetchCollectionState(String collection) throws InterruptedException, KeeperException {
    String collectionPath = ZkStateReader.getCollectionPath(collection);
    if (log.isDebugEnabled()) log.debug("Looking at fetching full clusterstate collection={}", collection);

    docCollUpdateRequests.mark();

    log.debug("getting latest state.json for {}", collection);
    CompletableFuture<DocCollection> future = new CompletableFuture<>();
    CompletableFuture<DocCollection> returnFuture = future.thenCompose(docCollection -> getAndProcessStateUpdates(docCollection));
    zkClient.getData(collectionPath, null, (rc, path, ctx, zkdata, stat) -> {
      if (rc != 0) {
        KeeperException e = KeeperException.create(KeeperException.Code.get(rc), path);
        future.completeExceptionally(e);
      } else {
        future.complete(ClusterState.createDocCollectionFromJson(stat.getVersion(), zkdata));
      }

    }, true);

    return returnFuture;
//    return CompletableFuture.supplyAsync(() -> {
//      try {
//        Stat stat = new Stat();
//        byte[] data = zkClient.getData(collectionPath, null, stat);
//        return ClusterState.createDocCollectionFromJson(stat.getVersion(), data);
//
//      } catch (KeeperException.NoNodeException e) {
//        log.debug("no state.json znode found");
//        return null;
//      } catch (Exception e) {
//        log.debug("Exception getting and parsing state.json");
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Exception getting and parsing state.json", e);
//      }
//    }, ParWork.getRootSharedExecutor()).thenCompose(docCollection -> getAndProcessStateUpdates(docCollection));
  }

//  private void processDocCollection(FetchStateUpdatesRequest fetchStateUpdatesRequest, DocCollection docCollection) {
//    try {
//      MDCLoggingContext.setNode(reader.node);
//
//      if (log.isDebugEnabled()) {
//        log.debug("process doc collection docCollection={}", docCollection);
//        if (docCollection == null) {
//          log.debug("null docState", new RuntimeException());
//        }
//      }
//
//      DocCollection finalDocCollection = docCollection;
//      fetchStateUpdatesRequest.future.complete(finalDocCollection);
//
//      if (reader.updateWatchedCollection(fetchStateUpdatesRequest.collection, new ClusterState.CollectionRef(docCollection))) {
//        reader.notifyStateUpdated(fetchStateUpdatesRequest.collection, docCollection, "state.json watcher");
//      }
//
//    } catch (Exception e) {
//      log.error("Failed processing state update fetch for collection={}", fetchStateUpdatesRequest.collection, e);
//      fetchStateUpdatesRequest.future.completeExceptionally(e);
//      return;
//    }
//  }

  static class FetchStateUpdatesRequest {
    final String collection;
    /** The single shard whose delta plane fired (finding #2), or null for a collection-wide apply. */
    final String shard;
    volatile CompletableFuture<DocCollection> future;
    final boolean justStates;

    public FetchStateUpdatesRequest(String collection, String shard, CompletableFuture<DocCollection> docCollectionCompletableFuture, boolean justStates) {
      this.collection = collection;
      this.shard = shard;
      this.future = docCollectionCompletableFuture;
      this.justStates = justStates;
    }
  }

}
