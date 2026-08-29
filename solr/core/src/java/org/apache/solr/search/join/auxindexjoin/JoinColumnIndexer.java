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
package org.apache.solr.search.join.auxindexjoin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;
import org.apache.solr.search.join.auxindexjoin.AuxIndexManager.JoinSegmentReference;
import org.apache.solr.search.join.auxindexjoin.AuxIndexManager.SegmentsTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds missing pair columns for an {@link AuxIndexManager}: dedups concurrent builders per pair
 * field name, computes the in-memory {@link JoinColumnModel}s, double-checks a fresh join-index
 * searcher for pairs persisted since the caller looked, and persists what is still missing through
 * {@link AuxIndexManager#writeBatch}.
 */
final class JoinColumnIndexer {

  private final AuxIndexManager joinIndex;

  /**
   * Dedups concurrent builders per pair field name: the thread that installs the future writes the
   * pair, others wait on it. A future completes as soon as the pair's in-memory {@link
   * JoinColumnModel} is ready -- before the owning thread persists and refreshes -- so completion
   * does not imply the column is visible through the index's searcher manager yet. Entries are
   * transient: the owner removes its claims once the pair is persisted and refreshed (and on build
   * failure), so the map only ever holds in-flight builds and never pins models in heap. A builder
   * that races a not-yet-visible refresh and re-claims an already-persisted pair is stopped from
   * writing a duplicate column by {@code #writeJoinSegments}'s fresh-searcher double-check, not by
   * this map.
   */
  private final ConcurrentHashMap<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>>
      pairBuilds = new ConcurrentHashMap<>();

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  JoinColumnIndexer(AuxIndexManager joinIndex) {
    this.joinIndex = joinIndex;
  }

  /**
   * How many of the given pair field names have a build currently in flight in {@link #pairBuilds}
   * (claims are removed once persisted). Diagnostic only: a pair counted here but still reported
   * missing by {@link JoinIndexUtils#extractExistingJoinColumns} means the caller is about to redo
   * from-side work for a pair some other thread is building right now.
   */
  int countClaimedBuilds(Set<String> pairFieldNames) {
    int claimed = 0;
    for (String pairFieldName : pairFieldNames) {
      if (pairBuilds.containsKey(pairFieldName)) {
        claimed++;
      }
    }
    return claimed;
  }

  /**
   * Builds and persists the given missing pair columns, keyed by pair field name to their
   * (from-segment, to-segment) leaf ordinals. Pairs concurrently built by another thread are
   * awaited, not rebuilt; awaiting yields the builder's in-memory model as soon as it is computed,
   * possibly before the builder has committed it, so on return pairs built by this thread are
   * refreshed into the internal searcher manager, while awaited ones may not be visible there yet.
   *
   * <p>Claims in {@link #pairBuilds} are transient -- the owner removes them once its pairs are
   * persisted and refreshed, keeping the map bounded by in-flight builds. That makes re-claiming an
   * already-persisted pair an expected event: the caller decided "missing" against {@code
   * observedAbsentSearcher}, which may predate another thread's completed build. So after claiming,
   * this method re-checks a fresh searcher and skips persisting any pair that turns out already
   * written. Since only the claim owner ever writes a pair, absence in a post-claim fresh searcher
   * is conclusive, so no duplicate pair column can be written; rediscovered pairs still cost their
   * in-memory recompute, which the caller gets back in the result like any built pair. As a
   * shortcut, when {@link AuxIndexManager#acquire()} returns {@code observedAbsentSearcher} itself,
   * nothing was committed since that searcher was current, and the re-check scan is skipped.
   *
   * @param observedAbsentSearcher the join-index searcher in which the caller established that
   *     {@code missingPairs} are absent; possibly stale by now. Pass {@code null} when absence
   *     wasn't verified against a live searcher -- that forces the re-check scan.
   * @return in memory data for just written segemts
   */
  Map<String, JoinColumnModel> buildAndPersistJoinColumns(
      Map<String, SegmentsTuple> missingPairs,
      IndexReader fromReader,
      IndexReader toReader,
      String toField,
      String traceCtxId,
      IndexSearcher observedAbsentSearcher,
      Future<FromLeafJoinContext>[] fromColumnFutures)
      throws IOException, ExecutionException, InterruptedException {
    long startNanos = System.nanoTime();
    Claims claims = claimPairs(missingPairs.keySet());
    Map<String, JoinColumnModel> loadedMappings = Map.of();
    Set<String> writtenPairs = Set.of();
    try {
      if (!claims.owned().isEmpty()) {
        loadedMappings =
            computeOwnedModels(
                claims.owned().keySet(),
                missingPairs,
                fromReader,
                toReader,
                toField,
                fromColumnFutures);
        completeClaimFutures(claims.owned(), loadedMappings);
        Map<String, JoinColumnModel> unwrittenMappings =
            dropAlreadyPersisted(loadedMappings, claims.owned().keySet(), observedAbsentSearcher);
        persistBatch(unwrittenMappings);
        writtenPairs = unwrittenMappings.keySet();
        releaseClaims(claims.owned());
      }
    } catch (Throwable t) {
      withdrawClaims(claims.owned(), t);
      throw t;
    }
    long builtNanos = System.nanoTime() - startNanos;
    Map<String, JoinColumnModel> result = new LinkedHashMap<>(loadedMappings);
    collectAwaited(claims.awaited(), result);
    logBuildDiagnostics(
        traceCtxId,
        missingPairs.size(),
        loadedMappings,
        claims.awaited().size(),
        writtenPairs,
        startNanos,
        builtNanos);
    return result;
  }

  /**
   * The claim/await split of one build request: {@code owned} holds the futures this thread
   * installed into {@link #pairBuilds} (it must build, persist, and eventually remove them), {@code
   * awaited} the futures of pairs some other thread is already building.
   */
  private record Claims(
      Map<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> owned,
      List<CompletableFuture<Map.Entry<String, JoinColumnModel>>> awaited) {}

  /**
   * Races a claim into {@link #pairBuilds} for every requested pair: the thread whose future lands
   * owns that pair's build, a pair already claimed by another thread is awaited instead.
   */
  private Claims claimPairs(Set<String> pairFieldNames) {
    Map<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> owned =
        new LinkedHashMap<>();
    List<CompletableFuture<Map.Entry<String, JoinColumnModel>>> awaited = new ArrayList<>();
    for (String pairFieldName : pairFieldNames) {
      CompletableFuture<Map.Entry<String, JoinColumnModel>> created = new CompletableFuture<>();
      CompletableFuture<Map.Entry<String, JoinColumnModel>> existing =
          pairBuilds.putIfAbsent(pairFieldName, created);
      if (existing == null) {
        owned.put(pairFieldName, created);
      } else {
        awaited.add(existing);
      }
    }
    return new Claims(owned, awaited);
  }

  /** Computes the in-memory {@link JoinColumnModel} for every owned pair. */
  private static Map<String, JoinColumnModel> computeOwnedModels(
      Set<String> ownedPairs,
      Map<String, SegmentsTuple> missingPairs,
      IndexReader fromReader,
      IndexReader toReader,
      String toField,
      Future<FromLeafJoinContext>[] fromColumnFutures)
      throws IOException, ExecutionException, InterruptedException {
    Map<String, JoinColumnModel> loadedMappings = new LinkedHashMap<>();
    JoinIndexUtils.ToDocInvertor toInvertor = new JoinIndexUtils.ToDocInvertor(toField);
    for (String pairFieldName : ownedPairs) {
      SegmentsTuple position = missingPairs.get(pairFieldName);
      LeafReaderContext toContext = toReader.leaves().get(position.toLeafOrd());
      LeafReaderContext fromContext = fromReader.leaves().get(position.fromLeafOrd());
      assert fromColumnFutures[fromContext.ord] != null;
      JoinIndexUtils.JoinColumnModel mapping =
          JoinIndexUtils.computeDocMapping(
              toContext, toField, fromColumnFutures[fromContext.ord].get().fkColumn, toInvertor);
      loadedMappings.put(pairFieldName, mapping);
    }
    return loadedMappings;
  }

  /**
   * Completes every owned claim future with its computed model. Runs before {@link #persistBatch}:
   * waiters only need the in-memory model, so they don't pay for the owner's commit + refresh;
   * completion does NOT imply the column is visible through the searcher manager yet.
   */
  private static void completeClaimFutures(
      Map<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> owned,
      Map<String, JoinColumnModel> loadedMappings) {
    for (Map.Entry<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> entry :
        owned.entrySet()) {
      entry
          .getValue()
          .complete(
              new AbstractMap.SimpleImmutableEntry<>(
                  entry.getKey(), loadedMappings.get(entry.getKey())));
    }
  }

  /**
   * Double-checks the owned pairs against a fresh searcher, returning only the mappings still
   * unwritten: claims are dropped once persisted, so a caller deciding "missing" on a stale
   * searcher can re-claim an already-persisted pair; owning the claim means no one else can write
   * it concurrently, so what the fresh searcher lacks is conclusively unwritten.
   *
   * <p>It's a race condition tradeoff:
   *
   * <ol>
   *   <li>this thread didn't see a column and decided to compete for it.
   *   <li>another thread might have persisted it in the meantime, and removed it's claim from
   *       pairBuilds.
   *   <li>thread 1. calculate a join column model, and ready to write it
   *   <li>if a column occur in already persisted: it skip wtiting it, but returns the calculated
   *       memory model to the caller
   * </ol>
   *
   * overall, under high concurrency we waste some computation for sweeping pairBuilds;
   * alternatively we need to invent a way to remove entries from it somewhere later without a race
   */
  private Map<String, JoinColumnModel> dropAlreadyPersisted(
      Map<String, JoinColumnModel> loadedMappings,
      Set<String> ownedPairs,
      IndexSearcher observedAbsentSearcher)
      throws IOException {
    Map<String, JoinColumnModel> unwrittenMappings = loadedMappings;
    IndexSearcher freshJoinSearcher = joinIndex.acquire();
    try {
      if (freshJoinSearcher != observedAbsentSearcher) {
        Map<String, JoinSegmentReference> alreadyPersisted =
            JoinIndexUtils.extractExistingJoinColumns(freshJoinSearcher, ownedPairs::contains);
        if (!alreadyPersisted.isEmpty()) {
          unwrittenMappings = new LinkedHashMap<>(loadedMappings);
          unwrittenMappings.keySet().removeAll(alreadyPersisted.keySet());
        }
      } // else: same searcher instance the caller saw the pairs absent in -- nothing was
      // committed since, so the pairs are provably unwritten and the scan is redundant
    } finally {
      joinIndex.release(freshJoinSearcher);
    }
    return unwrittenMappings;
  }

  /**
   * Persists the still-unwritten mappings, if any. All pairs of a batch go in together: pair
   * columns are addressed by from-side doc id, so a batch must start at doc 0 of its sidecar
   * segment, which {@link AuxIndexManager#writeBatch} guarantees by flushing one batch per commit.
   */
  // TODO it can be more asynchronous, besides of pairBuilds removal - and it's a problem
  // would be great if many concurrent threads merge lists and write it at once,
  // since it's synchronized bottleneck for now.
  // and the thread don't even need to wait until writeBatch() is done (beside of pairBuilds
  // removal, you know)
  // TODO flush every single field to get single field segments
  private void persistBatch(Map<String, JoinColumnModel> unwrittenMappings) throws IOException {
    if (!unwrittenMappings.isEmpty()) {
      joinIndex.writeBatch(unwrittenMappings);
    }
  }

  /**
   * Drops the owned claims after a successful build: every owned pair is now persisted (just
   * written, or found persisted by the double-check) and visible to any searcher acquired from here
   * on, and duplicate writes are prevented by the double-check, not by retained entries -- dropping
   * them keeps {@link #pairBuilds} bounded by in-flight builds and unpins the models from heap.
   * Waiters already holding the futures are unaffected.
   */
  private void releaseClaims(
      Map<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> owned) {
    for (Map.Entry<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> entry :
        owned.entrySet()) {
      pairBuilds.remove(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Withdraws the owned claims on failure so a later query can retry the build; {@code
   * completeExceptionally} only takes effect when the failure happened before {@link
   * #completeClaimFutures} -- if writeBatch failed, waiters have already observed success with a
   * valid in-memory model, and only the withdrawn claim (and the caller's rethrow) records that the
   * column was never persisted.
   */
  private void withdrawClaims(
      Map<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> owned, Throwable t) {
    for (Map.Entry<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> entry :
        owned.entrySet()) {
      pairBuilds.remove(entry.getKey(), entry.getValue());
      entry.getValue().completeExceptionally(t);
    }
  }

  /**
   * Joins the pairs claimed by other threads and merges their models into {@code result},
   * unwrapping a builder's failure into this thread's checked contract.
   */
  private static void collectAwaited(
      List<CompletableFuture<Map.Entry<String, JoinColumnModel>>> awaited,
      Map<String, JoinColumnModel> result)
      throws IOException {
    for (CompletableFuture<Map.Entry<String, JoinColumnModel>> future : awaited) {
      try {
        Map.Entry<String, JoinColumnModel> entry = future.join();
        result.put(entry.getKey(), entry.getValue());
      } catch (CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException ioe) {
          throw ioe;
        }
        if (cause instanceof RuntimeException re) {
          throw re;
        }
        throw new IOException(cause);
      }
    }
  }

  /**
   * Emits the {@code evt=build} diagnostic line. The built/awaited split matters: an awaited pair
   * cost this thread only the wait, so folding the two together would attribute another thread's
   * build work to this query; pairsBuilt counts models computed by this thread, writtenPairs what
   * actually reached the sidecar -- the difference is pairs the double-check found already
   * persisted.
   */
  private static void logBuildDiagnostics(
      String traceCtxId,
      int pairsRequested,
      Map<String, JoinColumnModel> loadedMappings,
      int pairsAwaited,
      Set<String> writtenPairs,
      long startNanos,
      long builtNanos) {
    if (!JoinIndexUtils.diagnosticsEnabled(log) || pairsRequested == 0) {
      return;
    }
    long toCount = 0;
    for (JoinColumnModel model : loadedMappings.values()) {
      toCount += model.edges().toCount();
    }
    JoinIndexUtils.logDiagnostic(
        log,
        "AUXIJOIN evt=build ctx={} pairsRequested={} pairsBuilt={} pairsAwaited={}"
            + " builtMs={} awaitedMs={} toCount={} writtenPairs={}",
        traceCtxId == null ? "-" : traceCtxId,
        pairsRequested,
        loadedMappings.size(),
        pairsAwaited,
        builtNanos / 1_000_000L,
        (System.nanoTime() - startNanos - builtNanos) / 1_000_000L,
        toCount,
        writtenPairs);
  }
}
