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
package org.apache.solr.search.join.aijoin;

import java.io.Closeable;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.search.join.aijoin.AIJoinUtil.JoinColumnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The auxiliary join index: a self-maintaining sidecar persisting per (from-segment, to-segment)
 * doc id mappings, so query-time joining reduces to bitset translation. It owns the sidecar's
 * {@link IndexWriter} and {@link SearcherManager}; pair columns are built lazily when an {@link
 * AIJoinQuery} first needs them, so users only construct an instance once, create queries with
 * {@link #newJoinQuery} and search them with a bare to-side {@link IndexSearcher}:
 *
 * <pre class="prettyprint">
 * AIJoinIndex joinIndex = new AIJoinIndex(joinDir);   // once per process
 * Query q = joinIndex.newJoinQuery(fromField, fromQuery, fromSearcher, toField);
 * TopDocs hits = toSearcher.search(q, 10);
 * ...
 * joinIndex.close();                                   // app shutdown
 * </pre>
 *
 * <p>After either side reopens, the next query builds only the missing (from, to) segment pairs:
 * pair columns are addressed by both sides' persistent segment keys, which survive reopens. Pair
 * columns orphaned by merges are not reclaimed yet; see {@code README.md} in this package.
 */
public final class AIJoinIndex implements Closeable {

  private final IndexWriter writer;
  private final SearcherManager manager;

  /**
   * Dedups concurrent builders per pair field name: the thread that installs the future writes the
   * pair, others wait on it. A future completes as soon as the pair's in-memory {@link
   * JoinColumnModel} is ready -- before the owning thread persists and refreshes -- so completion
   * does not imply the column is visible through {@link #manager} yet. Completed futures stay put
   * so a builder that raced a not-yet-visible refresh cannot write a duplicate pair column; that
   * dedup is what keeps handing out models ahead of the commit safe.
   */
  private final ConcurrentHashMap<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>>
      pairBuilds = new ConcurrentHashMap<>();

  // package-private (not private): tests reach in directly to observe the reaper's state
  final AIJoinMergePolicy mergePolicy;
  private final MergeScheduler mergeScheduler;
  static final AIJoinWriter INSTANCE = new AIJoinDocWriter(); // new AIJoinColumnWriter();

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** A pair's (from-segment, to-segment) leaf ordinals. */
  record SegmentsTuple(int fromLeafOrd, int toLeafOrd) {}

  /**
   * A pair column's address in the join index: the pair field name and the sidecar segment (name
   * plus current leaf ordinal) carrying it -- enough to locate and open the column's real
   * docvalues, or to check whether a pair already exists before deciding what still needs to be
   * built. A resolved cell's edges are tracked separately, as a plain {@code DocEdges}, since they
   * don't change as this reference is refreshed.
   */
  record JoinSegmentReference(
      String pairFieldName, String joinSegmentName, int joinSegmentLeafOrd) {}

  /**
   * Scans {@code joinSearcher}'s leaves for every pair column whose field name satisfies {@code
   * isNeeded}, returning where each one lives. Used both to seed a fresh {@link AIJoinWeight}'s
   * view of already-built pairs, and by {@link ToLeafJoinContext} to relocate a pair whose cached
   * segment reference no longer resolves. TODO subject for in-heap caching TODO commit's userdata
   * might have a list of pairs with known segment ords and names
   */
  static Map<String, JoinSegmentReference> extractExistingJoinColumns(
      IndexSearcher joinSearcher, Predicate<String> isNeeded) {
    Map<String, JoinSegmentReference> existingJoinSegments =
        CollectionUtil.newHashMap(joinSearcher.getIndexReader().leaves().size());
    for (LeafReaderContext joinContext : joinSearcher.getIndexReader().leaves()) {
      String segmentName = AIJoinUtil.segmentName(joinContext);
      for (FieldInfo fieldInfo : joinContext.reader().getFieldInfos()) {
        // pairs are detected by their toCount companion, which is written to doc 0 for every
        // built pair; the join column itself is sparse and a tombstone pair (disjoint terms)
        // never materializes it, so scanning for join columns kept re-reporting once-built
        // tombstones as missing -- and re-triggering their from-side FK loads on every query
        String splits[] = fieldInfo.name.split(AIJoinUtil.TO_COUNT_PREFIX);
        if (splits.length == 2 && isNeeded.test(splits[1])) {
          existingJoinSegments.computeIfAbsent(
              splits[1],
              fieldName -> new JoinSegmentReference(fieldName, segmentName, joinContext.ord));
        }
      }
    }
    return existingJoinSegments;
  }

  /**
   * Opens a persistent auxiliary join index over the given directory, creating it if empty, using
   * the default {@link AIJoinIndexConfig}. The caller retains ownership of the directory: {@link
   * #close()} does not close it.
   */
  public AIJoinIndex(Directory directory) throws IOException {
    this(directory, new AIJoinIndexConfig());
  }

  /**
   * Opens a persistent auxiliary join index over the given directory, creating it if empty, using
   * the given {@link AIJoinIndexConfig}. The caller retains ownership of the directory: {@link
   * #close()} does not close it.
   */
  public AIJoinIndex(Directory directory, AIJoinIndexConfig config) throws IOException {
    this(directory, config, new ConcurrentMergeScheduler());
  }

  /**
   * Opens a persistent auxiliary join index over the given directory, creating it if empty, using
   * the given {@link AIJoinIndexConfig} and {@link MergeScheduler} in place of the default {@link
   * ConcurrentMergeScheduler}. The caller retains ownership of the directory: {@link #close()} does
   * not close it.
   */
  public AIJoinIndex(Directory directory, AIJoinIndexConfig config, MergeScheduler mergeScheduler)
      throws IOException {
    this.mergeScheduler = mergeScheduler;
    this.mergePolicy = new AIJoinMergePolicy();
    this.mergePolicy.setSweepInterval(config.getSweepSamplingIntervalNanos(), TimeUnit.NANOSECONDS);
    this.writer =
        new IndexWriter(
            directory,
            new IndexWriterConfig().setMergePolicy(mergePolicy).setMergeScheduler(mergeScheduler));
    this.manager = new SearcherManager(writer, null);
  }

  /**
   * Creates a query joining the docs matching {@code fromQuery} in {@code fromSearcher}'s index to
   * the index the returned query is executed against, through {@code fromField} = {@code toField}
   * term equality. Missing pair columns are built into this join index on first execution.
   *
   * @deprecated use another constructor passing executor service
   */
  @Deprecated
  public Query newJoinQuery(
      String fromField, Query fromQuery, IndexSearcher fromSearcher, String toField) {
    return newJoinQuery(fromField, fromQuery, fromSearcher, toField, new DirectExecutorService());
  }

  public Query newJoinQuery(
      String fromField,
      Query fromQuery,
      IndexSearcher fromSearcher,
      String toField,
      ExecutorService fromExecutor) {
    return new AIJoinQuery(
        this,
        fromField,
        fromQuery,
        fromSearcher,
        toField,
        fromExecutor == null ? new DirectExecutorService() : fromExecutor);
  }

  /**
   * How many of the given pair field names already have a build claimed (in-flight or completed) in
   * {@link #pairBuilds}. Diagnostic only: a pair counted here but still reported missing by {@link
   * #extractExistingJoinColumns} means the caller is about to redo from-side work for a pair that
   * was already built -- its column just isn't visible through the searcher it consulted.
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

  IndexSearcher acquire() throws IOException {
    return manager.acquire();
  }

  void release(IndexSearcher searcher) throws IOException {
    manager.release(searcher);
  }

  /**
   * Builds and persists the given missing pair columns, keyed by pair field name to their
   * (from-segment, to-segment) leaf ordinals. Pairs concurrently built by another thread are
   * awaited, not rebuilt; awaiting yields the builder's in-memory model as soon as it is computed,
   * possibly before the builder has committed it, so on return pairs built by this thread are
   * refreshed into the internal searcher manager, while awaited ones may not be visible there yet.
   *
   * @return in memory data for just written segemts
   */
  Map<String, JoinColumnModel> writeJoinSegments(
      Map<String, SegmentsTuple> missingPairs,
      IndexReader fromReader,
      IndexReader toReader,
      String toField,
      String traceCtxId,
      Future<FromLeafJoinContext>[] fromColumnFutures)
      throws IOException, ExecutionException, InterruptedException {
    long startNanos = System.nanoTime();
    int batchNumDocsLogged = 0;
    Map<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> owned =
        new LinkedHashMap<>();
    List<CompletableFuture<Map.Entry<String, JoinColumnModel>>> awaited = new ArrayList<>();
    for (String pairFieldName : missingPairs.keySet()) {
      CompletableFuture<Map.Entry<String, JoinColumnModel>> created = new CompletableFuture<>();
      CompletableFuture<Map.Entry<String, JoinColumnModel>> existing =
          pairBuilds.putIfAbsent(pairFieldName, created);
      if (existing == null) {
        owned.put(pairFieldName, created);
      } else {
        awaited.add(existing);
      }
    }
    Map<String, JoinColumnModel> loadedMappings = new LinkedHashMap<>();
    try {
      if (!owned.isEmpty()) {
        // all owned pairs go into a single batch: pair columns are addressed by from-side doc id,
        // so a batch must start at doc 0 of its sidecar segment, which writeBatch guarantees by
        // flushing one batch per commit
        int batchNumDocs = 0;
        AIJoinUtil.ToDocInvertor toInvertor = new AIJoinUtil.ToDocInvertor(toField);
        for (String pairFieldName : owned.keySet()) {
          SegmentsTuple position = missingPairs.get(pairFieldName);
          LeafReaderContext toContext = toReader.leaves().get(position.toLeafOrd());
          LeafReaderContext fromContext = fromReader.leaves().get(position.fromLeafOrd());
          assert fromColumnFutures[fromContext.ord] != null;
          AIJoinUtil.JoinColumnModel mapping =
              AIJoinUtil.computeDocMapping(
                  toContext,
                  toField,
                  fromColumnFutures[fromContext.ord].get().fkColumn,
                  toInvertor);
          batchNumDocs = Math.max(batchNumDocs, fromContext.reader().maxDoc());
          loadedMappings.put(pairFieldName, mapping);
        }
        // complete before writeBatch: waiters only need the in-memory model, so they don't
        // pay for this thread's commit + refresh; completion does NOT imply the column is
        // visible through the searcher manager yet
        for (Map.Entry<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> entry :
            owned.entrySet()) {
          entry
              .getValue()
              .complete(
                  new AbstractMap.SimpleImmutableEntry<>(
                      entry.getKey(), loadedMappings.get(entry.getKey())));
        }
        writeBatch(batchNumDocs, loadedMappings);
        batchNumDocsLogged = batchNumDocs;
        // TODO flush every single field to get single field segments
      }
    } catch (Throwable t) {
      // withdraw the claims so a later query can retry the build; completeExceptionally only
      // takes effect when the failure happened before the completion loop above -- if writeBatch
      // failed, waiters have already observed success with a valid in-memory model, and only the
      // withdrawn claim (and this rethrow) records that the column was never persisted
      for (Map.Entry<String, CompletableFuture<Map.Entry<String, JoinColumnModel>>> entry :
          owned.entrySet()) {
        pairBuilds.remove(entry.getKey(), entry.getValue());
        entry.getValue().completeExceptionally(t);
      }
      throw t;
    }
    long builtNanos = System.nanoTime() - startNanos;
    Map<String, JoinColumnModel> result = new LinkedHashMap<>(loadedMappings);
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
    if (AIJoinUtil.diagnosticsEnabled(log) && !missingPairs.isEmpty()) {
      long toCount = 0;
      for (JoinColumnModel model : loadedMappings.values()) {
        toCount += model.edges().toCount();
      }
      // built/awaited split matters: an awaited pair cost this thread only the wait, so folding
      // the two together would attribute another thread's build work to this query
      AIJoinUtil.logDiagnostic(
          log,
          "AIJOIN evt=build ctx={} pairsRequested={} pairsBuilt={} pairsAwaited={}"
              + " builtMs={} awaitedMs={} toCount={} batchNumDocs={} writtenPairs={}",
          traceCtxId == null ? "-" : traceCtxId,
          missingPairs.size(),
          loadedMappings.size(),
          awaited.size(),
          builtNanos / 1_000_000L,
          (System.nanoTime() - startNanos - builtNanos) / 1_000_000L,
          toCount,
          batchNumDocsLogged,
          loadedMappings.keySet());
    }
    return result;
  }

  /**
   * deprecated don't write all of them upfront
   *
   * <p>Eagerly builds and persists every pair column in {@code neededPairs} not yet present in this
   * join index, so an {@link AIJoinWeight} being constructed at {@link AIJoinQuery#createWeight}
   * already sees a complete view of the pairs it needs, instead of discovering gaps lazily -- one
   * to-segment at a time -- in {@link ToLeafJoinContext}. Missing pairs are resolved to their
   * (from-segment, to-segment) leaf ordinals by crossing {@code fromSearcher}'s leaves against
   * {@code toSearcher}'s leaves; pairs concurrently built by another thread are awaited, not
   * rebuilt (see {@link #writeJoinSegments}).
   */
  //  @Deprecated
  //  void ensureJoinSegments(
  //      Set<String> neededPairs,
  //      IndexSearcher fromSearcher,
  //      String fromField,
  //      IndexSearcher toSearcher,
  //      String toField)
  //      throws IOException {
  //    Map<String, JoinSegmentReference> existing;
  //    IndexSearcher joinSearcher = acquire();
  //    try {
  //      existing = extractExistingJoinColumns(joinSearcher, neededPairs::contains);
  //    } finally {
  //      release(joinSearcher);
  //    }
  //    if (existing.keySet().containsAll(neededPairs)) {
  //      return;
  //    }
  //    Map<String, SegmentsTuple> missingPairs = new LinkedHashMap<>();
  //    for (LeafReaderContext fromContext : fromSearcher.getLeafContexts()) {
  //      for (LeafReaderContext toContext : toSearcher.getLeafContexts()) {
  //        String pairFieldName = AIJoinUtil.pairFieldName(fromContext, fromField, toContext,
  // toField);
  //        if (neededPairs.contains(pairFieldName) && !existing.containsKey(pairFieldName)) {
  //          missingPairs.put(pairFieldName, new SegmentsTuple(fromContext.ord, toContext.ord));
  //        }
  //      }
  //    }
  //    if (!missingPairs.isEmpty()) {
  //      writeJoinSegments(
  //          missingPairs,
  //          fromSearcher.getIndexReader(),
  //          fromField,
  //          toSearcher.getIndexReader(),
  //          toField,
  //          BuildCause.EAGER_CREATE_WEIGHT,
  //          null, fromColumnFutures); // runs before any ToLeafJoinContext exists, so there is no
  // context to blame
  //    }
  //  }

  /**
   * Serializes sidecar writes: one batch per commit keeps every batch at doc 0 of its own segment,
   * preserving pair-column doc number == from-side doc id. Builders' futures are completed before
   * this runs, so waiters consume the in-memory models without paying for the commit and refresh
   * here -- a completed future does not mean the reader already exposes the column.
   *
   * <p>It should be plain simple synchronized. As alternatives
   *
   * <ul>
   *   <li>push synchronized deeper to iw.addDocs(),iw.commit()
   *   <li>merge columns from concurrent threads and writes them as a single batch - too much </>
   *       The invariant is a column starts ad doc#==0, this iw.addDocs(),iw.commit() goes one by
   *       one. But such segment might contain many parallel columns, since they have distinguishing
   *       names.
   */
  private synchronized void writeBatch(int batchNumDocs, Map<String, JoinColumnModel> mappings)
      throws IOException {
    AIJoinIndex.INSTANCE.writeJoinColumns(writer, batchNumDocs, mappings);
    manager.maybeRefreshBlocking(); // consider using the non-blocking maybeRefresh().
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(manager, writer);
  }

  public void onCreateWeight(
      Set<String> neededPairs, IndexSearcher fromSearcher, IndexSearcher searcher)
      throws IOException {
    this.mergePolicy.onCreateWeight(neededPairs, fromSearcher, searcher);
  }

  /**
   * Test-only: blocks until any in-flight merges (e.g. a dead-pair reap) finish. Only supported
   * when this index was opened with a {@link ConcurrentMergeScheduler} (the default).
   */
  void waitForMerges() {
    if (mergeScheduler instanceof ConcurrentMergeScheduler cms) {
      cms.sync();
    } else {
      throw new UnsupportedOperationException(
          "waitForMerges() requires a ConcurrentMergeScheduler, got " + mergeScheduler.getClass());
    }
  }
}
