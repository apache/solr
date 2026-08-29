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

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;

/**
 * The auxiliary join index: a self-maintaining sidecar persisting per (from-segment, to-segment)
 * doc id mappings, so query-time joining reduces to bitset translation. It owns the sidecar's
 * {@link IndexWriter} and {@link SearcherManager}; pair columns are built lazily when an {@link
 * AuxIndexJoinQuery} first needs them, so users only construct an instance once, create queries
 * with {@link #newJoinQuery} and search them with a bare to-side {@link IndexSearcher}:
 *
 * <pre class="prettyprint">
 * AuxIndexManager joinIndex = new AuxIndexManager(joinDir);   // once per process
 * Query q = joinIndex.newJoinQuery(fromField, fromQuery, fromSearcher, toField);
 * TopDocs hits = toSearcher.search(q, 10);
 * ...
 * joinIndex.close();                                   // app shutdown
 * </pre>
 *
 * <p>After either side reopens, the next query builds only the missing (from, to) segment pairs:
 * pair columns are addressed by both sides' persistent segment keys, which survive reopens. Pair
 * columns orphaned by merges are not reclaimed yet; see package's javadoc.
 */
public final class AuxIndexManager implements Closeable {

  private final IndexWriter writer;
  private final SearcherManager manager;
  private final JoinColumnIndexer pairBuilder = new JoinColumnIndexer(this);

  // package-private (not private): tests reach in directly to observe the reaper's state
  final AuxIndexJoinMergePolicy mergePolicy;
  private final MergeScheduler mergeScheduler;
  private final JoinColumWriter writerDelegate;
  private final boolean blockingRefresh;

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
   * Opens a persistent auxiliary join index over the given directory, creating it if empty, using
   * the default {@link AuxIndexJoinConfig}. The caller retains ownership of the directory: {@link
   * #close()} does not close it.
   */
  public AuxIndexManager(Directory directory) throws IOException {
    this(directory, new AuxIndexJoinConfig());
  }

  /**
   * Opens a persistent auxiliary join index over the given directory, creating it if empty, using
   * the given {@link AuxIndexJoinConfig}. The caller retains ownership of the directory: {@link
   * #close()} does not close it.
   */
  public AuxIndexManager(Directory directory, AuxIndexJoinConfig config) throws IOException {
    this(directory, config, new ConcurrentMergeScheduler());
  }

  /**
   * Opens a persistent auxiliary join index over the given directory, creating it if empty, using
   * the given {@link AuxIndexJoinConfig} and {@link MergeScheduler} in place of the default {@link
   * ConcurrentMergeScheduler}. The caller retains ownership of the directory: {@link #close()} does
   * not close it.
   */
  public AuxIndexManager(
      Directory directory, AuxIndexJoinConfig config, MergeScheduler mergeScheduler)
      throws IOException {
    this.mergeScheduler = mergeScheduler;
    this.mergePolicy = new AuxIndexJoinMergePolicy();
    this.mergePolicy.setSweepInterval(config.getSweepSamplingIntervalNanos(), TimeUnit.NANOSECONDS);
    this.writer =
        new IndexWriter(
            directory,
            new IndexWriterConfig().setMergePolicy(mergePolicy).setMergeScheduler(mergeScheduler));
    this.manager = new SearcherManager(writer, null);
    JoinColumWriter bulkWriter = new JoinColumnDocWriter(); // new AIJoinColumnWriter()
    this.writerDelegate =
        config.getSingleFieldPerSegment()
            ? new SingleColumnBySegmentWriter(bulkWriter)
            : bulkWriter;
    this.blockingRefresh = config.getBlockingRefresh();
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
    return new AuxIndexJoinQuery(
        this,
        fromField,
        fromQuery,
        fromSearcher,
        toField,
        fromExecutor == null ? new DirectExecutorService() : fromExecutor);
  }

  /**
   * How many of the given pair field names have a build currently in flight (claims are removed
   * once persisted). Diagnostic only; see {@link JoinColumnIndexer#countClaimedBuilds}.
   */
  int countClaimedBuilds(Set<String> pairFieldNames) {
    return pairBuilder.countClaimedBuilds(pairFieldNames);
  }

  IndexSearcher acquire() throws IOException {
    return manager.acquire();
  }

  void release(IndexSearcher searcher) throws IOException {
    manager.release(searcher);
  }

  /**
   * Builds and persists the given missing pair columns, keyed by pair field name to their
   * (from-segment, to-segment) leaf ordinals. Delegates to {@link
   * JoinColumnIndexer#buildAndPersistJoinColumns(Map, IndexReader, IndexReader, String, String,
   * IndexSearcher, Future[])} writeJoinSegments}, which documents the claim/await dedup and the
   * fresh-searcher double-check in detail.
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
    return pairBuilder.buildAndPersistJoinColumns(
        missingPairs,
        fromReader,
        toReader,
        toField,
        traceCtxId,
        observedAbsentSearcher,
        fromColumnFutures);
  }

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
  synchronized void writeBatch(Map<String, JoinColumnModel> mappings) throws IOException {
    this.writerDelegate.writeJoinColumns(writer, mappings);
    if (this.blockingRefresh) {
      manager.maybeRefreshBlocking();
    } else {
      manager.maybeRefresh(); // perhaps it should be carried out the enclosing synchronize
    }
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

  private static class SingleColumnBySegmentWriter extends JoinColumWriter {
    private final JoinColumWriter bulkWriter;

    public SingleColumnBySegmentWriter(JoinColumWriter bulkWriter) {
      this.bulkWriter = bulkWriter;
    }

    @Override
    void writeJoinColumns(IndexWriter writer, Map<String, JoinColumnModel> mappings)
        throws IOException {
      for (Map.Entry<String, JoinColumnModel> entry : mappings.entrySet()) {
        bulkWriter.writeJoinColumns(writer, Map.of(entry.getKey(), entry.getValue()));
      }
    }
  }
}
