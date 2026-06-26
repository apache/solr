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
package org.apache.solr.search;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.core.CancellableQueryTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holders for the {@link CollectorManager} implementations used by Solr's search paths. Shared
 * across the single-slice and multi-slice (parallel-segment) execution paths.
 */
final class SolrCollectorManagers {

  private SolrCollectorManagers() {}

  static class MaxScoreResult {
    final float maxScore;

    MaxScoreResult(float maxScore) {
      this.maxScore = maxScore;
    }
  }

  static class TopDocsResult {
    final TopDocs topDocs;
    final int totalHits;

    TopDocsResult(TopDocs topDocs, int totalHits) {
      this.topDocs = topDocs;
      this.totalHits = totalHits;
    }
  }

  static class SearchResult {
    final ScoreMode scoreMode;
    private final Object[] result;

    SearchResult(ScoreMode scoreMode, Object[] result) {
      this.scoreMode = scoreMode;
      this.result = result;
    }

    public TopDocsResult getTopDocsResult() {
      for (Object res : result) {
        if (res instanceof TopDocsResult) {
          return (TopDocsResult) res;
        }
      }
      return null;
    }

    public float getMaxScore(int totalHits) {
      if (totalHits > 0) {
        for (Object res : result) {
          if (res instanceof MaxScoreResult) {
            return ((MaxScoreResult) res).maxScore;
          }
        }
        return Float.NaN;
      } else {
        return 0.0f;
      }
    }

    public DocSet getDocSet() {
      for (Object res : result) {
        if (res instanceof DocSet) {
          return (DocSet) res;
        }
        if (res instanceof FixedBitSet fbs) {
          return new BitDocSet(fbs);
        }
      }
      return null;
    }
  }

  static class FixedBitSetCollector extends SimpleCollector {
    @SuppressWarnings("JdkObsolete")
    private final LinkedList<FixedBitSet> bitSets = new LinkedList<>();

    @SuppressWarnings("JdkObsolete")
    private final LinkedList<Integer> skipWords = new LinkedList<>();

    @SuppressWarnings("JdkObsolete")
    private final LinkedList<Integer> skipBits = new LinkedList<>();

    FixedBitSetCollector() {}

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
      this.bitSets.add(null); // lazy allocate when collecting document(s)
      this.skipWords.add(context.docBase / 64);
      this.skipBits.add(context.docBase % 64);
    }

    @Override
    public void collect(int doc) throws IOException {
      FixedBitSet bitSet = this.bitSets.getLast();
      final int idx = this.skipBits.getLast() + doc;

      final int numWords = FixedBitSet.bits2words(idx + 1); // +1 to ensure minimum 1 word

      if (bitSet == null) {
        this.bitSets.removeLast();
        bitSet = new FixedBitSet(numWords * 64);
        this.bitSets.addLast(bitSet);

      } else if (bitSet.getBits().length < numWords) {
        FixedBitSet smallerBitSet = this.bitSets.removeLast();
        bitSet = new FixedBitSet(numWords * 64);
        bitSet.xor(smallerBitSet);
        this.bitSets.addLast(bitSet);
      }

      bitSet.set(idx);
    }

    void update(FixedBitSet allBitSet) {
      final long[] allBits = allBitSet.getBits();
      for (int bs_idx = 0; bs_idx < this.bitSets.size(); ++bs_idx) {
        final FixedBitSet itBitSet = this.bitSets.get(bs_idx);
        if (itBitSet != null) {
          final int skipWords = this.skipWords.get(bs_idx);
          final long[] itBits = itBitSet.getBits();
          for (int idx = 0; idx < itBits.length && skipWords + idx < allBits.length; ++idx) {
            allBits[skipWords + idx] ^= itBits[idx];
          }
        }
      }
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  static class MaxScoreCM implements CollectorManager<Collector, Object> {
    private final Collector[] firstCollectors;
    private final int firstMaxScoreCollectorIndex;

    MaxScoreCM(Collector[] firstCollectors, int firstMaxScoreCollectorIndex) {
      this.firstCollectors = firstCollectors;
      this.firstMaxScoreCollectorIndex = firstMaxScoreCollectorIndex;
    }

    @Override
    public Collector newCollector() throws IOException {
      MaxScoreCollector collector = new MaxScoreCollector();
      if (firstCollectors[firstMaxScoreCollectorIndex] == null) {
        firstCollectors[firstMaxScoreCollectorIndex] = collector;
      }
      return collector;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object reduce(Collection collectors) throws IOException {
      float maxScore = 0.0f;
      for (Object o : collectors) {
        Collector next = (Collector) o;
        if (next instanceof final EarlyTerminatingCollector etc) {
          next = etc.getDelegate();
        }
        maxScore = Math.max(maxScore, ((MaxScoreCollector) next).getMaxScore());
      }
      return new MaxScoreResult(maxScore);
    }
  }

  static class DocSetCM implements CollectorManager<Collector, Object> {
    private final int maxDoc;

    DocSetCM(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public Collector newCollector() throws IOException {
      return new FixedBitSetCollector();
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public Object reduce(Collection collectors) throws IOException {
      final FixedBitSet reduced = new FixedBitSet(maxDoc);
      for (Object collector : collectors) {
        if (collector instanceof FixedBitSetCollector fixedBitSetCollector) {
          fixedBitSetCollector.update(reduced);
        }
      }
      return reduced;
    }
  }

  /**
   * Single-slice {@link DocSet} producer. Wraps a {@link DocSetCollector} (which preserves Solr's
   * sparse {@code SortedIntDocSet} optimization for small result sets) and reduces via {@link
   * DocSetUtil#getDocSet}. Use this in single-slice paths where we want to keep the legacy DocSet
   * shape; multi-slice paths use {@link DocSetCM} which produces a {@link FixedBitSet} (and thus
   * always a {@code BitDocSet}).
   */
  static class DocSetCollectorCM implements CollectorManager<Collector, Object> {
    private final SolrIndexSearcher searcher;
    private final int maxDoc;

    DocSetCollectorCM(SolrIndexSearcher searcher) {
      this.searcher = searcher;
      this.maxDoc = searcher.maxDoc();
    }

    @Override
    public Collector newCollector() throws IOException {
      return new DocSetCollector(maxDoc);
    }

    @Override
    public Object reduce(Collection<Collector> collectors) throws IOException {
      DocSetCollector dsc = null;
      for (Collector c : collectors) {
        if (c instanceof DocSetCollector inner) {
          if (dsc != null) {
            throw new IllegalStateException("DocSetCollectorCM is single-slice only");
          }
          dsc = inner;
        }
      }
      if (dsc == null) {
        return null;
      }
      return DocSetUtil.getDocSet(dsc, searcher);
    }
  }

  static class TopDocsCM implements CollectorManager<Collector, Object> {
    private final SolrIndexSearcher searcher;
    private final int len;
    private final QueryCommand cmd;
    private final Collector[] firstCollectors;
    private final int firstTopDocsCollectorIndex;

    TopDocsCM(
        SolrIndexSearcher searcher,
        int len,
        QueryCommand cmd,
        Collector[] firstCollectors,
        int firstTopDocsCollectorIndex) {
      this.searcher = searcher;
      this.len = len;
      this.cmd = cmd;
      this.firstCollectors = firstCollectors;
      this.firstTopDocsCollectorIndex = firstTopDocsCollectorIndex;
    }

    @Override
    public Collector newCollector() throws IOException {
      @SuppressWarnings("rawtypes")
      TopDocsCollector collector = searcher.buildTopDocsCollector(len, cmd);
      if (firstCollectors[firstTopDocsCollectorIndex] == null) {
        firstCollectors[firstTopDocsCollectorIndex] = collector;
      }
      return collector;
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Object reduce(Collection collectors) throws IOException {

      TopDocs[] topDocs = new TopDocs[collectors.size()];

      int totalHits = -1;
      int i = 0;

      Collector collector;
      for (Object o : collectors) {
        collector = (Collector) o;
        if (collector instanceof final EarlyTerminatingCollector earlyTerminatingCollector) {
          collector = earlyTerminatingCollector.getDelegate();
        }
        if (collector instanceof TopDocsCollector) {
          TopDocs td = ((TopDocsCollector) collector).topDocs(0, len);
          assert td != null : Arrays.asList(topDocs);
          topDocs[i++] = td;
        }
      }

      TopDocs mergedTopDocs = null;

      if (topDocs.length > 0 && topDocs[0] != null) {
        if (i == 1) {
          // Single-slice: skip merge. TopDocs.merge with one shard creates a fresh TotalHits and
          // can lose the EQUAL_TO/GREATER_THAN_OR_EQUAL_TO distinction that the original
          // TopDocsCollector recorded — single-slice callers (e.g. queries with a post-filter)
          // expect the original relation to be preserved verbatim.
          mergedTopDocs = topDocs[0];
        } else if (Arrays.stream(topDocs).allMatch(td -> td instanceof TopFieldDocs)) {
          TopFieldDocs[] topFieldDocs =
              Arrays.copyOf(topDocs, topDocs.length, TopFieldDocs[].class);
          mergedTopDocs = TopFieldDocs.merge(searcher.weightSort(cmd.getSort()), len, topFieldDocs);
        } else {
          mergedTopDocs = TopDocs.merge(0, len, topDocs);
        }
        totalHits = (int) mergedTopDocs.totalHits.value();
      }
      return new TopDocsResult(mergedTopDocs, totalHits);
    }
  }

  /**
   * Wraps a single pre-built {@link Collector} as a single-slice {@link CollectorManager}. {@link
   * CollectorManager#newCollector()} returns the wrapped collector exactly once; a second call
   * throws. {@link CollectorManager#reduce} returns the same collector instance, letting callers
   * read state out of it.
   */
  static <C extends Collector> CollectorManager<C, C> singleSlice(C collector) {
    return new CollectorManager<C, C>() {
      private boolean given = false;

      @Override
      public C newCollector() {
        if (given) {
          throw new IllegalStateException(
              "singleSlice() collector manager only supports a single slice");
        }
        given = true;
        return collector;
      }

      @Override
      public C reduce(Collection<C> collectors) {
        return collector;
      }
    };
  }

  /**
   * Wraps a single inner {@link CollectorManager} as a {@code CollectorManager<C, Object[]>}
   * compatible with {@link SolrMultiCollectorManager}'s reduce shape, but without the per-doc
   * fan-out overhead.
   *
   * <p>{@link SolrMultiCollectorManager} adds a per-document {@code LeafCollectors.collect()} loop
   * on top of every inner collector — worth it when there are multiple inner CMs, but pure overhead
   * when there is only one. {@link SolrIndexSearcher#searchAndCollect} routes single-inner-CM
   * searches through this adapter so {@code LeafCollector.collect(doc)} dispatches directly to the
   * inner collector. Mirrors the spirit of Lucene's {@code MultiCollectorManager.wrap(...)}
   * short-circuit for the one-collector case.
   */
  static <C extends Collector> CollectorManager<C, Object[]> wrapSingle(
      CollectorManager<C, ?> inner) {
    return new CollectorManager<C, Object[]>() {
      @Override
      public C newCollector() throws IOException {
        return inner.newCollector();
      }

      @SuppressWarnings({"unchecked", "rawtypes"})
      @Override
      public Object[] reduce(Collection<C> collectors) throws IOException {
        return new Object[] {((CollectorManager) inner).reduce(collectors)};
      }
    };
  }

  /**
   * Wraps an inner {@link CollectorManager} with the search-chain features that {@link
   * SolrIndexSearcher} requires: {@code segmentTerminateEarly}, {@code maxHitsAllowed} early
   * termination, optional Solr post-filter, and query cancellation via a shared {@link
   * AtomicBoolean}.
   *
   * <p>Wrapping order from inside out: inner collector → {@link EarlyTerminatingSortingCollector} →
   * {@link EarlyTerminatingCollector} → {@link DelegatingCollector} post-filter → {@link
   * CancellableCollector}.
   *
   * <p>Per-slice cancellation works because every {@link CancellableCollector} created by {@link
   * #newCollector()} polls the same shared flag — see {@link
   * CancellableCollector#CancellableCollector(Collector, AtomicBoolean)}. The first cancellable
   * created is registered with {@link CancellableQueryTracker} so the existing cancel-by-id API
   * remains intact.
   *
   * <p>Post-filter handling is intentionally limited: a {@link DelegatingCollector} is a single
   * per-search instance with cross-collect state, so {@link #requiresSingleSlice()} returns {@code
   * true} when one is configured. The calling search path must honor that and execute as a single
   * slice.
   *
   * <p>{@link #reduce} aggregates {@code segmentTerminatedEarly} across the per-slice {@link
   * EarlyTerminatingSortingCollector} instances, calls {@link DelegatingCollector#complete()} once
   * if a post-filter is present, then forwards to the inner manager's reduce on the inner
   * collectors that were created.
   */
  static class ChainCM<C extends Collector, T> implements CollectorManager<Collector, T> {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private final CollectorManager<C, T> inner;
    private final QueryCommand cmd;
    private final DelegatingCollector postFilter;
    private final Sort mergePolicySort;
    private final QueryResult qr;
    private final AtomicBoolean cancellationFlag;
    private final CancellableQueryTracker cancellableTracker;

    private final List<C> innerCollectors = Collections.synchronizedList(new ArrayList<>());
    private final List<EarlyTerminatingSortingCollector> segmentTermCollectors =
        Collections.synchronizedList(new ArrayList<>());
    private final LongAdder earlyTerminatingHits;
    private CancellableCollector trackerRegistration;
    private Collector firstWrappedCollector;
    private boolean newCollectorCalled = false;
    private boolean reduced = false;
    private T reducedResult;

    ChainCM(
        CollectorManager<C, T> inner,
        QueryCommand cmd,
        DelegatingCollector postFilter,
        Sort mergePolicySort,
        QueryResult qr,
        AtomicBoolean cancellationFlag,
        CancellableQueryTracker cancellableTracker,
        boolean parallelEligible) {
      this.inner = inner;
      this.cmd = cmd;
      this.postFilter = postFilter;
      this.mergePolicySort = mergePolicySort;
      this.qr = qr;
      this.cancellationFlag = cancellationFlag;
      this.cancellableTracker = cancellableTracker;
      boolean singleSlice = (postFilter != null) || !parallelEligible;
      this.earlyTerminatingHits =
          (cmd.shouldEarlyTerminateSearch() && !singleSlice) ? new LongAdder() : null;
    }

    @Override
    public Collector newCollector() throws IOException {
      if (postFilter != null && newCollectorCalled) {
        throw new IllegalStateException(
            "ChainCM with a post-filter requires single-slice execution");
      }

      C innerCollector = inner.newCollector();
      innerCollectors.add(innerCollector);
      Collector c = innerCollector;

      if (cmd.getSegmentTerminateEarly()) {
        Sort cmdSort = cmd.getSort();
        int cmdLen = cmd.getLen();
        if (cmdSort == null
            || cmdLen <= 0
            || mergePolicySort == null
            || !EarlyTerminatingSortingCollector.canEarlyTerminate(cmdSort, mergePolicySort)) {
          log.warn(
              "unsupported combination: segmentTerminateEarly=true cmdSort={} cmdLen={} mergeSort={}",
              cmdSort,
              cmdLen,
              mergePolicySort);
        } else {
          EarlyTerminatingSortingCollector etsc =
              new EarlyTerminatingSortingCollector(c, cmdSort, cmdLen);
          segmentTermCollectors.add(etsc);
          c = etsc;
        }
      }

      if (cmd.shouldEarlyTerminateSearch()) {
        c = new EarlyTerminatingCollector(c, cmd.getMaxHitsAllowed(), earlyTerminatingHits);
      }

      if (postFilter != null) {
        postFilter.setLastDelegate(c);
        c = postFilter;
      }

      if (cmd.isQueryCancellable()) {
        CancellableCollector cancellable = new CancellableCollector(c, cancellationFlag);
        c = cancellable;
        synchronized (this) {
          if (trackerRegistration == null && cancellableTracker != null) {
            trackerRegistration = cancellable;
            cancellableTracker.addShardLevelActiveQuery(cmd.getQueryID(), cancellable);
          }
        }
      }

      synchronized (this) {
        newCollectorCalled = true;
        if (firstWrappedCollector == null) {
          firstWrappedCollector = c;
        }
      }
      return c;
    }

    /**
     * Returns the {@link ScoreMode} of the first chain-wrapped collector built by {@link
     * #newCollector()}. Used by callers (e.g. {@link SolrIndexSearcher#searchAndCollect}) that need
     * the score mode after applying the chain wrappers — a post-filter, for example, may override
     * its delegate's score mode (e.g. {@code CollapsingPostFilter} returns {@code COMPLETE}), and
     * that override is invisible to the inner per-CM collectors. Returns {@code null} if {@link
     * #newCollector()} has not been called.
     */
    public ScoreMode getScoreMode() {
      return firstWrappedCollector == null ? null : firstWrappedCollector.scoreMode();
    }

    @Override
    public T reduce(Collection<Collector> collectors) throws IOException {
      if (reduced) {
        return reducedResult;
      }
      reduced = true;

      boolean anyTerminatedEarly = false;
      synchronized (segmentTermCollectors) {
        for (EarlyTerminatingSortingCollector etsc : segmentTermCollectors) {
          if (etsc.terminatedEarly()) {
            anyTerminatedEarly = true;
            break;
          }
        }
      }
      if (anyTerminatedEarly && qr != null) {
        qr.setSegmentTerminatedEarly(true);
      }

      // postFilter.complete() emits any buffered docs into the delegate collector chain,
      // which may include EarlyTerminatingCollector. A throw from here must still leave
      // reducedResult populated with whatever the inner collectors managed to gather,
      // because the outer catch handler in SolrIndexSearcher.runChainAndCollect calls
      // chain.reduce(List.of()) a second time to fetch the partial result -- and that
      // second call short-circuits on `reduced == true` above.
      EarlyTerminatingCollectorException pendingEtc = null;
      if (postFilter != null) {
        try {
          postFilter.complete();
        } catch (EarlyTerminatingCollectorException etce) {
          pendingEtc = etce;
        }
      }

      List<C> innerList;
      synchronized (innerCollectors) {
        innerList = new ArrayList<>(innerCollectors);
      }
      reducedResult = inner.reduce(innerList);

      if (pendingEtc != null) {
        throw pendingEtc;
      }
      return reducedResult;
    }

    /**
     * Removes the tracker registration created lazily on first {@link #newCollector()}. Callers
     * must invoke this after the search completes, in a {@code finally} block, to avoid leaking
     * registrations on exception.
     */
    public void cleanup() {
      if (trackerRegistration != null && cancellableTracker != null) {
        cancellableTracker.removeCancellableQuery(cmd.getQueryID());
      }
    }

    public boolean requiresSingleSlice() {
      return postFilter != null;
    }
  }
}
