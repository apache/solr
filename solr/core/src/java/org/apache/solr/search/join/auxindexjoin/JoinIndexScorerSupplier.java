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
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.search.join.auxindexjoin.AuxIndexManager.JoinSegmentReference;
import org.apache.solr.search.join.auxindexjoin.AuxIndexManager.SegmentsTuple;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.DocEdges;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Joins all from-side segments to the single to-side segment.
 *
 * <h2>Instrumentation</h2>
 *
 * Emits TRACE lines prefixed {@code AUXIJOIN} in logfmt (space-separated {@code key=value}), one
 * {@code evt=} per line, so a run can be parsed without a logging-config change:
 *
 * <ul>
 *   <li>{@code evt=build} -- a join-index write happened; carries the wall time it cost. Emitted by
 *       {@code AuxIndexManager#writeJoinSegments}, the chokepoint both build paths share: {@code
 *       cause=eager-create-weight} for the bulk build {nolink AuxIndexManager#ensureJoinSegments}
 *       does at {@link AuxIndexJoinQuery#createWeight} time, and {@code cause=lazy-to-segment} for
 *       the per-context fallback below. In a steady run the eager path does all the work and the
 *       lazy one never fires, so a log with no {@code cause=lazy-to-segment} line is the expected
 *       shape.
 *   <li>{@code evt=ctx} -- this context finished setting up: how many (from, to) pairs contributed,
 *       how many the a-priori from-edge check dropped before any column was opened, and how loose
 *       the resulting approximation is.
 *   <li>{@code evt=drain} -- one join column was read through during confirmation, and whether that
 *       read confirmed the doc under test (an early exit) or not.
 *   <li>{@code evt=done} -- confirmation reached a terminal state for this context: every column
 *       has been drained, so the half-read union has converged. Carries the per-context totals.
 * </ul>
 *
 * Every line carries {@code ctx=}, the {@link #ctxId} of the context it belongs to ({@code -} on an
 * eager build, which precedes every context), so a parser can attribute drains exactly instead of
 * assuming one context per to-segment is alive at a time.
 *
 * <p>The counters are best-effort: they are plain fields on a context confined to one to-segment,
 * and so to one search thread, but nothing enforces that. A context whose confirmation never
 * converges emits no {@code evt=done} line -- absence of one is itself the signal that laziness
 * paid off for that segment, at the cost of that context's final counters going unreported.
 */
class JoinIndexScorerSupplier extends ScorerSupplier {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Per-JVM sequence behind {@link #ctxId}. Only has to distinguish contexts alive at the same
   * time, so wrapping after 2^63 is not a concern; base 36 just keeps the log lines short.
   */
  private static final AtomicLong CTX_SEQ = new AtomicLong();

  /**
   * Identifies this context in the log. Every {@code evt=ctx} / {@code evt=drain} / {@code
   * evt=done} line carries it, so drains can be attributed to their context exactly, rather than by
   * assuming only one context per to-segment is alive -- which only holds when a single query runs
   * at a time.
   */
  private final String ctxId = Long.toString(CTX_SEQ.incrementAndGet(), 36);

  final LeafReaderContext toContext;
  final Query fromQuery;
  final IndexSearcher fromSearcher;
  private final Future<FromLeafJoinContext>[] fromColumnFutures;
  private final ScoreMode scoreMode;
  private final float boost;
  private IndexSearcher lastSeenJoinSearcher;
  final String fromField;
  final String toField;
  private final AuxIndexManager joinIndex;

  // TODO all of these might be final since they are set in the constructor
  private int firstToDoc = DocIdSetIterator.NO_MORE_DOCS;
  private int lastToDoc = -1;
  private long matchedToDocsCount = 0;
  private FixedBitSet falsePositiveToDocsBits = null;

  // ---- instrumentation, best effort; see the class javadoc for the emitted format ----
  /** nanos spent inside {@code AuxIndexManager#writeJoinSegments} on behalf of this context */
  private long joinIndexBuildNanos;

  /** pairs that had a from-side match before the a-priori from-edge check ran */
  private int joinLeafsCreated;

  /** pairs the a-priori from-edge check dropped, i.e. columns never opened at all */
  private int leafsDroppedApriori;

  /** calls to {@link LazyConfimationIterator#matches()} */
  private int confirmCalls;

  /** those answered from the half-read union alone, with no column read */
  private int confirmFreeHits;

  /** columns read through during confirmation */
  private int leafsDrained;

  /** from-docs walked while draining, i.e. the column reads laziness is trying to avoid */
  private long fromDocsWalked;

  /** set once the {@code evt=done} line has been emitted, so it is emitted at most once */
  private boolean reported;

  /** ordered by from-cost descending */
  private final List<LeafJoin> leafJoins = new ArrayList<>();

  private final IndexReader toReader;

  private final class LazyConfimationIterator extends TwoPhaseIterator {
    FixedBitSet falseNegToDocsBits = null;
    private int shift;

    private LazyConfimationIterator(DocIdSetIterator approximation) {
      super(approximation);
    }

    @Override
    public boolean matches() throws IOException {
      confirmCalls++;
      if (leafJoins.isEmpty()) {
        assert falsePositiveToDocsBits.get(approximation.docID());
        confirmFreeHits++;
        return true; /// aproximation is a refined true pos already
      }
      if (falseNegToDocsBits != null) {
        if (falseNegToDocsBits.get(approximation.docID() - shift)) {
          confirmFreeHits++;
          return true;
        } // otherwise we don't know if 0 is real false
      }

      IndexSearcher freshSearcher = JoinIndexScorerSupplier.this.joinIndex.acquire();
      try {
        refreshJoinTasksReferences(freshSearcher);
        assert JoinIndexScorerSupplier.this.lastSeenJoinSearcher == freshSearcher;
        for (LeafJoin joinTask : new ArrayList<>(leafJoins)) {
          if (falseNegToDocsBits == null) {
            this.shift = approximation.docID();
            falseNegToDocsBits = new FixedBitSet(lastToDoc + 1 - shift);
          }
          int walked = joinTask.dumpMatchesInto(falseNegToDocsBits, shift, approximation.docID());
          if (!joinTask.fromSegIterIsNotExausted()) {
            JoinIndexScorerSupplier.this.dropJoinLeaf(joinTask);
            leafsDrained++;
          }
          fromDocsWalked += walked;
          if (!leafJoins.isEmpty()) {
            if (falseNegToDocsBits.get(approximation.docID() - shift)) {
              logDrain(joinTask, walked, true);
              return true;
            } // otherwise we don't know if 0 is real false
          }
          logDrain(joinTask, walked, false);
        }
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        JoinIndexScorerSupplier.this.joinIndex.release(freshSearcher);
      }
      // drop all to masks, got no hit - it means it's a true negative now.

      falsePositiveToDocsBits.clear(shift, lastToDoc + 1);
      FixedBitSet.orRange(
          falseNegToDocsBits, 0, falsePositiveToDocsBits, shift, lastToDoc - shift + 1);

      boolean matched = falsePositiveToDocsBits.get(approximation.docID());
      // every column has now been drained: the half-read union has converged, so from here on
      // every answer -- true or false -- is a free lookup and there is nothing left to prune
      logConfirmationDone(matched ? "converged-on-match" : "converged-on-refutation");
      return matched;
    }

    /**
     * Reports one column read. {@code confirmed=true} is the early exit the lazy variant exists
     * for: the doc under test was found before the remaining {@code cellsLeft} columns were
     * touched.
     */
    private void logDrain(LeafJoin joinTask, int walked, boolean confirmed) {
      if (!JoinIndexUtils.diagnosticsEnabled(log)) {
        return;
      }
      JoinIndexUtils.logDiagnostic(
          log,
          "AUXIJOIN evt=drain ctx={} toSeg={} pair={} confirmed={} walked={} colToCount={}"
              + " cellsLeft={} hCard={} confirmCalls={}",
          ctxId,
          JoinIndexUtils.segmentName(toContext),
          joinTask.pairFieldName,
          confirmed,
          walked,
          joinTask.toCount(),
          leafJoins.size(),
          falseNegToDocsBits == null ? 0 : falseNegToDocsBits.cardinality(),
          confirmCalls);
    }

    @Override
    public float matchCost() {
      return matchedToDocsCount;
    }
  }

  /**
   * Represents a cell in the join matrix bounded to from and to segments. Resolved exactly once,
   * either from the join index ({@link #resolveFromReadingJoinIndex}: just the pair's {@link
   * DocEdges}, with real docvalues opened later, on demand, through {@link #joinSegmentRef} --
   * which keeps being refreshed as the join reader reopens -- or from the indexer ({@link
   * #resolveFromCalculatedModel}: the pair's edges plus an in-memory {@link JoinColumnModel} that
   * needs no further indirection to read.
   */
  class LeafJoin implements DocEdges {
    final String pairFieldName;
    final DocIdSetIterator fromSegmentDocIdIter;
    final long fromMatchCount;
    JoinSegmentReference joinSegmentRef;
    final SegmentsTuple segmentsFromTo;
    private DocEdges edges;
    // set only when resolved from the indexer; null means real docvalues are opened through
    // joinSegmentRef instead
    private JoinColumnModel docMapping;

    LeafJoin(
        String pairFieldName,
        SegmentsTuple segmentsFromTo,
        DocIdSetIterator fromSegmentDocIdIter,
        long fromMatchCount) {
      this.pairFieldName = pairFieldName;
      this.segmentsFromTo = segmentsFromTo;
      this.fromSegmentDocIdIter = fromSegmentDocIdIter;
      this.fromMatchCount = fromMatchCount;
      assert fromSegmentDocIdIter.docID() != DocIdSetIterator.NO_MORE_DOCS;
      assert fromSegmentDocIdIter.docID() >= 0;
    }

    /** Resolves this cell, once, to a join-index-persisted pair's edges. */
    void resolveFromReadingJoinIndex(DocEdges edges) {
      assert this.edges == null : "already resolved: " + this.edges;
      this.edges = edges;
    }

    /** Resolves this cell, once, to an in-memory doc mapping built on demand. */
    void resolveFromCalculatedModel(JoinColumnModel docMapping) {
      assert this.edges == null : "already resolved: " + this.edges;
      this.edges = docMapping.edges();
      this.docMapping = docMapping;
    }

    boolean isResolved() {
      return edges != null;
    }

    SortedNumericDocValues toDocsByFromDocsDV() throws IOException {
      assert edges != null : "not resolved yet: " + this;
      if (docMapping != null) {
        // built on demand, so it's available directly, with no searcher involved
        return docMapping.toDocByFromDoc();
      }
      // resolved from the join index: joinSegmentRef locates the real, on-disk column
      LeafReaderContext joinContext =
          lastSeenJoinSearcher.getLeafContexts().get(joinSegmentRef.joinSegmentLeafOrd());
      assert JoinIndexUtils.segmentName(joinContext).equals(joinSegmentRef.joinSegmentName());
      return joinContext
          .reader()
          .getSortedNumericDocValues(
              JoinIndexUtils.TO_DOC_VAL_BY_FROM_DOCNUM + joinSegmentRef.pairFieldName());
    }

    /**
     * Walks this cell's from-iterator from its current (prepositioned) doc through its edges' last
     * from-doc, setting every to-doc it maps to -- shifted by {@code shift} -- in {@code
     * matchedToDocs}.
     *
     * @return how many from-docs were walked, i.e. the column-read work this drain cost
     */
    int dumpMatchesInto(FixedBitSet matchedToDocs, int shift, int earlyExitDoc) throws IOException {
      SortedNumericDocValues toDocsByFromDoc = toDocsByFromDocsDV();
      int walked = 0;
      boolean confirmedCurrent = false;
      for (int fromDoc = fromSegmentDocIdIter.docID(); // prepositioned to the first match
          fromSegIterIsNotExausted();
          fromDoc = fromSegmentDocIdIter.nextDoc()) {
        walked++;
        if (toDocsByFromDoc.advanceExact(fromDoc)) {
          for (int i = 0; i < toDocsByFromDoc.docValueCount(); i++) {
            int toDocMatch = (int) toDocsByFromDoc.nextValue();
            assert toDocMatch <= toDocEdges()[1] && toDocMatch >= toDocEdges()[0]
                : "to doc "
                    + toDocMatch
                    + " above edges union max "
                    + Arrays.toString(toDocEdges());
            // shift is wherever the approximation iterator first landed, which need not be
            // the global firstToDoc (e.g. under a boolean conjunction); a match below shift
            // is unreachable -- the iterator only moves forward -- so it's dropped rather
            // than written at a negative offset
            if (toDocMatch >= shift) {
              matchedToDocs.set(toDocMatch - shift);
              if (toDocMatch == earlyExitDoc) {
                confirmedCurrent = true;
              }
            }
          }
          if (confirmedCurrent) {
            // we've just confirmed the doc, which was matches() is called for
            return walked; // giveup
          }
        }
      }
      return walked;
    }

    private boolean fromSegIterIsNotExausted() {
      return fromSegmentDocIdIter.docID() != DocIdSetIterator.NO_MORE_DOCS
          && fromSegmentDocIdIter.docID() <= fromDocEdges()[1];
    }

    @Override
    public int[] fromDocEdges() {
      return edges.fromDocEdges();
    }

    @Override
    public int[] toDocEdges() {
      return edges.toDocEdges();
    }

    @Override
    public int toCount() {
      return edges.toCount();
    }
  }

  /** Appends {@code cell} to {@link #leafJoins} and registers it in both indices. */
  private LeafJoin addJoinLeaf(LeafJoin cell) {
    leafJoins.add(cell);
    return cell;
  }

  /**
   * Drops {@code cell} from {@link #leafJoins}. It's a little bit awkward. Every iteration join
   * task list is copied and then removed from original list via reference equality. Ideally,
   * iterators should be used for removals.
   */
  private void dropJoinLeaf(LeafJoin cell) {
    leafJoins.remove(cell);
  }

  record TaskRefreshResult(
      Set<Map.Entry<LeafJoin, LeafReaderContext>> joinSegments,
      Set<Map.Entry<LeafJoin, JoinColumnModel>> justWritten) {}

  /**
   * @param weightAgeJoinSegmentsReadOnly the join segments cached at {@link
   *     AuxIndexJoinQuery#createWeight} time DON'T MODIFY ME!!!
   * @param weightAgeJoinSearcher the join searcher cached at {@link AuxIndexJoinQuery#createWeight}
   *     time
   * @param scorerSupplierAgeJoinSearcher the join searcher used at {@link
   *     JoinIndexWeight#scorerSupplier} time
   * @param fromColumnFutures from side data by segment ord.
   */
  JoinIndexScorerSupplier(
      LeafReaderContext toContext,
      String fromField,
      Query fromQuery,
      IndexSearcher fromSearcher,
      String toField,
      IndexReader toReader,
      Map<String, JoinSegmentReference> weightAgeJoinSegmentsReadOnly,
      IndexSearcher weightAgeJoinSearcher,
      IndexSearcher scorerSupplierAgeJoinSearcher,
      AuxIndexManager joinIndex,
      Future<FromLeafJoinContext>[] fromColumnFutures,
      ScoreMode scoreMode,
      float boost)
      throws ExecutionException, InterruptedException, IOException {
    this.toContext = toContext;
    this.fromField = fromField;
    this.fromQuery = fromQuery;
    this.fromSearcher = fromSearcher;
    this.toField = toField;
    this.toReader = toReader;
    this.fromColumnFutures = fromColumnFutures;
    this.joinIndex = joinIndex;
    this.scoreMode = scoreMode;
    this.boost = boost;

    // 1. check from scorers
    List<LeafJoin> fromItersTasks = createLeafJoins();
    for (LeafJoin newJoinTask : fromItersTasks) {
      this.joinLeafsCreated++;
      this.addJoinLeaf(newJoinTask);
      // 2. set old segment references
      JoinSegmentReference oldReference =
          weightAgeJoinSegmentsReadOnly.get(newJoinTask.pairFieldName);
      if (oldReference != null) {
        newJoinTask.joinSegmentRef = oldReference;
      }
    }

    this.lastSeenJoinSearcher =
        weightAgeJoinSearcher; // set old searcher, it corresponds to weightAgeJoinSegmentsReadOnly
    TaskRefreshResult refreshedAndNew = refreshJoinTasksReferences(scorerSupplierAgeJoinSearcher);
    for (Entry<LeafJoin, LeafReaderContext> entry : refreshedAndNew.joinSegments) {
      LeafJoin task = entry.getKey();
      LeafReaderContext joinLeaf = entry.getValue(); // got it from searcher leafs by refOrd
      assert JoinIndexUtils.segmentName(joinLeaf).equals(task.joinSegmentRef.joinSegmentName());
      assert joinLeaf.ord == task.joinSegmentRef.joinSegmentLeafOrd();
      task.resolveFromReadingJoinIndex( // ok. this one may be ready for search.
          new JoinIndexUtils.Edges(
              JoinIndexUtils.loadEdges(
                  joinLeaf, JoinIndexUtils.FROM_EDGES_PREFIX + task.pairFieldName),
              // TODO it might not need to be loaded, if "from" edges fully cut of the column. Thus,
              // we won't read even "to" edges at all.
              JoinIndexUtils.loadEdges(
                  joinLeaf, JoinIndexUtils.TO_EDGES_PREFIX + task.pairFieldName),
              // TODO use it for ordering join segment iteration, desc
              JoinIndexUtils.loadEdges(
                  joinLeaf, JoinIndexUtils.TO_COUNT_PREFIX + task.pairFieldName)[0]));
    }
    for (Entry<LeafJoin, JoinColumnModel> entry : refreshedAndNew.justWritten) {
      LeafJoin task = entry.getKey();
      task.resolveFromCalculatedModel(entry.getValue());
    }
    // edges are  loaded
    for (LeafJoin task :
        List.copyOf(
            leafJoins)) { // hell. it might remove task from the list. that's sad. I have to copy
      // it.
      assert task.isResolved();
      // a little bit tricky. It assumes that column is join-index backed or just written and
      // array-backed,
      advanceAtMinFromEdge(task);
    }
    // now let's read each cell's edges, then build "to" side bitset of approximation
    // first pass: union the contributing pairs' to-doc ranges; every possible match in this
    // to segment falls into [minToDoc, maxToDoc]
    for (LeafJoin task : leafJoins) {
      DocEdges docEdges = task;
      firstToDoc = Math.min(firstToDoc, docEdges.toDocEdges()[0]);
      lastToDoc = Math.max(lastToDoc, docEdges.toDocEdges()[1]);
      matchedToDocsCount += docEdges.toDocEdges()[1] - docEdges.toDocEdges()[0] + 1;
      if (falsePositiveToDocsBits == null) {
        falsePositiveToDocsBits = new FixedBitSet(toContext.reader().maxDoc());
      }
      falsePositiveToDocsBits.set(docEdges.toDocEdges()[0], docEdges.toDocEdges()[1] + 1);
    }
    // TODO and only here is worth to order cells by count in join column
    logContextSetUp();
  }

  /**
   * Reports how this context was set up, once. {@code approxCard} is the real size of the
   * approximation (the union of the surviving pairs' to-ranges), while {@code approxSpanSum} adds
   * those ranges up with their overlaps counted twice; the two together say how much the
   * single-range-per-column approximation actually narrows the segment, which is what bounds every
   * saving the confirmation phase can make.
   */
  private void logContextSetUp() {
    if (!JoinIndexUtils.diagnosticsEnabled(log)) {
      return;
    }
    long colToCountSum = 0;
    for (LeafJoin cell : leafJoins) {
      colToCountSum += cell.toCount();
    }
    JoinIndexUtils.logDiagnostic(
        log,
        "AUXIJOIN evt=ctx ctx={} toSeg={} toMaxDoc={} cellsCreated={} cellsDroppedApriori={}"
            + " cellsLive={} buildMs={} approxCard={} approxSpanSum={} approxFrom={} approxTo={}"
            + " colToCountSum={}",
        ctxId,
        JoinIndexUtils.segmentName(toContext),
        toContext.reader().maxDoc(),
        joinLeafsCreated,
        leafsDroppedApriori,
        leafJoins.size(),
        joinIndexBuildNanos / 1_000_000L,
        falsePositiveToDocsBits == null ? 0 : falsePositiveToDocsBits.cardinality(),
        matchedToDocsCount,
        firstToDoc,
        lastToDoc,
        colToCountSum);
  }

  /**
   * Reports the confirmation totals for this context, once. Emitted when every column has been
   * drained -- i.e. the half-read union has converged and laziness has run out. A context that
   * never emits this line never had to converge, which is the case the lazy variant exists for.
   */
  private void logConfirmationDone(String reason) {
    if (reported || !JoinIndexUtils.diagnosticsEnabled(log)) {
      return;
    }
    reported = true;
    JoinIndexUtils.logDiagnostic(
        log,
        "AUXIJOIN evt=done ctx={} toSeg={} reason={} confirmCalls={} freeHits={} cellsDrained={}"
            + " cellsLive={} fromDocsWalked={} buildMs={}",
        ctxId,
        JoinIndexUtils.segmentName(toContext),
        reason,
        confirmCalls,
        confirmFreeHits,
        leafsDrained,
        leafJoins.size(),
        fromDocsWalked,
        joinIndexBuildNanos / 1_000_000L);
  }

  private TaskRefreshResult refreshJoinTasksReferences(IndexSearcher newJoinIndexSearcher)
      throws IOException, ExecutionException, InterruptedException {
    Set<Map.Entry<LeafJoin, LeafReaderContext>> joinSegments = new LinkedHashSet<>();
    Set<Map.Entry<LeafJoin, JoinColumnModel>> justWritten = new LinkedHashSet<>();

    Set<LeafJoin> refreshReference = new LinkedHashSet<>();
    Set<LeafJoin> loadReference = new LinkedHashSet<>();
    Map<String, LeafJoin> needIndex = new LinkedHashMap<>();
    Set<LeafJoin> resolveTarget =
        (newJoinIndexSearcher == this.lastSeenJoinSearcher) ? loadReference : refreshReference;
    for (LeafJoin task : leafJoins) {
      if (task.joinSegmentRef != null) {
        resolveTarget.add(task);
      } else if (!task.isResolved()) {
        needIndex.put(task.pairFieldName, task);
      }
      // else: resolved from the indexer
    }
    // refresh old refs, pass 1: same searcher, just get a segment by ord and check
    // the segment name
    List<LeafReaderContext> newLeaves = newJoinIndexSearcher.getLeafContexts();
    for (Iterator<LeafJoin> iter = refreshReference.iterator(); iter.hasNext(); ) {
      LeafJoin task = iter.next();
      // check segment name by ord, if true, resolve it right here, remove from here
      LeafReaderContext byOrd =
          task.joinSegmentRef.joinSegmentLeafOrd() < newLeaves.size()
              ? newLeaves.get(task.joinSegmentRef.joinSegmentLeafOrd())
              : null;
      if (byOrd != null
          && JoinIndexUtils.segmentName(byOrd).equals(task.joinSegmentRef.joinSegmentName())) {
        joinSegments.add(new SimpleEntry<>(task, byOrd));
        iter.remove();
      }
    }
    // pass 2: searcher have changed, need to lookup segments by name in the new one
    if (!refreshReference.isEmpty()) {
      Map<String, LeafJoin> byOldJoinSegName = new HashMap<>();
      for (LeafJoin task : refreshReference) {
        byOldJoinSegName.put(task.joinSegmentRef.joinSegmentName(), task);
      }
      for (LeafReaderContext joinLeaf : newLeaves) {
        String segName = JoinIndexUtils.segmentName(joinLeaf);
        LeafJoin task = byOldJoinSegName.get(segName);
        if (task != null) {
          task.joinSegmentRef =
              new JoinSegmentReference(task.joinSegmentRef.pairFieldName(), segName, joinLeaf.ord);
          joinSegments.add(new SimpleEntry<>(task, joinLeaf));
          refreshReference.remove(task);
        }
      }
    }
    // pass 3: search by field name
    if (!refreshReference.isEmpty()) {
      Map<String, LeafJoin> byPairFieldName = new HashMap<>();
      for (LeafJoin task : refreshReference) {
        byPairFieldName.put(task.joinSegmentRef.pairFieldName(), task);
      }
      // loop join segments search for fields
      Map<String, JoinSegmentReference> joinSegmentsByPairFieldName =
          JoinIndexUtils.extractExistingJoinColumns(
              lastSeenJoinSearcher, byPairFieldName::containsKey);
      // if found move to load set
      for (LeafJoin task : byPairFieldName.values()) {
        JoinSegmentReference found = joinSegmentsByPairFieldName.get(task.pairFieldName);
        if (found != null) {
          task.joinSegmentRef = found;
          loadReference.add(task);
          refreshReference.remove(task);
        }
      }
    }
    if (!refreshReference.isEmpty()) { // TODO presumably we can go to index it
      throw new IllegalStateException(
          "unable to refresh segment refs " + refreshReference + " at " + lastSeenJoinSearcher);
    }
    // load edges for regulars, repeat pass 1, for those who was found at pass 3
    for (LeafJoin cell : loadReference) {
      // String pairFieldName = cell.pairFieldName;
      LeafReaderContext joinLeafSeg =
          lastSeenJoinSearcher.getLeafContexts().get(cell.joinSegmentRef.joinSegmentLeafOrd());
      assert JoinIndexUtils.segmentName(joinLeafSeg).equals(cell.joinSegmentRef.joinSegmentName());
      joinSegments.add(new SimpleEntry<>(cell, joinLeafSeg));
    }
    // index unlucky ones
    if (!needIndex.isEmpty()) {

      Map<String, SegmentsTuple> missingPairs = new HashMap<>();
      for (LeafJoin cell : needIndex.values()) {
        missingPairs.put(cell.pairFieldName, cell.segmentsFromTo);
      }
      long buildStartNanos = System.nanoTime();
      Map<String, JoinColumnModel> written =
          this.joinIndex.buildAndPersistJoinColumns(
              Collections.unmodifiableMap(missingPairs),
              this.fromSearcher.getIndexReader(),
              this.toReader,
              this.toField,
              this.ctxId,
              // null refs trace back to the weight-age extract, so that's the searcher these
              // pairs were last seen absent in; still lastSeenJoinSearcher at this point --
              // it advances to newJoinIndexSearcher only after this method
              this.lastSeenJoinSearcher,
              fromColumnFutures);
      this.joinIndexBuildNanos += System.nanoTime() - buildStartNanos;
      assert written.keySet().containsAll(missingPairs.keySet());
      assert missingPairs.keySet().containsAll(written.keySet());
      for (Map.Entry<String, JoinColumnModel> entry : written.entrySet()) { // TODO optimize
        LeafJoin cell = needIndex.get(entry.getKey());
        justWritten.add(new SimpleEntry<>(cell, entry.getValue()));
      }
    }
    this.lastSeenJoinSearcher =
        newJoinIndexSearcher; // set old searcher, it corresponds to weightAgeJoinSegmentsReadOnly
    return new TaskRefreshResult(joinSegments, justWritten);
  }

  /**
   * Positions {@code cell}'s from-iterator behind {@code docEdges}'s first from-doc, or drops
   * {@code cell} via {@link #dropJoinLeaf} when the iterator can no longer reach any doc the pair
   * maps -- either because the pair maps nothing ({-1, -1} sentinel), the iterator already moved
   * past the pair's last from-doc, or it exhausts before reaching the pair's first one (the
   * iterator only moves forward, so none of these are recoverable). Returns whether the cell
   * survives.
   */
  private boolean advanceAtMinFromEdge(LeafJoin cell) throws IOException {
    int[] fromDocEdges = cell.fromDocEdges();
    int minFromDoc = fromDocEdges[0];
    int maxFromDoc = fromDocEdges[1];
    DocIdSetIterator fromSegemtIter = cell.fromSegmentDocIdIter;
    if (minFromDoc < 0) {
      // {-1, -1} sentinel: this pair maps no from doc to any to doc at all
      leafsDroppedApriori++;
      dropJoinLeaf(cell);
      return false;
    }
    if (maxFromDoc >= 0
        && maxFromDoc != DocIdSetIterator.NO_MORE_DOCS
        && fromSegemtIter.docID() > maxFromDoc) {
      // from iter is already past the last from doc this pair maps, so it cannot contribute
      leafsDroppedApriori++;
      dropJoinLeaf(cell); // no more matches in this join segment, so the pair cannot contribute
      return false;
    }
    if (minFromDoc >= 0
        && maxFromDoc != DocIdSetIterator.NO_MORE_DOCS
        && fromSegemtIter.docID() < minFromDoc) {
      int firstMatch = fromSegemtIter.advance(minFromDoc);
      if (firstMatch == DocIdSetIterator.NO_MORE_DOCS || firstMatch > maxFromDoc) {
        /// wow from iter exhausted, no match in this join segment, so the pair cannot contribute
        // thus we need to return them from request `
        leafsDroppedApriori++;
        dropJoinLeaf(cell); // no more matches in this join segment, so the pair cannot contribute
        return false;
      } // else from iter is advanced behind the first from match , good
    }
    return true;
  }

  /**
   * every to segment call for prepositioned from seg iters populates a {@link LeafJoin} per
   * contributing from segment, its iterator PREPOSITIONED to the first matching doc
   *
   * @return tasks are orfered by descending from-side match count, so the first task is the one
   *     with the most matches
   */
  private List<LeafJoin> createLeafJoins()
      throws ExecutionException, InterruptedException, IOException {
    List<LeafReaderContext> leaves = new ArrayList<>(this.fromSearcher.getLeafContexts());
    Collections.shuffle(leaves, ThreadLocalRandom.current());

    List<LeafJoin> tasks = new ArrayList<>();
    for (LeafReaderContext fromContext : leaves) {
      FromLeafJoinContext matchAndCount = this.fromColumnFutures[fromContext.ord].get();
      if (matchAndCount == null) {
        continue;
      }
      DocIdSetIterator matchedFromDocs;
      if (matchAndCount.matches != null
          && (matchedFromDocs = matchAndCount.matches.iterator()) != null
          && matchedFromDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        // name every contributing (from, to) pair column; pair field names are unique across pairs
        String pairFieldName =
            JoinIndexUtils.pairFieldName(fromContext, this.fromField, toContext, this.toField);
        tasks.add(
            new LeafJoin(
                pairFieldName,
                new SegmentsTuple(fromContext.ord, toContext.ord),
                matchedFromDocs,
                matchAndCount.matches.count()));
      }
    }
    // process the from segments with the most matches first
    // TODO won't we reorder them again then? why do we do it here though?
    tasks.sort(Comparator.<LeafJoin>comparingLong(t -> t.fromMatchCount).reversed());
    return tasks;
  }

  /** True if this context has any candidate docs at all; used by {@code JoinIndexWeight}. */
  boolean isEmpty() {
    return falsePositiveToDocsBits == null || matchedToDocsCount == 0;
  }

  @Override
  public Scorer get(long leadCost) throws IOException {
    assert !isEmpty();
    DocIdSetIterator approximation =
        new BitSetIterator(falsePositiveToDocsBits, matchedToDocsCount);

    TwoPhaseIterator twoPhase = new LazyConfimationIterator(approximation);
    return new ConstantScoreScorer(boost, scoreMode, twoPhase);
  }

  @Override
  public long cost() {
    return (long) lastToDoc - firstToDoc + 1;
  }
}
