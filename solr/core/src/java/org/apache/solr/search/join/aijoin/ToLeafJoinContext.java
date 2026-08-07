package org.apache.solr.search.join.aijoin;

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
import org.apache.solr.search.join.aijoin.AIJoinIndex.JoinSegmentReference;
import org.apache.solr.search.join.aijoin.AIJoinIndex.SegmentsTuple;
import org.apache.solr.search.join.aijoin.AIJoinUtil.DocEdges;
import org.apache.solr.search.join.aijoin.AIJoinUtil.JoinColumnModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO prune by "to" range if it's under slice searching TODO pass raw cacheless searcher
 *
 * <h2>Instrumentation</h2>
 *
 * Emits INFO lines prefixed {@code AIJOIN} in logfmt (space-separated {@code key=value}), one
 * {@code evt=} per line, so a run can be parsed without a logging-config change:
 *
 * <ul>
 *   <li>{@code evt=build} -- a join-index write happened; carries the wall time it cost. Emitted by
 *       {@link AIJoinIndex#writeJoinSegments}, the chokepoint both build paths share: {@code
 *       cause=eager-create-weight} for the bulk build {@link AIJoinIndex#ensureJoinSegments} does
 *       at {@link AIJoinQuery#createWeight} time, and {@code cause=lazy-to-segment} for the
 *       per-context fallback below. In a steady run the eager path does all the work and the lazy
 *       one never fires, so a log with no {@code cause=lazy-to-segment} line is the expected shape.
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
class ToLeafJoinContext {
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
  private final Future<AIJoinUtil.CacheAndCount>[] fromDocIdSetFutures;
  private IndexSearcher lastSeenJoinSearcher;
  final String fromField;
  final String toField;
  private final AIJoinIndex joinIndex;

  // TODO all of these might be final since they are set in the constructor
  private int firstToDoc = DocIdSetIterator.NO_MORE_DOCS;
  private int lastToDoc = -1;
  private long matchedToDocsCount = 0;
  private FixedBitSet falsePositiveToDocsBits = null;

  /** ordered by from-cost descending */
  private final List<JoinTask> joinCells = new ArrayList<>();

  // ---- instrumentation, best effort; see the class javadoc for the emitted format ----
  /** nanos spent inside {@link AIJoinIndex#writeJoinSegments} on behalf of this context */
  private long joinIndexBuildNanos;

  /** pairs that had a from-side match before the a-priori from-edge check ran */
  private int cellsCreated;

  /** pairs the a-priori from-edge check dropped, i.e. columns never opened at all */
  private int cellsDroppedApriori;

  /** calls to {@link LazyRefineTwoPhIter#matches()} */
  private int confirmCalls;

  /** those answered from the half-read union alone, with no column read */
  private int confirmFreeHits;

  /** columns read through during confirmation */
  private int cellsDrained;

  /** from-docs walked while draining, i.e. the column reads laziness is trying to avoid */
  private long fromDocsWalked;

  /** set once the {@code evt=done} line has been emitted, so it is emitted at most once */
  private boolean reported;

  // secondary indices over joinCells, kept in sync by addJoinTask/removeJoinCell: every cell is
  // reachable both by its from-segment ordinal (dense, so a plain array) and by its pair field
  // name (sparse across the full from-segment space, so a map)
  private final JoinTask[] joinCellsByFromSegOrd;
  private final Map<String, JoinTask> joinCellsByPairFieldName = new HashMap<>();
  private IndexReader toReader;

  // TODO kept alongside LazyRefineTwoPhIter for comparison/rollback; not currently wired up
  private final class EagerRefineTwoPhIter extends TwoPhaseIterator {
    boolean pruned = false;

    private EagerRefineTwoPhIter(DocIdSetIterator approximation) {
      super(approximation);
    }

    @Override
    public boolean matches() throws IOException {
      if (!pruned) {
        FixedBitSet matchedToDocs;
        int shift;
        // prune the approximation to the resolved matches: drop the
        // remaining range bits and or the matches back in at their
        // absolute positions, so the approximation stops visiting
        // non-matching docs
        shift = approximation.docID();
        // TODO the this is: it's enough to just confirm the match the return the control.
        // TODO there, should be a refined bitset
        // TODO when we need to refine an approx, we go throug join segments,
        // TODO and drop them into refined bitset until we confirm the match
        // TODO the order of iteration is: to side doc freq, which we neeed to persist and read then
        // TODO once we drop a join segment to refined bitset we exclude it from iterations
        // TODO also, just boundary check the following matches checks
        // TODO the following matches() checks, at first confirm with refied bitset,
        // TODO if it's false procede with to bitset dumping into refined bitset
        matchedToDocs = refineToMatches(shift);
        falsePositiveToDocsBits.clear(shift, lastToDoc + 1);
        FixedBitSet.orRange(
            matchedToDocs, 0, falsePositiveToDocsBits, shift, lastToDoc - shift + 1);
        pruned = true;
        return falsePositiveToDocsBits.get(approximation.docID());
      }
      // the bitset spans [shift, lastToDoc] shifted to zero
      // return matchedToDocs.get(approximation.docID() - shift);
      assert /*return*/ falsePositiveToDocsBits.get(approximation.docID()); // always true ??
      return true;
    }

    @Override
    public float matchCost() {
      return matchedToDocsCount;
    }
  }

  private final class LazyRefineTwoPhIter extends TwoPhaseIterator {
    FixedBitSet falseNegToDocsBits = null;
    private int shift;

    private LazyRefineTwoPhIter(DocIdSetIterator approximation) {
      super(approximation);
    }

    @Override
    public boolean matches() throws IOException {
      confirmCalls++;
      if (joinCells.isEmpty()) {
        assert falsePositiveToDocsBits.get(approximation.docID());
        confirmFreeHits++;
        return true; /// aprox is a true pos already
      }
      if (falseNegToDocsBits != null) {
        if (falseNegToDocsBits.get(approximation.docID() - shift)) {
          confirmFreeHits++;
          return true;
        } // otherwise we don't know if 0 is real false
      }

      IndexSearcher freshSearcher = ToLeafJoinContext.this.joinIndex.acquire();
      try {
        refreshJoinTasksReferences(freshSearcher);
        assert ToLeafJoinContext.this.lastSeenJoinSearcher == freshSearcher;
        for (JoinTask cell : new ArrayList<>(joinCells)) {
          if (falseNegToDocsBits == null) {
            this.shift = approximation.docID();
            falseNegToDocsBits = new FixedBitSet(lastToDoc + 1 - shift);
          }
          int walked = cell.dumpMatchesInto(falseNegToDocsBits, shift);
          ToLeafJoinContext.this.removeJoinCell(cell);
          cellsDrained++;
          fromDocsWalked += walked;
          if (!joinCells.isEmpty()) {
            if (falseNegToDocsBits.get(approximation.docID() - shift)) {
              logDrain(cell, walked, true);
              return true;
            } // otherwise we don't know if 0 is real false
          }
          logDrain(cell, walked, false);
        }
      } finally {
        ToLeafJoinContext.this.joinIndex.release(freshSearcher);
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
    private void logDrain(JoinTask cell, int walked, boolean confirmed) {
      if (!AIJoinUtil.diagnosticsEnabled(log)) {
        return;
      }
      AIJoinUtil.logDiagnostic(
          log,
          "AIJOIN evt=drain ctx={} toSeg={} pair={} confirmed={} walked={} colToCount={}"
              + " cellsLeft={} hCard={} confirmCalls={}",
          ctxId,
          AIJoinUtil.segmentName(toContext),
          cell.pairFieldName,
          confirmed,
          walked,
          cell.toCount(),
          joinCells.size(),
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
   * either from the join index ({@link #resolveFromIndex}: just the pair's {@link DocEdges}, with
   * real docvalues opened later, on demand, through {@link #joinSegmentRef} -- which keeps being
   * refreshed as the join reader reopens -- or from the indexer ({@link #resolveFromIndexer}: the
   * pair's edges plus an in-memory {@link JoinColumnModel} that needs no further indirection to
   * read.
   */
  class JoinTask implements DocEdges {
    final String pairFieldName;
    final DocIdSetIterator fromSegmentDocIdIter;
    final long fromMatchCount;
    JoinSegmentReference joinSegmentRef;
    final SegmentsTuple segmentsFromTo;
    private DocEdges edges;
    // set only when resolved from the indexer; null means real docvalues are opened through
    // joinSegmentRef instead
    private JoinColumnModel docMapping;

    JoinTask(
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
    void resolveFromIndex(DocEdges edges) {
      assert this.edges == null : "already resolved: " + this.edges;
      this.edges = edges;
    }

    /**
     * Resolves this cell, once, to an in-memory doc mapping built on demand.
     *
     * <p>TODO decide whether this path still earns its keep. It is reached only when {@link
     * #refreshJoinTasksReferences} found a pair with no column, and {@link
     * AIJoinIndex#ensureJoinSegments} has already built every pair the query needs at {@link
     * AIJoinQuery#createWeight} time -- so in a steady run it never fires. Instrumenting a 3000
     * to-segment run produced no lazy build at all (see the class javadoc: {@code evt=build
     * cause=lazy-to-segment}), i.e. {@code docMapping} was null throughout and every column was
     * read from disk through {@link #joinSegmentRef}. It is not dead, though: it covers a pair that
     * disappeared between weight creation and scoring, which the reaper in {@link
     * AIJoinMergePolicy} can do. Before deleting it, confirm that case is handled elsewhere.
     */
    void resolveFromIndexer(JoinColumnModel docMapping) {
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
      assert AIJoinUtil.segmentName(joinContext).equals(joinSegmentRef.joinSegmentName());
      return joinContext
          .reader()
          .getSortedNumericDocValues(
              AIJoinUtil.TO_DOC_VAL_BY_FROM_DOCNUM + joinSegmentRef.pairFieldName());
    }

    /**
     * Walks this cell's from-iterator from its current (prepositioned) doc through its edges' last
     * from-doc, setting every to-doc it maps to -- shifted by {@code shift} -- in {@code
     * matchedToDocs}.
     *
     * @return how many from-docs were walked, i.e. the column-read work this drain cost
     */
    int dumpMatchesInto(FixedBitSet matchedToDocs, int shift) throws IOException {
      SortedNumericDocValues toDocsByFromDoc = toDocsByFromDocsDV();
      int walked = 0;
      for (int fromDoc = fromSegmentDocIdIter.docID(); // prepositioned to the first match
          fromDoc != DocIdSetIterator.NO_MORE_DOCS && fromDoc <= fromDocEdges()[1];
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
            }
          }
        }
      }
      return walked;
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

  /** Appends {@code cell} to {@link #joinCells} and registers it in both indices. */
  private JoinTask addJoinTask(JoinTask cell) {
    joinCells.add(cell);
    joinCellsByFromSegOrd[cell.segmentsFromTo.fromLeafOrd()] = cell;
    joinCellsByPairFieldName.put(cell.pairFieldName, cell);
    return cell;
  }

  /**
   * Drops {@code cell} from {@link #joinCells} and both indices, so a pair found to no longer
   * contribute disappears from every view of it at once.
   */
  private void removeJoinCell(JoinTask cell) {
    joinCells.remove(cell);
    joinCellsByFromSegOrd[cell.segmentsFromTo.fromLeafOrd()] = null;
    joinCellsByPairFieldName.remove(cell.pairFieldName);
  }

  /** Looks up a cell by from-segment ordinal, or {@code null} if none is registered there. */
  JoinTask cellByFromSegOrd(int fromSegOrd) {
    return joinCellsByFromSegOrd[fromSegOrd];
  }

  /** Looks up a cell by pair field name, or {@code null} if none is registered under that name. */
  JoinTask cellByPairFieldName(String pairFieldName) {
    return joinCellsByPairFieldName.get(pairFieldName);
  }

  record TaskRefreshResult(
      Set<Map.Entry<JoinTask, LeafReaderContext>> joinSegements,
      Set<Map.Entry<JoinTask, JoinColumnModel>> justWritten) {}

  /**
   * @param weightAgeJoinSegmentsReadOnly the join segments cached at {@link
   *     AIJoinQuery#createWeight} time DON'T MODIFY ME!!!
   * @param weightAgeJoinSearcher the join searcher cached at {@link AIJoinQuery#createWeight} time
   * @param scorerSupplierAgeJoinSearcher the join searcher used at {@link
   *     AIJoinWeight#scorerSupplier} time
   * @param fromDocIdSetFutures
   */
  ToLeafJoinContext(
      LeafReaderContext toContext,
      String fromField,
      Query fromQuery,
      IndexSearcher fromSearcher,
      String toField,
      IndexReader toReader,
      Map<String, JoinSegmentReference> weightAgeJoinSegmentsReadOnly,
      IndexSearcher weightAgeJoinSearcher,
      IndexSearcher scorerSupplierAgeJoinSearcher,
      AIJoinIndex joinIndex,
      Future<AIJoinUtil.CacheAndCount>[] fromDocIdSetFutures)
      throws IOException, ExecutionException, InterruptedException {
    this.toContext = toContext;
    this.fromField = fromField;
    this.fromQuery = fromQuery;
    this.fromSearcher = fromSearcher;
    this.toField = toField;
    this.toReader = toReader;
    this.fromDocIdSetFutures = fromDocIdSetFutures;

    this.joinIndex = joinIndex;
    this.joinCellsByFromSegOrd = new JoinTask[fromSearcher.getLeafContexts().size()];

    // 1. check from scorers
    for (JoinTask newJoinTask : createFromItersTasks()) {
      this.cellsCreated++;
      this.addJoinTask(newJoinTask);
      // 2. set old segment refernces
      JoinSegmentReference oldReference =
          weightAgeJoinSegmentsReadOnly.get(newJoinTask.pairFieldName);
      if (oldReference != null) {
        newJoinTask.joinSegmentRef = oldReference;
      }
    }

    this.lastSeenJoinSearcher =
        weightAgeJoinSearcher; // set old searcher, it correspondts to weightAgeJoinSegmentsReadOnly
    TaskRefreshResult refreshedAndNew = refreshJoinTasksReferences(scorerSupplierAgeJoinSearcher);
    for (Entry<JoinTask, LeafReaderContext> entry : refreshedAndNew.joinSegements) {
      JoinTask cell = entry.getKey();
      LeafReaderContext joinLeaf = entry.getValue(); // got it from searcher leafs by refOrd
      assert AIJoinUtil.segmentName(joinLeaf).equals(cell.joinSegmentRef.joinSegmentName());
      assert joinLeaf.ord == cell.joinSegmentRef.joinSegmentLeafOrd();
      cell.resolveFromIndex( // ok. this one may be ready for search.
          new AIJoinUtil.Edges(
              AIJoinUtil.loadEdges(joinLeaf, AIJoinUtil.FROM_EDGES_PREFIX + cell.pairFieldName),
              AIJoinUtil.loadEdges(joinLeaf, AIJoinUtil.TO_EDGES_PREFIX + cell.pairFieldName),
              // TODO use it for ordering join segment iteration, desc
              AIJoinUtil.loadEdges(joinLeaf, AIJoinUtil.TO_COUNT_PREFIX + cell.pairFieldName)[0]));
    }
    for (Entry<JoinTask, JoinColumnModel> entry : refreshedAndNew.justWritten) {
      JoinTask cell = entry.getKey();
      cell.resolveFromIndexer(entry.getValue());
    }
    // edges are  loaded
    for (JoinTask cell :
        List.copyOf(
            joinCells)) { // hell. it might remove task from the list. that's sad. I have to copy
      // it.
      assert cell.isResolved();
      // a little bit tricky. It assumes that column is join-index backed or just written and
      // array-backed,
      advanceAtMinFromEdge(cell);
    }
    // now let's read each cell's edges, then build "to" side bitset of approximation
    // first pass: union the contributing pairs' to-doc ranges; every possible match in this
    // to segment falls into [minToDoc, maxToDoc]
    for (JoinTask cell : joinCells) {
      DocEdges docEdges = cell;
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
    if (!AIJoinUtil.diagnosticsEnabled(log)) {
      return;
    }
    long colToCountSum = 0;
    for (JoinTask cell : joinCells) {
      colToCountSum += cell.toCount();
    }
    AIJoinUtil.logDiagnostic(
        log,
        "AIJOIN evt=ctx ctx={} toSeg={} toMaxDoc={} cellsCreated={} cellsDroppedApriori={}"
            + " cellsLive={} buildMs={} approxCard={} approxSpanSum={} approxFrom={} approxTo={}"
            + " colToCountSum={}",
        ctxId,
        AIJoinUtil.segmentName(toContext),
        toContext.reader().maxDoc(),
        cellsCreated,
        cellsDroppedApriori,
        joinCells.size(),
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
    if (reported || !AIJoinUtil.diagnosticsEnabled(log)) {
      return;
    }
    reported = true;
    AIJoinUtil.logDiagnostic(
        log,
        "AIJOIN evt=done ctx={} toSeg={} reason={} confirmCalls={} freeHits={} cellsDrained={}"
            + " cellsLive={} fromDocsWalked={} buildMs={}",
        ctxId,
        AIJoinUtil.segmentName(toContext),
        reason,
        confirmCalls,
        confirmFreeHits,
        cellsDrained,
        joinCells.size(),
        fromDocsWalked,
        joinIndexBuildNanos / 1_000_000L);
  }

  private TaskRefreshResult refreshJoinTasksReferences(IndexSearcher newJoinIndexSearcher)
      throws IOException {
    Set<Map.Entry<JoinTask, LeafReaderContext>> joinSegements = new LinkedHashSet<>();
    Set<Map.Entry<JoinTask, JoinColumnModel>> justWritten = new LinkedHashSet<>();

    Set<JoinTask> refeshReference = new LinkedHashSet<>();
    Set<JoinTask> loadReference = new LinkedHashSet<>();
    Map<String, JoinTask> needIndex = new LinkedHashMap<>();
    for (JoinTask task : joinCells) {
      JoinSegmentReference oldReference = task.joinSegmentRef;
      if (oldReference != null) {
        task.joinSegmentRef = oldReference;
        if (newJoinIndexSearcher == this.lastSeenJoinSearcher) {
          loadReference.add(task);
        } else {
          refeshReference.add(task);
        }
      } else {
        needIndex.put(task.pairFieldName, task);
      }
    }
    // referesh old refs, pass 1: same ord, same segment name -> the leaf is already in hand, so
    // resolve straight into joinSegements instead of adding to loadReference just to re-fetch the
    // very same leaf by ord in the "load edges for regulars" loop below
    List<LeafReaderContext> newLeaves = newJoinIndexSearcher.getLeafContexts();
    for (Iterator<JoinTask> iter = refeshReference.iterator(); iter.hasNext(); ) {
      JoinTask task = iter.next();
      // check segment name by ord, if true, resolve it right here, remove from here
      LeafReaderContext byOrd =
          task.joinSegmentRef.joinSegmentLeafOrd() < newLeaves.size()
              ? newLeaves.get(task.joinSegmentRef.joinSegmentLeafOrd())
              : null;
      if (byOrd != null
          && AIJoinUtil.segmentName(byOrd).equals(task.joinSegmentRef.joinSegmentName())) {
        joinSegements.add(new SimpleEntry<>(task, byOrd));
        iter.remove();
      }
    }
    // pass 2
    if (!refeshReference.isEmpty()) {
      Map<String, JoinTask> byOldJoinSegName = new HashMap<>();
      for (JoinTask task : refeshReference) {
        byOldJoinSegName.put(task.joinSegmentRef.joinSegmentName(), task);
      }
      for (LeafReaderContext joinLeaf : newLeaves) {
        String segName = AIJoinUtil.segmentName(joinLeaf);
        JoinTask task = byOldJoinSegName.get(segName);
        if (task != null) {
          // renamed segment, found by scanning for its old name -- joinLeaf is already the
          // resolved leaf, so resolve straight into joinSegements, same as pass 1
          task.joinSegmentRef =
              new JoinSegmentReference(task.joinSegmentRef.pairFieldName(), segName, joinLeaf.ord);
          joinSegements.add(new SimpleEntry<>(task, joinLeaf));
          refeshReference.remove(task);
        }
      }
    }
    // pass 3
    if (!refeshReference.isEmpty()) {
      Map<String, JoinTask> byPairFieldName = new HashMap<>();
      for (JoinTask task : refeshReference) {
        byPairFieldName.put(task.joinSegmentRef.pairFieldName(), task);
      }
      // loop join segments search for fields
      Map<String, JoinSegmentReference> joinSegmentsByPairFieldName =
          AIJoinIndex.extractExistingJoinColumns(
              lastSeenJoinSearcher, byPairFieldName::containsKey);
      // if found move to load set
      for (JoinTask task : byPairFieldName.values()) {
        JoinSegmentReference found = joinSegmentsByPairFieldName.get(task.pairFieldName);
        if (found != null) {
          task.joinSegmentRef = found;
          loadReference.add(task);
          refeshReference.remove(task);
        }
      }
    }
    if (!refeshReference.isEmpty()) { // TODO presumabily we can go to index it
      throw new IllegalStateException(
          "unable to refresh segment refs " + refeshReference + " at " + lastSeenJoinSearcher);
    }
    // load edges for regulars
    for (JoinTask cell : loadReference) {
      // String pairFieldName = cell.pairFieldName;
      LeafReaderContext joinFeafSeg =
          lastSeenJoinSearcher.getLeafContexts().get(cell.joinSegmentRef.joinSegmentLeafOrd());
      assert AIJoinUtil.segmentName(joinFeafSeg).equals(cell.joinSegmentRef.joinSegmentName());
      joinSegements.add(new SimpleEntry<>(cell, joinFeafSeg));
    }
    // index for unlucked
    if (!needIndex.isEmpty()) {

      Map<String, SegmentsTuple> missingPairs = new HashMap<>();
      for (JoinTask cell : needIndex.values()) {
        missingPairs.put(cell.pairFieldName, cell.segmentsFromTo);
      }
      // the build itself is timed and reported by AIJoinIndex#writeJoinSegments, which is the
      // chokepoint both this lazy path and the eager AIJoinQuery#createWeight path go through;
      // here we only accumulate what it cost this context, for the evt=ctx / evt=done lines
      long buildStartNanos = System.nanoTime();
      Map<String, JoinColumnModel> written =
          this.joinIndex.writeJoinSegments(
              Collections.unmodifiableMap(missingPairs),
              this.fromSearcher.getIndexReader(),
              this.fromField,
              this.toReader,
              this.toField,
              AIJoinIndex.BuildCause.LAZY_TO_SEGMENT,
              this.ctxId);
      this.joinIndexBuildNanos += System.nanoTime() - buildStartNanos;
      assert written.keySet().containsAll(missingPairs.keySet());
      assert missingPairs.keySet().containsAll(written.keySet());
      for (Map.Entry<String, JoinColumnModel> entry : written.entrySet()) { // TODO optimize
        JoinTask cell = needIndex.get(entry.getKey());
        justWritten.add(new SimpleEntry<>(cell, entry.getValue()));
      }
    }
    this.lastSeenJoinSearcher =
        newJoinIndexSearcher; // set old searcher, it correspondts to weightAgeJoinSegmentsReadOnly
    return new TaskRefreshResult(joinSegements, justWritten);
  }

  /**
   * Positions {@code cell}'s from-iterator behind {@code docEdges}'s first from-doc, or drops
   * {@code cell} via {@link #removeJoinCell} when the iterator can no longer reach any doc the pair
   * maps -- either because the pair maps nothing ({-1, -1} sentinel), the iterator already moved
   * past the pair's last from-doc, or it exhausts before reaching the pair's first one (the
   * iterator only moves forward, so none of these are recoverable). Returns whether the cell
   * survives.
   */
  private boolean advanceAtMinFromEdge(JoinTask cell) throws IOException {
    int[] fromDocEdges = cell.fromDocEdges();
    int minFromDoc = fromDocEdges[0];
    int maxFromDoc = fromDocEdges[1];
    DocIdSetIterator fromSegemtIter = cell.fromSegmentDocIdIter;
    if (minFromDoc < 0) {
      // {-1, -1} sentinel: this pair maps no from doc to any to doc at all
      cellsDroppedApriori++;
      removeJoinCell(cell);
      return false;
    }
    if (maxFromDoc >= 0
        && maxFromDoc != DocIdSetIterator.NO_MORE_DOCS
        && fromSegemtIter.docID() > maxFromDoc) {
      // from iter is already past the last from doc this pair maps, so it cannot contribute
      cellsDroppedApriori++;
      removeJoinCell(cell); // no more matches in this join segment, so the pair cannot contribute
      return false;
    }
    if (minFromDoc >= 0
        && maxFromDoc != DocIdSetIterator.NO_MORE_DOCS
        && fromSegemtIter.docID() < minFromDoc) {
      int firstMatch = fromSegemtIter.advance(minFromDoc);
      if (firstMatch == DocIdSetIterator.NO_MORE_DOCS || firstMatch > maxFromDoc) {
        /// wow from iter exhausted, no match in this join segment, so the pair cannot contribute
        // thus we need to return them from request `
        cellsDroppedApriori++;
        removeJoinCell(cell); // no more matches in this join segment, so the pair cannot contribute
        return false;
      } // else from iter is advanced behind the first from match , good
    }
    return true;
  }

  /**
   * every to segment call for prepositioned from seg iters populates a {@link JoinTask} per
   * contributing from segment, its iterator PREPOSITIONED to the first matching doc
   *
   * @return tasks are orfered by descending from-side match count, so the first task is the one
   *     with the most matches
   * @throws IOException
   */
  private List<JoinTask> createFromItersTasks()
      throws IOException, ExecutionException, InterruptedException {
    List<LeafReaderContext> leaves = new ArrayList<>(this.fromSearcher.getLeafContexts());
    Collections.shuffle(leaves, ThreadLocalRandom.current());

    List<JoinTask> tasks = new ArrayList<>();
    for (LeafReaderContext fromContext : leaves) {
      AIJoinUtil.CacheAndCount matchAndCount = this.fromDocIdSetFutures[fromContext.ord].get();
      if (matchAndCount == null) {
        continue;
      }
      DocIdSetIterator matchedFromDocs = matchAndCount.iterator();
      if (matchedFromDocs != null && matchedFromDocs.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        // name every contributing (from, to) pair column; pair field names are unique across pairs
        String pairFieldName =
            AIJoinUtil.pairFieldName(fromContext, this.fromField, toContext, this.toField);
        tasks.add(
            new JoinTask(
                pairFieldName,
                new SegmentsTuple(fromContext.ord, toContext.ord),
                matchedFromDocs,
                matchAndCount.count()));
      }
    }
    // process the from segments with the most matches first
    // TODO won't we reorder them again then? why do we do it here though?
    tasks.sort(Comparator.<JoinTask>comparingLong(t -> t.fromMatchCount).reversed());
    return tasks;
  }

  public ScorerSupplier scorerSupplier(ScoreMode scoreMode, float boost) {
    if (falsePositiveToDocsBits == null || matchedToDocsCount == 0) {
      return null; // no matches in this to segment
    }
    return new ScorerSupplier() {
      @Override
      public Scorer get(long leadCost) throws IOException {
        DocIdSetIterator approximation =
            new BitSetIterator(falsePositiveToDocsBits, matchedToDocsCount);

        TwoPhaseIterator twoPhase = new LazyRefineTwoPhIter(approximation);
        return new ConstantScoreScorer(boost, scoreMode, twoPhase);
      }

      @Override
      public long cost() {
        return lastToDoc - firstToDoc + 1;
      }
    };
  }

  /**
   * TODO this refines false positeve approximation , but it can iteratively refine false-negative
   * docset, this let us giveup looping join tasks
   * @deprecated called from EagerRefineTwoPhIter which is out of use now
   */
  private FixedBitSet refineToMatches(int shift) throws IOException {
    FixedBitSet matchedToDocs = new FixedBitSet(lastToDoc - shift + 1);
    IndexSearcher freshSearcher = this.joinIndex.acquire();
    try {

      refreshJoinTasksReferences(freshSearcher);
      assert this.lastSeenJoinSearcher == freshSearcher;
      for (JoinTask cell : joinCells) {
        cell.dumpMatchesInto(matchedToDocs, shift);
      }
    } finally { // TODO release before looping remaining cells separately
      this.joinIndex.release(freshSearcher);
    }
    return matchedToDocs;
  }
}
