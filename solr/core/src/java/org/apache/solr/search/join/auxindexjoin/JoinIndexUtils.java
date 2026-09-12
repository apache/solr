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
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ParallelCompositeReader;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.apache.lucene.util.packed.PagedMutable;
import org.slf4j.Logger;
import org.slf4j.event.Level;

/**
 * Column-building and addressing helpers for the auxiliary join index managed by {@link
 * AuxIndexManager}: for every (from-segment, to-segment) pair it produces a SORTED_NUMERIC column
 * named {@link #pairFieldName}, whose doc number is the from-side doc id and whose value is the
 * to-side doc id whose {@code toField} term equals the from doc's {@code fromField} term, plus two
 * companion edges columns persisting the pair's {min, max} from-doc and to-doc bounds.
 *
 * @lucene.experimental
 */
final class JoinIndexUtils {

  /** Suffix of the always-written column persisting a pair's {min, max} from-doc edges. */
  static final String FROM_EDGES_PREFIX = "fromDoc_edges_"; // TODO reduce to the singe letter

  /** Suffix of the always-written column persisting a pair's {min, max} to-doc edges. */
  static final String TO_EDGES_PREFIX = "toDoc_edges_";

  /** main join columns for join index to_doc_num[from_docnum] */
  static final String TO_DOC_VAL_BY_FROM_DOCNUM = "join_toDoc_";

  static final String TO_COUNT_PREFIX = "num_toDoc_";

  /**
   * Every field one pair occupies in a sidecar segment -- the inverse of {@link #pairFieldNames},
   * which recovers pair names from field names. Reaping a pair means hiding exactly these; keep the
   * two in step if a fifth column is ever added.
   */
  static Set<String> columnFieldNames(String pairFieldName) {
    return Set.of(
        TO_DOC_VAL_BY_FROM_DOCNUM + pairFieldName,
        FROM_EDGES_PREFIX + pairFieldName,
        TO_EDGES_PREFIX + pairFieldName,
        TO_COUNT_PREFIX + pairFieldName);
  }

  /**
   * Commit user-data key under which the {@code SolrVersion} that wrote the index is recorded, so a
   * future major version can detect and discard an incompatible aux join index.
   */
  static final String AUX_INDEX_VERSION = "AuxJoinIndexVersion";

  /**
   * Values per page in every paged structure this join builds -- columns, the from-side maps, the
   * to-side inversion. Sized so a page stays comfortably under G1's humongous threshold even in the
   * worst case: 16384 values at the full 64 bits is 128KB, against half of a 1MB region.
   */
  static final int PAGE_SIZE = 1 << 14;

  private JoinIndexUtils() {}

  /**
   * Level the {@code AIJOIN evt=...} diagnostic logs emit at; kept in one place so all diagnostics
   * can be raised or lowered together. To see them, enable {@code TRACE} for the emitting loggers
   * in log4j config.
   */
  private static final Level LOG_LEVEL = Level.TRACE;

  /** Whether the AIJOIN diagnostic logs would emit at the configured level. */
  static boolean diagnosticsEnabled(Logger log) {
    return log.isEnabledForLevel(LOG_LEVEL);
  }

  /** Emits an AIJOIN diagnostic line at the configured level. */
  static void logDiagnostic(Logger log, String message, Object... args) {
    log.atLevel(LOG_LEVEL).log(message, args);
  }

  /**
   * Scans {@code joinSearcher}'s leaves for every pair column whose field name satisfies {@code
   * isNeeded}, returning where each one lives. Used both to seed a fresh {@link JoinIndexWeight}'s
   * view of already-built pairs, and by {@link JoinIndexScorerSupplier} to relocate a pair whose
   * cached segment reference no longer resolves. TODO subject for in-heap caching TODO commit's
   * userdata might have a list of pairs with known segment ords and names
   */
  static Map<String, AuxIndexManager.JoinSegmentReference> extractExistingJoinColumns(
      IndexSearcher joinSearcher, Predicate<String> isNeeded) {
    Map<String, AuxIndexManager.JoinSegmentReference> existingJoinSegments =
        CollectionUtil.newHashMap(joinSearcher.getIndexReader().leaves().size());
    for (LeafReaderContext joinContext : joinSearcher.getIndexReader().leaves()) {
      String segmentName = segmentName(joinContext);
      for (FieldInfo fieldInfo : joinContext.reader().getFieldInfos()) {
        // pairs are detected by their toCount companion, which is written to doc 0 for every
        // built pair; the join column itself is sparse and a tombstone pair (disjoint terms)
        // never materializes it, so scanning for join columns kept re-reporting once-built
        // tombstones as missing -- and re-triggering their from-side FK loads on every query
        String splits[] = fieldInfo.name.split(TO_COUNT_PREFIX);
        if (splits.length == 2 && isNeeded.test(splits[1])) {
          existingJoinSegments.computeIfAbsent(
              splits[1],
              fieldName ->
                  new AuxIndexManager.JoinSegmentReference(
                      fieldName, segmentName, joinContext.ord));
        }
      }
    }
    return existingJoinSegments;
  }

  static CacheAndCount computeDocIdSet(Weight fromWeight, LeafReaderContext ctx)
      throws IOException {
    ScorerSupplier supplier = fromWeight.scorerSupplier(ctx);
    if (supplier == null) {
      return null; // NO matches ???
    }
    BulkScorer scorer = supplier.bulkScorer();
    return cacheImpl(scorer, ctx.reader().maxDoc(), ctx.reader().getLiveDocs());
  }

  /**
   * A pair's {min, max} from-doc and to-doc bounds and match count, common to both a pair freshly
   * built on demand ({@link JoinColumnModel#edges()}) and one already persisted in the join index
   * ({@link Edges}, loaded through {@link #loadEdges}), so code walking matches doesn't need to
   * care which one backs it.
   */
  interface DocEdges {
    int[] fromDocEdges();

    int[] toDocEdges();

    /** this is rather doubtful */
    int toCount();
  }

  /** A self-contained {@link DocEdges} value, with no addressing information of its own. */
  record Edges(int[] fromDocEdges, int[] toDocEdges, int toCount) implements DocEdges {}

  /**
   * The from-doc -> to-doc map produced by {@link #computeDocMapping}, paired with its resolved
   * {@link #edges()}. {@link #toDocByFromDoc()} mirrors the on-disk column's read API, so freshly
   * built pairs (not yet flushed to the join index) and pairs loaded from the join index can be
   * walked by the same code.
   *
   * <p>Two layouts back that read API, chosen per pair by {@link #sparseIsSmaller} from the match
   * count the measuring pass established:
   *
   * <ul>
   *   <li><b>dense</b> -- one slot per from-doc up to the pair's last match, holding {@code toDoc +
   *       1} so that {@code 0} means "no match here". Addressed straight by from-doc, so a probe is
   *       one read.
   *   <li><b>sparse</b> -- only the from-docs that do have a match, ascending, alongside the to-doc
   *       each maps to. A probe searches the from-docs, which a cursor keeps linear by resuming
   *       where the previous one stopped.
   * </ul>
   *
   * <p>Sparse is the smaller of the two below 50% density, and the layouts are far from evenly
   * used: over 34k column drains from a production run, 71% of the columns matched under 1% of
   * their from-segment's docs (median 2,125 of 5,031,945) while the other 29% matched essentially
   * every doc.
   *
   * <p>Both layouts are held as {@link PackedLongValues}, which stores values at the width they
   * need rather than a flat 32 bits, and pages them at {@link #PAGE_SIZE} rather than in one array.
   * Paging is the point: a five-million-doc from-segment as one {@code int[]} is 20MB, and G1 gives
   * any allocation over half a region (512KB at a 1MB region size) its own run of regions, so
   * columns like that had taken a production heap 74% humongous -- unreclaimable by anything short
   * of a full collection, which is how this crashed. No page here reaches 128KB whatever the
   * column's width or density, so no column is ever allocated humongous again; the narrower packing
   * (23 bits for five million to-docs) is a second, smaller win on top.
   */
  static final class JoinColumnModel {

    private final int maxDoc;
    private final DocEdges edges;

    /**
     * Dense layout: {@code toDoc + 1} at each from-doc, {@code 0} where the from-doc has no match,
     * running to the last matched from-doc and no further. Null when sparse.
     */
    private final PackedLongValues toDocPlusOneByFromDoc;

    /** Sparse layout: the from-docs carrying a match, ascending. Null when dense. */
    private final PackedLongValues matchedFromDocs;

    /** Sparse layout: the to-doc of the from-doc at the same index. Null when dense. */
    private final PackedLongValues matchedToDocs;

    private JoinColumnModel(
        int maxDoc,
        DocEdges edges,
        PackedLongValues toDocPlusOneByFromDoc,
        PackedLongValues matchedFromDocs,
        PackedLongValues matchedToDocs) {
      this.maxDoc = maxDoc;
      this.edges = edges;
      this.toDocPlusOneByFromDoc = toDocPlusOneByFromDoc;
      this.matchedFromDocs = matchedFromDocs;
      this.matchedToDocs = matchedToDocs;
    }

    /**
     * Collects one pair's matches into the layout {@code sparse} selects. Matches must arrive in
     * ascending from-doc order, which is how both build passes walk the from-segment; the dense
     * layout relies on it to know how many empty slots to lay down before each match.
     */
    static final class Builder {
      private final int maxDoc;
      private final boolean sparse;
      private final PackedLongValues.Builder matchedFromDocs;
      private final PackedLongValues.Builder toDocs;

      /** Dense layout: how many slots are already laid down, i.e. the next from-doc to fill. */
      private int nextSlot;

      private int matches;

      Builder(int maxDoc, boolean sparse) {
        this.maxDoc = maxDoc;
        this.sparse = sparse;
        // from-docs only ever ascend, and the monotonic encoding stores each one as its distance
        // from a linear approximation of the whole column -- so a nearly-dense column, whose
        // from-docs step by one, costs about nothing to remember, which is exactly the column
        // whose to-docs are most expensive.
        this.matchedFromDocs =
            sparse ? PackedLongValues.monotonicBuilder(PAGE_SIZE, PackedInts.COMPACT) : null;
        this.toDocs = PackedLongValues.packedBuilder(PAGE_SIZE, PackedInts.COMPACT);
      }

      /** Records that {@code fromDoc} matches {@code toDoc}. */
      void add(int fromDoc, int toDoc) {
        assert fromDoc >= nextSlot : "matches must ascend, got " + fromDoc + " after " + nextSlot;
        assert toDoc >= 0 : "a match must name a to-doc, got " + toDoc;
        if (sparse) {
          matchedFromDocs.add(fromDoc);
          toDocs.add(toDoc);
        } else {
          while (nextSlot < fromDoc) {
            toDocs.add(0L); // no match at this from-doc
            nextSlot++;
          }
          toDocs.add(toDoc + 1L);
        }
        nextSlot = fromDoc + 1;
        matches++;
      }

      /** How many matches were added, i.e. what {@code edges.toCount()} has to be. */
      int matches() {
        return matches;
      }

      JoinColumnModel build(DocEdges edges) {
        assert matches == edges.toCount() : "added " + matches + " matches for edges " + edges;
        return sparse
            ? new JoinColumnModel(maxDoc, edges, null, matchedFromDocs.build(), toDocs.build())
            : new JoinColumnModel(maxDoc, edges, toDocs.build(), null, null);
      }
    }

    /** A builder laying out a {@code maxDoc}-wide pair densely or sparsely. */
    static Builder builder(int maxDoc, boolean sparse) {
      return new Builder(maxDoc, sparse);
    }

    /** A model laid out densely from {@code toDocByFromDoc}, {@code -1} meaning no match. */
    static JoinColumnModel dense(int[] toDocByFromDoc, DocEdges edges) {
      return of(toDocByFromDoc, edges, false);
    }

    /** The same map as {@link #dense}, laid out as its matches only. */
    static JoinColumnModel sparse(int[] toDocByFromDoc, DocEdges edges) {
      return of(toDocByFromDoc, edges, true);
    }

    private static JoinColumnModel of(int[] toDocByFromDoc, DocEdges edges, boolean sparse) {
      Builder builder = builder(toDocByFromDoc.length, sparse);
      for (int fromDoc = 0; fromDoc < toDocByFromDoc.length; fromDoc++) {
        if (toDocByFromDoc[fromDoc] >= 0) {
          builder.add(fromDoc, toDocByFromDoc[fromDoc]);
        }
      }
      return builder.build(edges);
    }

    /**
     * A model for a pair that matches nothing, holding no per-doc storage at all. It still reports
     * {@code maxDoc}, so a batch made only of tombstones is still sized to the from-segments it
     * stands for, and its edges are the symmetric {@code {-1, -1}} sentinel: an asymmetric one
     * (e.g. {@code {NO_MORE_DOCS, -1}}) doesn't round-trip through the join index's SORTED_NUMERIC
     * edges column, which always returns its two values in ascending numeric order regardless of
     * which was written as "min".
     */
    static JoinColumnModel tombstone(int maxDoc) {
      return builder(maxDoc, true).build(new Edges(new int[] {-1, -1}, new int[] {-1, -1}, 0));
    }

    /**
     * Whether {@code toCount} matches spread over {@code denseSlots} from-docs are cheaper held as
     * matches than as a slot per from-doc -- i.e. whether the column is under 50% dense. Sparse
     * remembers two values per match against dense's one per slot, and while its from-doc column
     * actually costs far less than that (see {@link Builder}), counting it at full price keeps the
     * choice a comparison the measuring pass can make with no second guess about how well anything
     * will pack.
     */
    static boolean sparseIsSmaller(int toCount, int denseSlots) {
      return (long) toCount * 2 < denseSlots;
    }

    /**
     * Returns a fresh single-valued cursor over the from-doc -> to-doc map, positioned before doc
     * 0. Both cursors only move forward, which is what the on-disk columns require of their readers
     * anyway; see {@link SparseColumnValues}.
     */
    SortedNumericDocValues toDocByFromDoc() {
      return toDocPlusOneByFromDoc != null
          ? new DenseColumnValues(toDocPlusOneByFromDoc)
          : new SparseColumnValues(matchedFromDocs, matchedToDocs);
    }

    DocEdges edges() {
      return edges;
    }

    /** Returns one greater than the largest possible document number. */
    public int maxDoc() {
      return maxDoc;
    }

    /** Whether this model holds only its matches, rather than a slot per from-doc. */
    boolean isSparse() {
      return toDocPlusOneByFromDoc == null;
    }

    /** What this model's per-doc storage costs on the heap, for the build diagnostics. */
    long ramBytesUsed() {
      return isSparse()
          ? matchedFromDocs.ramBytesUsed() + matchedToDocs.ramBytesUsed()
          : toDocPlusOneByFromDoc.ramBytesUsed();
    }
  }

  /**
   * The dense layout's cursor: {@code toDoc + 1} addressed straight by from-doc, {@code 0} where
   * there is no match, so a probe is a single read and needs no search. Slots past the pair's last
   * match were never laid down, which {@link #size} stands for. Always single-valued until M:N
   * pairs are supported.
   */
  private static final class DenseColumnValues extends SortedNumericDocValues {
    private final PackedLongValues toDocPlusOneByFromDoc;
    private final int size;
    private int doc = -1;

    /** The to-doc at {@link #doc}, read by the same probe that found it. */
    private long toDoc;

    DenseColumnValues(PackedLongValues toDocPlusOneByFromDoc) {
      this.toDocPlusOneByFromDoc = toDocPlusOneByFromDoc;
      this.size = Math.toIntExact(toDocPlusOneByFromDoc.size());
    }

    @Override
    public long nextValue() {
      return toDoc;
    }

    @Override
    public int docValueCount() {
      return 1;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      if (target >= size) {
        return false;
      }
      long toDocPlusOne = toDocPlusOneByFromDoc.get(target);
      toDoc = toDocPlusOne - 1;
      return toDocPlusOne != 0;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return doc == NO_MORE_DOCS ? NO_MORE_DOCS : advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      for (int fromDoc = target; fromDoc < size; fromDoc++) {
        long toDocPlusOne = toDocPlusOneByFromDoc.get(fromDoc);
        if (toDocPlusOne != 0) {
          toDoc = toDocPlusOne - 1;
          doc = fromDoc;
          return doc;
        }
      }
      doc = NO_MORE_DOCS;
      return doc;
    }

    @Override
    public long cost() {
      return size;
    }
  }

  /**
   * The sparse layout's cursor: the same read API over the matched from-docs (ascending, no
   * duplicates while the mapping stays single-valued) and their to-docs.
   *
   * <p>Unlike the dense cursor, which is addressed by from-doc directly, this one has to find the
   * from-doc -- so it only moves forward, resuming each seek where the previous one stopped, which
   * keeps a full walk of the column linear instead of a binary search per doc. That is the standard
   * {@link org.apache.lucene.index.DocValuesIterator} contract, and the one the on-disk columns
   * already impose on the same call sites; both of them here -- {@code
   * JoinColumnDocWriter.PairColumn} walking {@code nextDoc()} up the batch, and {@code
   * JoinIndexScorerSupplier.LeafJoin dumpMatchesInto} calling {@code advanceExact(fromDoc)} along a
   * forward-only from-doc iterator -- take a freshly positioned cursor and only ever move it
   * forward.
   */
  private static final class SparseColumnValues extends SortedNumericDocValues {
    private final PackedLongValues matchedFromDocs;
    private final PackedLongValues matchedToDocs;
    private final int size;

    /** Where the last seek stopped: the first match at or after the doc last asked for. */
    private int index;

    /** The from-doc at {@link #index}, or {@code NO_MORE_DOCS} once the column is spent. */
    private int indexedFromDoc;

    private int doc = -1;

    SparseColumnValues(PackedLongValues matchedFromDocs, PackedLongValues matchedToDocs) {
      this.matchedFromDocs = matchedFromDocs;
      this.matchedToDocs = matchedToDocs;
      this.size = Math.toIntExact(matchedFromDocs.size());
    }

    @Override
    public long nextValue() {
      return matchedToDocs.get(index);
    }

    @Override
    public int docValueCount() {
      return 1;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public boolean advanceExact(int target) {
      seek(target);
      doc = target;
      return indexedFromDoc == target;
    }

    @Override
    public int nextDoc() {
      return doc == NO_MORE_DOCS ? NO_MORE_DOCS : advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      seek(target);
      doc = indexedFromDoc;
      return doc;
    }

    /**
     * Positions {@link #index} on the first match at or after {@code target}, and {@link
     * #indexedFromDoc} on its from-doc. Matches before the cursor can't answer a target that never
     * goes backwards, so the search starts there: stepping to the next match or two -- how a full
     * walk of the column advances -- costs a read, and only a real jump falls through to a binary
     * search over what is left.
     */
    private void seek(int target) {
      assert doc == NO_MORE_DOCS || target >= doc
          : "sparse cursor only moves forward, got " + target + " at " + doc;
      int at = index;
      for (int probe = 0; probe < 2 && at < size; probe++, at++) {
        int fromDoc = (int) matchedFromDocs.get(at);
        if (fromDoc >= target) {
          index = at;
          indexedFromDoc = fromDoc;
          return;
        }
      }
      int low = at;
      int high = size - 1;
      while (low <= high) {
        int mid = (low + high) >>> 1;
        int fromDoc = (int) matchedFromDocs.get(mid);
        if (fromDoc < target) {
          low = mid + 1;
        } else if (fromDoc > target) {
          high = mid - 1;
        } else {
          index = mid;
          indexedFromDoc = fromDoc;
          return;
        }
      }
      index = low;
      indexedFromDoc = low < size ? (int) matchedFromDocs.get(low) : NO_MORE_DOCS;
    }

    @Override
    public long cost() {
      return size;
    }
  }

  /**
   * Maps a to-side {@link LeafReaderContext} to the live to-side doc id at each of its ordinals,
   * for a fixed {@code toField}. this is quite local lifecycle class, thus we can cache it so. the
   * trick is that, there always single entry in this cache.
   *
   * <p>The inversion is addressed by ordinal in no particular order, so unlike a column it has to
   * stay mutable -- but it is still paged, at {@link #PAGE_SIZE}, and packed at the bits a to-doc
   * needs rather than a flat 32: a five-million-doc to-segment inverted to one {@code int[]} was a
   * 20MB humongous allocation per to-segment loaded.
   */
  static class ToDocInvertor implements Function<LeafReaderContext, PagedMutable> {
    private final String toField;
    private final Map<Integer, PagedMutable> cache = new HashMap<>();

    ToDocInvertor(String toField) {
      this.toField = toField;
    }

    @Override
    public PagedMutable apply(LeafReaderContext toContext) {
      return cache.computeIfAbsent(toContext.ord, (i) -> computeToDoc(toContext));
    }

    /** {@code toDoc + 1} at each to-ord, {@code 0} where no live to-doc carries that ordinal. */
    private PagedMutable computeToDoc(LeafReaderContext toContext) {
      try {
        SortedSetDocValues toDV = DocValues.getSortedSet(toContext.reader(), toField);
        Bits toLiveDocs = toContext.reader().getLiveDocs();
        PagedMutable toDocPlusOneByToOrd =
            new PagedMutable(
                toDV.getValueCount(),
                PAGE_SIZE,
                PackedInts.bitsRequired(toContext.reader().maxDoc()),
                PackedInts.COMPACT);
        for (int toDoc = toDV.nextDoc();
            toDoc != DocIdSetIterator.NO_MORE_DOCS;
            toDoc = toDV.nextDoc()) {
          if (toLiveDocs != null && !toLiveDocs.get(toDoc)) {
            continue;
          }
          for (int i = 0; i < toDV.docValueCount(); i++) {
            toDocPlusOneByToOrd.set(toDV.nextOrd(), toDoc + 1L);
          }
        }
        return toDocPlusOneByToOrd;
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  /**
   * Builds the join column for one (from-segment, to-segment) pair: resolves every from-side doc to
   * its matching to-side doc id, along with the pair's from-doc and to-doc bounds. From-side terms
   * are hashed by {@link ForeignKeyColumn}; each to-side term is looked up in that hash to map
   * from-side ords to to-side ords.
   *
   * <p>Docs already deleted at build time are skipped, purely to avoid persisting entries nobody
   * can ever match -- deletes are otherwise re-checked live at query time (from-side in {@code
   * JoinIndexScorerSupplier}, to-side by the searcher's own {@code acceptDocs}), since a pair's
   * cached mapping outlives whatever gets deleted after it was built.
   */
  static JoinColumnModel computeDocMapping(
      LeafReaderContext toContext,
      String toField,
      ForeignKeyColumn fromSideData,
      ToDocInvertor toDocInvertor)
      throws IOException {
    assert fromSideData != null;

    SortedSetDocValues toDV = DocValues.getSortedSet(toContext.reader(), toField);
    // addressed by from-ord in term order, not doc order, so this one stays mutable -- but paged
    // and packed all the same: as a flat long[] it was 40MB for a five-million-term from side,
    // allocated afresh for every pair built
    PagedMutable toOrdPlusOneByFromOrd =
        new PagedMutable(
            fromSideData.getFromValuesCount(),
            PAGE_SIZE,
            PackedInts.bitsRequired(toDV.getValueCount()),
            PackedInts.COMPACT);
    TermsEnum toTerms = toDV.termsEnum();
    // resolve from-side ords to to-side ords: look each to-side term up in the from-side hash.
    boolean termsAreDisjoint = true;
    for (BytesRef term = toTerms.next(); term != null; term = toTerms.next()) {
      int fromOrd = fromSideData.getFromTermOrdOrDashOne(term);
      if (fromOrd != -1) {
        toOrdPlusOneByFromOrd.set(fromOrd, toTerms.ord() + 1);
        termsAreDisjoint = false;
      }
    }
    // TODO: this degrades M:N joins to M:1. Both toDocByToOrd and the resolved column keep a single
    // to-side doc per from doc, so when several to docs share a term (non-unique toField) or a
    // fromSideData doc is multi-valued with several matching terms, later assignments overwrite
    // earlier ones and only the last match survives. The read side (AuxIndexJoinQuery) already
    // consumes all docValueCount() values per doc, so only this writer needs to learn to emit
    // multiple to docs per fromSideData doc.
    if (termsAreDisjoint) { // no from-side term occurs on the to side: nothing to lay out at all
      return JoinColumnModel.tombstone(fromSideData.fromSideMaxDocs());
    }
    PagedMutable toDocPlusOneByToOrd = toDocInvertor.apply(toContext);
    int fromSideMaxDocs = fromSideData.fromSideMaxDocs();

    // pass one: measure. Nothing is allocated until the match count is known, which is what decides
    // the layout -- and lets the layout be allocated at exactly its final size.
    int minFromDoc = DocIdSetIterator.NO_MORE_DOCS;
    int maxFromDoc = -1;
    int minToDoc = DocIdSetIterator.NO_MORE_DOCS;
    int maxToDoc = -1;
    int toCount = 0;
    for (ForeignKeyColumn.FromOrds fromOrds = fromSideData.fromOrds(); fromOrds.next(); ) {
      int toDoc = resolveToDoc(fromOrds.fromOrd(), toOrdPlusOneByFromOrd, toDocPlusOneByToOrd);
      if (toDoc == -1) {
        continue;
      }
      int fromDoc = fromOrds.fromDoc();
      minFromDoc = Math.min(minFromDoc, fromDoc);
      maxFromDoc = fromDoc;
      minToDoc = Math.min(minToDoc, toDoc);
      maxToDoc = Math.max(maxToDoc, toDoc);
      toCount++;
    }
    if (maxFromDoc < 0) { // terms overlapped, but no live from-doc reaches a live to-doc
      return JoinColumnModel.tombstone(fromSideMaxDocs);
    }
    DocEdges edges =
        new Edges(new int[] {minFromDoc, maxFromDoc}, new int[] {minToDoc, maxToDoc}, toCount);

    // pass two: lay the matches out, over the same walk. The dense layout lays down a slot per
    // from-doc only as far as the last match, so that span, not the segment's width, is what it
    // would cost -- and so what the layout is chosen on.
    JoinColumnModel.Builder column =
        JoinColumnModel.builder(
            fromSideMaxDocs, JoinColumnModel.sparseIsSmaller(toCount, maxFromDoc + 1));
    for (ForeignKeyColumn.FromOrds fromOrds = fromSideData.fromOrds(); fromOrds.next(); ) {
      int toDoc = resolveToDoc(fromOrds.fromOrd(), toOrdPlusOneByFromOrd, toDocPlusOneByToOrd);
      if (toDoc != -1) {
        column.add(fromOrds.fromDoc(), toDoc);
      }
    }
    assert column.matches() == toCount
        : "measured " + toCount + " matches, laid out " + column.matches();
    return column.build(edges);
  }

  /**
   * The to-side doc a from-doc's {@code fromOrd} reaches, or {@code -1} when the from-doc has no
   * value, its term has no to-side match, or that to-side term's only doc is deleted. Both build
   * passes resolve every from-doc through here, so they agree on what counts as a match by
   * construction. Both maps hold their values offset by one, so that the zero a paged map starts at
   * reads back as "no value".
   */
  private static int resolveToDoc(
      int fromOrd, PagedMutable toOrdPlusOneByFromOrd, PagedMutable toDocPlusOneByToOrd) {
    if (fromOrd == -1) {
      return -1;
    }
    long toOrdPlusOne = toOrdPlusOneByFromOrd.get(fromOrd);
    return toOrdPlusOne == 0 ? -1 : (int) toDocPlusOneByToOrd.get(toOrdPlusOne - 1) - 1;
  }

  /**
   * Reads a pair's persisted {@code {min, max}} edges (or {@code toCount}), all stored on doc 0 of
   * the column -- the read-side counterpart of {@link JoinColumWriter}'s edges columns.
   */
  static int[] loadEdges(LeafReaderContext joinContext, String edgesFieldName) throws IOException {
    SortedNumericDocValues edgesDV = joinContext.reader().getSortedNumericDocValues(edgesFieldName);
    assert edgesDV != null : "expected edges column to be present: " + edgesFieldName;
    int zeroDoc = edgesDV.nextDoc();
    assert zeroDoc == 0 : "expected edges column to be fully materialized, but got doc " + zeroDoc;
    int[] values = new int[edgesDV.docValueCount()];
    for (int i = 0; i < values.length; i++) {
      values[i] = (int) edgesDV.nextValue();
    }
    return values;
  }

  /**
   * The join index field name addressing the ordinal map of one (from-segment, to-segment) pair.
   */
  static String pairFieldName(
      LeafReaderContext fromContext,
      String fromField,
      LeafReaderContext toContext,
      String toField) {
    return getSideKey(fromContext, fromField) + "_" + getSideKey(toContext, toField);
  }

  /**
   * Persistent identifier of one join side: the join field name, the immutable id the segment was
   * created with (it survives reopens, growing deletes mask and reorderings of {@link
   * IndexReader#leaves()}; a merge produces a new segment with a new id) and the docvalues
   * generation of the join field.
   */
  /**
   * Every side key {@code searcher}'s index currently offers for {@code field}: one per leaf, the
   * same strings {@link #getSideKey} builds into a pair field name. A pair whose key is missing
   * from this set names a segment that no longer exists, so its column can never be read again.
   */
  static Set<String> liveSideKeys(IndexSearcher searcher, String field) {
    Set<String> keys = CollectionUtil.newHashSet(searcher.getIndexReader().leaves().size());
    for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
      keys.add(getSideKey(leaf, field));
    }
    return keys;
  }

  /**
   * Splits a pair field name back into the two side keys {@link #pairFieldName} joined, or {@code
   * null} when it isn't one.
   *
   * <p>The separator is found rather than searched for blindly: a side key is {@code
   * field:segmentId:dvGen}, whose field name may itself contain underscores (PRODUCT_ID_FK) but no
   * colon, and whose generation is a number. So the underscore that joins the two keys is the first
   * one after the second colon -- everything between that colon and it is the generation.
   */
  static String[] splitPairFieldName(String pairFieldName) {
    int fieldEnd = pairFieldName.indexOf(':');
    if (fieldEnd < 0) {
      return null;
    }
    int idEnd = pairFieldName.indexOf(':', fieldEnd + 1);
    if (idEnd < 0) {
      return null;
    }
    int separator = pairFieldName.indexOf('_', idEnd + 1);
    if (separator < 0) {
      return null;
    }
    return new String[] {
      pairFieldName.substring(0, separator), pairFieldName.substring(separator + 1)
    };
  }

  /** Whether {@code sideKey} is a key of {@code field}, as opposed to some other join field's. */
  static boolean isKeyOf(String sideKey, String field) {
    return sideKey.length() > field.length()
        && sideKey.charAt(field.length()) == ':'
        && sideKey.startsWith(field);
  }

  static String getSideKey(LeafReaderContext context, String field) {
    byte[] segmentId = segmentReader(context.reader()).getSegmentInfo().info.getId();
    // dvGen starts at -1 and advances only when this particular field receives an in-place
    // IndexWriter.updateDocValues update; deletes only bump delGen and leave it untouched. So the
    // key is insensitive to deletes but changes when the join field's docvalues are updated.
    FieldInfo fieldInfo = context.reader().getFieldInfos().fieldInfo(field);
    long dvGen = fieldInfo == null ? -1 : fieldInfo.getDocValuesGen();
    String key = field + ":" + StringHelper.idToString(segmentId) + ":" + dvGen;
    return key;
  }

  /**
   * Peels wrappers off a leaf reader down to its {@link SegmentReader}. {@link
   * FilterLeafReader#unwrap} alone is not enough: wrappers like {@code
   * SoftDeletesDirectoryReaderWrapper} produce {@link FilterCodecReader} leaves, which are not
   * {@link FilterLeafReader}s, and the two kinds may alternate. Tests additionally wrap readers in
   * a {@link ParallelLeafReader} (e.g. {@code LuceneTestCase#newSearcher}); that class can in
   * general combine several independent readers side by side, but when it wraps exactly one (the
   * common case, including every reader the test framework wraps purely for coverage) that one is
   * unambiguous and safe to descend into.
   */
  static SegmentReader segmentReader(LeafReader reader) {
    while (true) {
      if (reader instanceof SegmentReader segmentReader) {
        return segmentReader;
      } else if (reader instanceof FilterLeafReader filterLeafReader) {
        reader = filterLeafReader.getDelegate();
      } else if (reader instanceof FilterCodecReader filterCodecReader) {
        reader = filterCodecReader.getDelegate();
      } else if (reader instanceof ParallelLeafReader parallelLeafReader) {
        LeafReader[] parallelReaders = parallelLeafReader.getParallelReaders();
        if (parallelReaders.length != 1) {
          throw new IllegalArgumentException(
              "cannot unwrap a SegmentReader from a ParallelLeafReader combining "
                  + parallelReaders.length
                  + " independent readers");
        }
        reader = parallelReaders[0];
      } else {
        throw new IllegalArgumentException(
            "cannot unwrap a SegmentReader from " + reader.getClass().getName());
      }
    }
  }

  /** The name of the sidecar segment carrying the given join index leaf. */
  static String segmentName(LeafReaderContext joinContext) {
    return segmentReader(joinContext.reader()).getSegmentName();
  }

  /**
   * The {@link Directory} backing {@code reader}, unwrapped from any composite that lacks a single
   * directory of its own (e.g. a {@link ParallelCompositeReader}): a {@link DirectoryReader}
   * exposes its directory directly, anything else contributes the first leaf-reader whose directory
   * owns it (descending through {@link ParallelLeafReader} and codec/filter wrappers).
   */
  static Directory directory(IndexReader reader) {
    if (reader instanceof DirectoryReader dr) {
      return dr.directory();
    }
    for (LeafReaderContext leaf : reader.leaves()) {
      return segmentReader(leaf.reader()).getSegmentInfo().info.dir;
    }
    throw new IllegalArgumentException("no directory backing " + reader);
  }

  /**
   * A hashable key identifying the storage location behind {@code directory}, stable across
   * separate opens of the same path so repeated calls resolve to the same cache entry.
   */
  static Object directoryKey(Directory directory) throws IOException {
    Directory unwrapped = FilterDirectory.unwrap(directory);
    if (unwrapped instanceof FSDirectory fsDir) {
      return fsDir.getDirectory().toRealPath(); // Path has proper equals/hashCode
    }
    return unwrapped; // RAMDirectory/ByteBuffersDirectory etc: identity is the real key
  }

  /**
   * Every pair field name (the part after {@link #TO_COUNT_PREFIX}) present in {@code fieldInfos},
   * i.e. every join pair this segment carries, needed or not. Detected by the toCount companion --
   * written for every built pair -- not by the join column itself, which a tombstone pair (disjoint
   * terms) never materializes; the reaper must see tombstones too, or it would judge a segment
   * carrying only tombstones as pair-free and reap it, resurrecting the pairs' rebuilds.
   */
  static Set<String> pairFieldNames(FieldInfos fieldInfos) {
    Set<String> names = new HashSet<>();
    for (FieldInfo fieldInfo : fieldInfos) {
      String[] splits = fieldInfo.name.split(TO_COUNT_PREFIX);
      if (splits.length == 2) {
        names.add(splits[1]);
      }
    }
    return names;
  }

  /**
   * Reads a segment's {@link FieldInfos} straight off disk, without opening a full reader. Mirrors
   * {@code IndexWriter#readFieldInfos}, which isn't visible outside its package.
   */
  static FieldInfos readFieldInfos(SegmentCommitInfo info) throws IOException {
    Codec codec = info.info.getCodec();
    FieldInfosFormat fieldInfosFormat = codec.fieldInfosFormat();
    if (info.hasFieldUpdates()) {
      String segmentSuffix = Long.toString(info.getFieldInfosGen(), Character.MAX_RADIX);
      return fieldInfosFormat.read(info.info.dir, info.info, segmentSuffix, IOContext.READONCE);
    } else if (info.info.getUseCompoundFile()) {
      try (Directory cfs = codec.compoundFormat().getCompoundReader(info.info.dir, info.info)) {
        return fieldInfosFormat.read(cfs, info.info, "", IOContext.READONCE);
      }
    } else {
      return fieldInfosFormat.read(info.info.dir, info.info, "", IOContext.READONCE);
    }
  }

  // copy of org.apache.lucene.search.LRUQueryCache.cacheIntoRoaringDocIdSet
  protected static class CacheAndCount implements Accountable {
    protected static final CacheAndCount EMPTY = new CacheAndCount(DocIdSet.EMPTY, 0);

    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(CacheAndCount.class);
    private final DocIdSet cache;
    private final int count;

    public CacheAndCount(DocIdSet cache, int count) {
      this.cache = cache;
      this.count = count;
    }

    public DocIdSetIterator iterator() throws IOException {
      return cache.iterator();
    }

    // TODO is it really a cost or a count???
    public int count() {
      return count;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + cache.ramBytesUsed();
    }
  }

  static CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc, Bits liveDocs) throws IOException {
    if (scorer.cost() * 100 >= maxDoc) {
      // FixedBitSet is faster for dense sets and will enable the random-access
      // optimization in ConjunctionDISI
      return cacheIntoBitSet(scorer, maxDoc, liveDocs);
    } else {
      return cacheIntoRoaringDocIdSet(scorer, maxDoc, liveDocs);
    }
  }

  private static CacheAndCount cacheIntoBitSet(BulkScorer scorer, int maxDoc, Bits liveDocs)
      throws IOException {
    final FixedBitSet bitSet = new FixedBitSet(maxDoc);
    int[] count = new int[1];
    scorer.score(
        new LeafCollector() {

          private int[] buffer;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            if (liveDocs == null || liveDocs.get(doc)) {
              count[0]++;
              bitSet.set(doc);
            }
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            if (buffer == null) {
              buffer = new int[128];
            }
            for (int c = stream.intoArray(buffer); c != 0; c = stream.intoArray(buffer)) {
              int skip = 0;
              for (int i = 0; i < c; ++i) {
                if (liveDocs != null && !liveDocs.get(buffer[i])) {
                  skip++;
                  continue;
                }
                bitSet.set(buffer[i]);
              }
              count[0] += c - skip;
            }
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    return new CacheAndCount(new BitDocIdSet(bitSet, count[0]), count[0]);
  }

  private static CacheAndCount cacheIntoRoaringDocIdSet(
      BulkScorer scorer, int maxDoc, Bits liveDocs) throws IOException {
    RoaringDocIdSet.Builder builder = new RoaringDocIdSet.Builder(maxDoc);
    scorer.score(
        new LeafCollector() {

          private int[] buffer = null;

          @Override
          public void setScorer(Scorable scorer) throws IOException {}

          @Override
          public void collect(int doc) throws IOException {
            if (liveDocs != null && !liveDocs.get(doc)) {
              return;
            }
            builder.add(doc);
          }

          @Override
          public void collect(DocIdStream stream) throws IOException {
            if (buffer == null) {
              buffer = new int[128];
            }
            for (int c = stream.intoArray(buffer); c != 0; c = stream.intoArray(buffer)) {
              for (int i = 0; i < c; ++i) {
                if (liveDocs != null && !liveDocs.get(buffer[i])) {
                  continue;
                }
                builder.add(buffer[i]);
              }
            }
          }
        },
        null,
        0,
        DocIdSetIterator.NO_MORE_DOCS);
    RoaringDocIdSet cache = builder.build();
    return new CacheAndCount(cache, cache.cardinality());
  }
}
