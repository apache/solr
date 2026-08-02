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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilteredDocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

/**
 * Column-building and addressing helpers for the auxiliary join index managed by {@link
 * AIJoinIndex}: for every (from-segment, to-segment) pair it produces a SORTED_NUMERIC column named
 * {@link #pairFieldName}, whose doc number is the from-side doc id and whose value is the to-side
 * doc id whose {@code toField} term equals the from doc's {@code fromField} term, plus two
 * companion edges columns persisting the pair's {min, max} from-doc and to-doc bounds.
 */
final class AIJoinUtil {

  /** Suffix of the always-written column persisting a pair's {min, max} from-doc edges. */
  static final String FROM_EDGES_PREFIX = "fromDoc_edges_"; // TODO reduce to the singe letter

  /** Suffix of the always-written column persisting a pair's {min, max} to-doc edges. */
  static final String TO_EDGES_PREFIX = "toDoc_edges_";

  /** main join colums for join index to_doc_num[from_docnum] */
  static final String TO_DOC_VAL_BY_FROM_DOCNUM = "join_toDoc_";

  static final String TO_COUNT_PREFIX = "num_toDoc_";

  private AIJoinUtil() {}

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
   * The from-doc-to-to-doc map produced by {@link #computeDocMapping}, paired with its resolved
   * {@link #edges()}. {@link #toDocByFromDoc()} mirrors the on-disk column's read API, so freshly
   * built pairs (not yet flushed to the join index) and pairs loaded from the join index can be
   * walked by the same code.
   */
  static final class JoinColumnModel {
    private final int[] toDocByFromDoc;
    private final DocEdges edges;

    JoinColumnModel(int[] toDocByFromDoc, DocEdges edges) {
      this.toDocByFromDoc = toDocByFromDoc;
      this.edges = edges;
    }

    /**
     * Returns a fresh single-valued cursor over the from-doc -> to-doc map, positioned before doc
     * 0.
     */
    SortedNumericDocValues toDocByFromDoc() {
      return new ArrayBackedSortedNumericDocValues(toDocByFromDoc);
    }

    DocEdges edges() {
      return edges;
    }
  }

  /**
   * Adapts an int-array from-doc -> to-doc map (as produced by {@link #computeDocMapping}, {@code
   * -1} meaning no value) to the {@link SortedNumericDocValues} read API, so it can be consumed the
   * same way as the on-disk join column. Always single-valued until M:N pairs are supported.
   */
  private static final class ArrayBackedSortedNumericDocValues extends SortedNumericDocValues {
    private final int[] toDocByFromDoc;
    private int doc = -1;

    ArrayBackedSortedNumericDocValues(int[] toDocByFromDoc) {
      this.toDocByFromDoc = toDocByFromDoc;
    }

    @Override
    public long nextValue() {
      return toDocByFromDoc[doc];
    }

    @Override
    public int docValueCount() {
      return 1;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      return target < toDocByFromDoc.length && toDocByFromDoc[target] >= 0;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) {
      while (target < toDocByFromDoc.length && toDocByFromDoc[target] < 0) {
        target++;
      }
      doc = target < toDocByFromDoc.length ? target : NO_MORE_DOCS;
      return doc;
    }

    @Override
    public long cost() {
      return toDocByFromDoc.length;
    }
  }

  /**
   * Merges the sorted term dictionaries of one (from-segment, to-segment) pair and resolves every
   * from-side doc to its matching to-side doc id, along with the pair's from-doc and to-doc bounds.
   * {@code scratch} is a shared from-ord indexed merge buffer, safe to reuse for the next pair.
   *
   * <p>Docs already deleted at build time are skipped, purely to avoid persisting entries nobody
   * can ever match -- deletes are otherwise re-checked live at query time (from-side in {@code
   * ToLeafJoinContext}, to-side by the searcher's own {@code acceptDocs}), since a pair's cached
   * mapping outlives whatever gets deleted after it was built.
   */
  static JoinColumnModel computeDocMapping(
      LeafReaderContext fromContext,
      String fromField,
      LeafReaderContext toContext,
      String toField,
      long[] scratch)
      throws IOException {
    SortedSetDocValues fromDV = DocValues.getSortedSet(fromContext.reader(), fromField);
    SortedSetDocValues toDV = DocValues.getSortedSet(toContext.reader(), toField);
    Bits fromLiveDocs = fromContext.reader().getLiveDocs();
    Bits toLiveDocs = toContext.reader().getLiveDocs();
    // map from-segment ords to to-segment ords by merging the two sorted term dictionaries
    // TODO this merge is per pair, so a from segment's term dictionary is walked once for every
    // to segment it pairs with, and vice versa: N*M merges where 2*(N+M) dictionary reads would
    // do, if ord maps were derived per segment and reused across pairings. Worth measuring before
    // parallelising the caller (AIJoinIndex#writeJoinSegments): removing the redundancy may buy
    // more than spreading it across threads.
    long[] toOrdByFromOrd = scratch;
    Arrays.fill(toOrdByFromOrd, -1L);
    // dead code, kept until M:N support settles: the reverse ord map was filled but never read
    // long[] fromOrdByToOrd = new long[Math.toIntExact(toDV.getValueCount())];
    // Arrays.fill(fromOrdByToOrd, -1L);
    TermsEnum fromTerms = fromDV.termsEnum();
    TermsEnum toTerms = toDV.termsEnum();
    BytesRef fromTerm = fromTerms.next();
    BytesRef toTerm = toTerms.next();
    while (fromTerm != null && toTerm != null) {
      int cmp = fromTerm.compareTo(toTerm);
      if (cmp == 0) {
        toOrdByFromOrd[(int) fromTerms.ord()] = toTerms.ord();
        // fromOrdByToOrd[(int) toTerms.ord()] = fromTerms.ord();
        fromTerm = fromTerms.next();
        toTerm = toTerms.next();
      } else if (cmp < 0) {
        fromTerm = fromTerms.next();
      } else {
        toTerm = toTerms.next();
      }
    }
    // TODO: this degrades M:N joins to M:1. Both toDocByToOrd and toDocByFromDoc keep a single
    // to-side doc per slot, so when several to docs share a term (non-unique toField) or a from
    // doc is multi-valued with several matching terms, later assignments overwrite earlier ones
    // and only the last match survives. The read side (AIJoinQuery) already consumes all
    // docValueCount() values per doc, so only this writer needs to learn to emit multiple
    // to docs per from doc.
    int[] toDocByToOrd = new int[Math.toIntExact(toDV.getValueCount())];
    Arrays.fill(toDocByToOrd, -1);
    for (int toDoc = toDV.nextDoc();
        toDoc != DocIdSetIterator.NO_MORE_DOCS;
        toDoc = toDV.nextDoc()) {
      if (toLiveDocs != null && !toLiveDocs.get(toDoc)) {
        continue;
      }
      for (int i = 0; i < toDV.docValueCount(); i++) {
        long toOrd = toDV.nextOrd();
        toDocByToOrd[(int) toOrd] = toDoc;
      }
    }

    // resolve every from doc to its to-side ordinal: the doc's fromField ord looked up in the
    // dictionary merge result. Docs without the field, or whose term has no to-side match, keep -1
    int[] toDocByFromDoc = new int[fromContext.reader().maxDoc()];
    Arrays.fill(toDocByFromDoc, -1);
    int minFromDoc = DocIdSetIterator.NO_MORE_DOCS;
    int maxFromDoc = -1;
    int minToDoc = DocIdSetIterator.NO_MORE_DOCS;
    int maxToDoc = -1;
    int toCount = 0;
    for (int fromDoc = fromDV.nextDoc();
        fromDoc != DocIdSetIterator.NO_MORE_DOCS;
        fromDoc = fromDV.nextDoc()) {
      if (fromLiveDocs != null && !fromLiveDocs.get(fromDoc)) {
        continue;
      }
      for (int i = 0; i < fromDV.docValueCount(); i++) {
        long fromOrd = fromDV.nextOrd();
        int toOrd = (int) toOrdByFromOrd[(int) fromOrd];
        if (toOrd == -1) {
          continue;
        }
        int toDoc = toDocByToOrd[toOrd];
        if (toDoc == -1) {
          continue;
        }
        toDocByFromDoc[fromDoc] = toDoc;
        minFromDoc = Math.min(minFromDoc, fromDoc);
        maxFromDoc = Math.max(maxFromDoc, fromDoc);
        minToDoc = Math.min(minToDoc, toDoc);
        maxToDoc = Math.max(maxToDoc, toDoc);
        toCount++;
      }
    }
    if (maxFromDoc < 0) {
      // no from doc in this pair maps to any to doc: normalize both edges to the symmetric
      // {-1, -1} sentinel. An asymmetric one (e.g. {NO_MORE_DOCS, -1}) doesn't round-trip
      // through the join index's SORTED_NUMERIC edges column, which always returns its two
      // values in ascending numeric order regardless of which was written as "min" -- so
      // {NO_MORE_DOCS, -1} silently comes back as {-1, NO_MORE_DOCS} on the next read.
      minFromDoc = -1;
      minToDoc = -1;
      maxToDoc = -1;
    }
    return new JoinColumnModel(
        toDocByFromDoc,
        new Edges(new int[] {minFromDoc, maxFromDoc}, new int[] {minToDoc, maxToDoc}, toCount));
  }

  /**
   * A from-segment's matches against the cached from-side weight: {@link #iterator()} is a fresh,
   * live-doc-filtered {@link DocIdSetIterator} positioned before doc 0, and {@link #cost()} is the
   * underlying {@link ScorerSupplier}'s cost, captured before the iterator was created.
   */
  record MatchingFromDocs(DocIdSetIterator iterator, long cost) {}

  /**
   * Resolves {@code fromContext} against {@code cachedFromWeight}, filtering out deleted docs, or
   * returns {@code null} if the segment has no live match at all. Shared by {@link
   * ToLeafJoinContext#createFromItersTasks}, which walks the returned iterator once per to-segment,
   * and by {@link AIJoinQuery#createWeight}, which only needs to know whether the segment matches
   * anything.
   */
  static MatchingFromDocs matchingFromDocs(Weight cachedFromWeight, LeafReaderContext fromContext)
      throws IOException {
    ScorerSupplier fromSupplier = cachedFromWeight.scorerSupplier(fromContext);
    if (fromSupplier == null) {
      return null;
    }
    long fromMatchCost = fromSupplier.cost();
    Scorer fromScorer = fromSupplier.get(Long.MAX_VALUE);
    DocIdSetIterator matchedFromDocs = fromScorer.iterator();
    Bits liveDocs = fromContext.reader().getLiveDocs();
    if (liveDocs != null) {
      // the cached weight's scorer doesn't filter deletions itself, and a from doc deleted
      // since the pair columns were built (e.g. by an update) must not resolve to a match
      matchedFromDocs =
          new FilteredDocIdSetIterator(matchedFromDocs) {
            @Override
            protected boolean match(int doc) {
              return liveDocs.get(doc);
            }
          };
    }
    return new MatchingFromDocs(matchedFromDocs, fromMatchCost);
  }

  /**
   * The from-side keys ({@link #getSideKey}) of every from-segment with at least one live doc
   * matching {@code fromQuery} -- i.e. every from-segment that could possibly contribute a pair to
   * any to-segment. Used at {@link AIJoinQuery#createWeight} time to narrow which pair columns are
   * worth looking up in the join index, without yet knowing the to-side searcher's leaves.
   */
  static Set<String> matchingFromSideKeys(
      IndexSearcher cachedFromSearcher, Query fromQuery, String fromField) throws IOException {
    Weight fromWeight = cachedFromSearcher.createWeight(fromQuery, ScoreMode.COMPLETE_NO_SCORES, 1);
    Set<String> keys = new HashSet<>();
    for (LeafReaderContext fromContext : cachedFromSearcher.getLeafContexts()) {
      MatchingFromDocs matching = matchingFromDocs(fromWeight, fromContext);
      if (matching != null && matching.iterator().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        keys.add(getSideKey(fromContext, fromField));
      }
    }
    return keys;
  }

  /**
   * Reads a pair's persisted {@code {min, max}} edges (or {@code toCount}), all stored on doc 0 of
   * the column -- the read-side counterpart of {@link AIJoinWriter}'s edges columns.
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

  // Lucene puts no hard constraints on field names, but conservatively keep side keys usable as
  // one by reducing them to identifier characters
  private static final Pattern NON_IDENTIFIER = Pattern.compile("[^A-Za-z0-9_]");

  /**
   * Persistent identifier of one join side: the join field name, the immutable id the segment was
   * created with (it survives reopens, growing deletes mask and reorderings of {@link
   * IndexReader#leaves()}; a merge produces a new segment with a new id) and the docvalues
   * generation of the join field.
   */
  static String getSideKey(LeafReaderContext context, String field) {
    byte[] segmentId = segmentReader(context.reader()).getSegmentInfo().info.getId();
    // dvGen starts at -1 and advances only when this particular field receives an in-place
    // IndexWriter.updateDocValues update; deletes only bump delGen and leave it untouched. So the
    // key is insensitive to deletes but changes when the join field's docvalues are updated.
    long dvGen = context.reader().getFieldInfos().fieldInfo(field).getDocValuesGen();
    String key = field + ":" + StringHelper.idToString(segmentId) + ":" + dvGen;
    // TODO this is dangerous, no one flip them back
    return NON_IDENTIFIER.matcher(key).replaceAll("_");
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
   * Every pair field name (the part after {@link #TO_DOC_VAL_BY_FROM_DOCNUM}) present in {@code
   * fieldInfos}, i.e. every join pair this segment carries, needed or not.
   */
  static Set<String> pairFieldNames(FieldInfos fieldInfos) {
    Set<String> names = new HashSet<>();
    for (FieldInfo fieldInfo : fieldInfos) {
      String[] splits = fieldInfo.name.split(TO_DOC_VAL_BY_FROM_DOCNUM);
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
}
