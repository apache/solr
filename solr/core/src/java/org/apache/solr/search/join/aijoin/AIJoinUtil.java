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
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.RoaringDocIdSet;
import org.apache.lucene.util.StringHelper;
import org.slf4j.Logger;
import org.slf4j.event.Level;

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
   * Configurable level for the {@code AIJOIN evt=...} diagnostic logs; defaults to {@code INFO},
   * override with the {@code solr.aijoin.log.level} system property (or {@code
   * SOLR_AIJOIN_LOG_LEVEL} env var).
   */
  static final Level AIJOIN_LOG_LEVEL = Level.TRACE;

  /** Whether the AIJOIN diagnostic logs would emit at the configured level. */
  static boolean diagnosticsEnabled(Logger log) {
    return log.isEnabledForLevel(AIJOIN_LOG_LEVEL);
  }

  /** Emits an AIJOIN diagnostic line at the configured level. */
  static void logDiagnostic(Logger log, String message, Object... args) {
    log.atLevel(AIJOIN_LOG_LEVEL).log(message, args);
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
   * Builds the join column for one (from-segment, to-segment) pair: resolves every from-side doc to
   * its matching to-side doc id, along with the pair's from-doc and to-doc bounds. From-side terms
   * are hashed by {@link FromSideData}; each to-side term is looked up in that hash to map
   * from-side ords to to-side ords.
   *
   * <p>Docs already deleted at build time are skipped, purely to avoid persisting entries nobody
   * can ever match -- deletes are otherwise re-checked live at query time (from-side in {@code
   * ToLeafJoinContext}, to-side by the searcher's own {@code acceptDocs}), since a pair's cached
   * mapping outlives whatever gets deleted after it was built.
   */
  static JoinColumnModel computeDocMapping(
      LeafReaderContext toContext, String toField, FromSideData fromSideData
      //    , long[] scratch
      ) throws IOException {

    long[] toOrdByFromOrd = new long[fromSideData.getFromValuesCount()];
    Arrays.fill(toOrdByFromOrd, -1L);
    SortedSetDocValues toDV = DocValues.getSortedSet(toContext.reader(), toField);
    Bits toLiveDocs = toContext.reader().getLiveDocs();
    TermsEnum toTerms = toDV.termsEnum();
    // resolve from-side ords to to-side ords: look each to-side term up in the from-side hash.
    for (BytesRef term = toTerms.next(); term != null; term = toTerms.next()) {
      int fromOrd = fromSideData.getFromTermOrdOrDashOne(term);
      if (fromOrd != -1) {
        toOrdByFromOrd[fromOrd] = (int) toTerms.ord();
      }
    }
    // TODO: this degrades M:N joins to M:1. Both toDocByToOrd and toDocByFromDoc keep a single
    // to-side doc per slot, so when several to docs share a term (non-unique toField) or a
    // fromSideData
    // doc is multi-valued with several matching terms, later assignments overwrite earlier ones
    // and only the last match survives. The read side (AIJoinQuery) already consumes all
    // docValueCount() values per doc, so only this writer needs to learn to emit multiple
    // to docs per fromSideData doc.
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
        // TODO we can apply toOrdByFromOrd right here
        // and get toDocByFromOrd[]
      }
    }

    // resolve every fromSideData doc to its to-side doc. Docs without the field, or whose term has
    // no to-side match, keep -1.
    int[] toDocByFromDoc = fromSideData.cloneFromOrdByFromDoc();
    int minFromDoc = DocIdSetIterator.NO_MORE_DOCS;
    int maxFromDoc = -1;
    int minToDoc = DocIdSetIterator.NO_MORE_DOCS;
    int maxToDoc = -1;
    int toCount = 0;
    // walk the array, mapping each fromSideData ord to its to-side doc in place.
    for (int fromDoc = 0; fromDoc < toDocByFromDoc.length; fromDoc++) {
      int fromOrd = toDocByFromDoc[fromDoc];
      if (fromOrd == -1) {
        continue;
      }
      int toOrd = (int) toOrdByFromOrd[fromOrd];
      int toDoc = toOrd == -1 ? -1 : toDocByToOrd[toOrd];
      if (toDoc == -1) {
        toDocByFromDoc[fromDoc] = -1; // wiping is crucial
        continue;
      }
      toDocByFromDoc[fromDoc] = toDoc;
      minFromDoc = Math.min(minFromDoc, fromDoc);
      maxFromDoc = Math.max(maxFromDoc, fromDoc);
      minToDoc = Math.min(minToDoc, toDoc);
      maxToDoc = Math.max(maxToDoc, toDoc);
      toCount++;
    }
    if (maxFromDoc < 0) {
      // no fromSideData doc in this pair maps to any to doc: normalize both edges to the symmetric
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

    public int count() {
      return count;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + cache.ramBytesUsed();
    }
  }

  protected static CacheAndCount cacheImpl(BulkScorer scorer, int maxDoc, Bits liveDocs)
      throws IOException {
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
