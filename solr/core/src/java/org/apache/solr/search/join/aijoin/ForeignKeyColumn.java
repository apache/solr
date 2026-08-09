package org.apache.solr.search.join.aijoin;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * From-side state of one (from-segment, to-segment) pair: the from doc values, its live-docs mask,
 * and the hashed from-side term dictionary. The hash maps each from-side term to its from-ord via
 * {@link #fromOrdByHashOrd}, so the to-side stage of {@link AIJoinUtil#computeDocMapping} can
 * resolve a to-side term to the from-ord it shares. {@link #toDocByFromDoc} is zero-filled to
 * {@code -1} and indexed by each live from-doc's from-ord by ; the to-side stage rewrites it in
 * place from from-ords to to-docs.
 */
final class ForeignKeyColumn {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final BytesRefHash fromTermsHash;
  private final int[] fromOrdByHashOrd;
  private final int[] toDocByFromDoc;
  private final int fromValuesCount;

  public ForeignKeyColumn(LeafReaderContext fromContext, String fromField) throws IOException {
    long startNanos = System.nanoTime();
    SortedSetDocValues fromDV = DocValues.getSortedSet(fromContext.reader(), fromField);
    Bits fromLiveDocs = fromContext.reader().getLiveDocs();
    // dead code, kept until M:N support settles: the reverse (to-side sized) ord map was filled
    // but never read
    // long[] fromOrdByToOrd = new long[Math.toIntExact(toDV.getValueCount())];
    // Arrays.fill(fromOrdByToOrd, -1L);
    TermsEnum fromTerms = fromDV.termsEnum();
    int fkVals = Math.toIntExact(fromDV.getValueCount());
    // BytesRefHash only rehashes once it is exactly half full (count == hashHalfSize), so a table
    // seeded at size 1 (fkVals == 1) never rehashes and ends up full; a probe for an absent term
    // then never finds an empty slot and loops forever. Seeding at least 2 lets that rehash kick
    // in.
    int capacity =
        Math.max(
            2,
            BitUtil.nextHighestPowerOfTwo(
                fkVals * 2 + 2 // don't want to rehash
                ));
    // pool,
    BytesRefHash fromTermsHash =
        new BytesRefHash(
            new ByteBlockPool(new ByteBlockPool.DirectAllocator()),
            capacity,
            new BytesRefHash.DirectBytesStartArray(capacity));
    int[] fromOrdByHashOrd = new int[capacity]; // should we fill it with -1 ? - probably shouldn't
    for (BytesRef term = fromTerms.next(); term != null; term = fromTerms.next()) {
      fromOrdByHashOrd[fromTermsHash.add(term)] = (int) fromTerms.ord();
    }
    // should we shrink it then? idk
    int[] toDocByFromDoc = new int[fromContext.reader().maxDoc()];
    Arrays.fill(toDocByFromDoc, -1);
    for (int fromDoc = fromDV.nextDoc();
        fromDoc != DocIdSetIterator.NO_MORE_DOCS;
        fromDoc = fromDV.nextDoc()) {
      if (fromLiveDocs != null && !fromLiveDocs.get(fromDoc)) {
        continue;
      }
      toDocByFromDoc[fromDoc] = (int) fromDV.nextOrd();
    }

    this.fromTermsHash = fromTermsHash;
    this.fromOrdByHashOrd = fromOrdByHashOrd;
    this.toDocByFromDoc = toDocByFromDoc;
    this.fromValuesCount = Math.toIntExact(fromDV.getValueCount());
    if (AIJoinUtil.diagnosticsEnabled(log)) {
      // this constructor is the heavy from-side work (hashes the whole term dictionary), so every
      // line here is one profiler-visible FK load; a segment recurring across queries means its
      // pairs never get persisted and the load is being repeated in vain
      AIJoinUtil.logDiagnostic(
          log,
          "AIJOIN evt=fkload fromSeg={} field={} ord={} maxDoc={} values={} tookUs={}",
          AIJoinUtil.segmentName(fromContext),
          fromField,
          fromContext.ord,
          fromContext.reader().maxDoc(),
          fromValuesCount,
          (System.nanoTime() - startNanos) / 1_000L);
    }
  }

  public int getFromTermOrdOrDashOne(BytesRef value) {
    int hashOrd = this.fromTermsHash.find(value);
    if (hashOrd != -1) {
      return this.fromOrdByHashOrd[hashOrd];
    } else {
      return -1;
    }
  }

  /**
   * per thread to-segment tasks uses these copies as a scratch, to turn values to "toDoc#" TODO
   * presumably, we know how many copies we need, and the last call might return the original array
   * which will be overridden by transformation.
   */
  public int[] cloneFromOrdByFromDoc() {
    return toDocByFromDoc.clone();
  }

  public int fromSideMaxDocs() {
    return toDocByFromDoc.length;
  }

  public int getFromValuesCount() {
    return fromValuesCount;
  }
}
