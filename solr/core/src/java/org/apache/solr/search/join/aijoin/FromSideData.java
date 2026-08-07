package org.apache.solr.search.join.aijoin;

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;

/**
 * From-side state of one (from-segment, to-segment) pair: the from doc values, its live-docs mask, and the hashed from-side term
 * dictionary. The hash maps each from-side term to its from-ord via {@link #fromOrdByHashOrd}, so the to-side stage of
 * {@link AIJoinUtil#computeDocMapping} can resolve a to-side term to the from-ord it shares. {@link #toDocByFromDoc} is zero-filled to
 * {@code -1} and indexed by each live from-doc's from-ord by ; the to-side stage rewrites it in place from
 * from-ords to to-docs.
 */
final class FromSideData {
  private final BytesRefHash fromTermsHash;
  private final int[] fromOrdByHashOrd;
  private final int[] toDocByFromDoc;
  private final int fromValuesCount;

  public FromSideData(LeafReaderContext fromContext, String fromField
  ) throws IOException {

    SortedSetDocValues fromDV = DocValues.getSortedSet(fromContext.reader(), fromField);
    Bits fromLiveDocs = fromContext.reader().getLiveDocs();
    // dead code, kept until M:N support settles: the reverse (to-side sized) ord map was filled
    // but never read
    // long[] fromOrdByToOrd = new long[Math.toIntExact(toDV.getValueCount())];
    // Arrays.fill(fromOrdByToOrd, -1L);
    TermsEnum fromTerms = fromDV.termsEnum();
    BytesRefHash fromTermsHash = new BytesRefHash();
    int[] fromOrdByHashOrd = new int[Math.toIntExact(fromDV.getValueCount())];
    for (BytesRef term = fromTerms.next(); term != null; term = fromTerms.next()) {
      fromOrdByHashOrd[fromTermsHash.add(term)] = (int) fromTerms.ord();
    }
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
   * per thread to-segment tasks uses these copies as a scratch, to turn values to "toDoc#"
   *
   */
  public int[] cloneFromOrdByFromDoc() {
    return toDocByFromDoc.clone();
  }

  public int getFromValuesCount() {
    return fromValuesCount;
  }
}
