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
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.apache.lucene.util.packed.PagedMutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * From-side state of one (from-segment, to-segment) pair: the from doc values, its live-docs mask,
 * and the hashed from-side term dictionary. The hash maps each from-side term to its from-ord via
 * {@link #getFromTermOrdOrDashOne}, so the to-side stage of {@link
 * JoinIndexUtils#computeDocMapping} can resolve a to-side term to the from-ord it shares. {@link
 * #fromOrds()} walks each live from-doc's from-ord, which that stage reads to resolve from-ords to
 * to-docs, into a column of its own.
 *
 * <p>One of these is loaded per from-segment and shared by every to-segment paired with it, but
 * that is only within a query: a pair that never reaches the join index has its from side loaded
 * again on the next query, and a production run was doing about eighty of these loads a minute. So
 * both maps here are paged the way {@link JoinIndexUtils.JoinColumnModel} is -- a from-segment of
 * five million docs held the from-ord map alone as a 20MB {@code int[]}, which G1 can only place as
 * a run of humongous regions, and eighty of those a minute is what a 2GB heap died of. Packing each
 * map at the bits its values need takes the same map to about 14MB, in pages no bigger than 128KB.
 *
 * @lucene.experimental
 */
final class ForeignKeyColumn {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final BytesRefHash fromTermsHash;

  /** The from-ord of the term at each of {@link #fromTermsHash}'s hash ords. */
  private final PagedMutable fromOrdByHashOrd;

  /**
   * {@code fromOrd + 1} at each from-doc, {@code 0} where the doc is deleted or has no value,
   * running to the last from-doc that has one and no further.
   */
  private final PackedLongValues fromOrdPlusOneByFromDoc;

  private final int fromSideMaxDocs;
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
    // no sentinel needed: this is only ever read at a hash ord the hash itself handed back, i.e.
    // one that was written here, so the zero a fresh page starts at is never mistaken for a value
    PagedMutable fromOrdByHashOrd =
        new PagedMutable(
            capacity,
            JoinIndexUtils.PAGE_SIZE,
            PackedInts.bitsRequired(fkVals),
            PackedInts.COMPACT);
    for (BytesRef term = fromTerms.next(); term != null; term = fromTerms.next()) {
      fromOrdByHashOrd.set(fromTermsHash.add(term), fromTerms.ord());
    }
    // from-docs arrive ascending, so the map is appended rather than addressed: each doc's
    // from-ord is offset by one so that the zero a skipped doc leaves behind reads back as "no
    // value", and the tail past the last valued doc is never laid down at all
    PackedLongValues.Builder fromOrdPlusOneByFromDoc =
        PackedLongValues.packedBuilder(JoinIndexUtils.PAGE_SIZE, PackedInts.COMPACT);
    int laidDown = 0;
    for (int fromDoc = fromDV.nextDoc();
        fromDoc != DocIdSetIterator.NO_MORE_DOCS;
        fromDoc = fromDV.nextDoc()) {
      if (fromLiveDocs != null && !fromLiveDocs.get(fromDoc)) {
        continue;
      }
      while (laidDown < fromDoc) {
        fromOrdPlusOneByFromDoc.add(0L);
        laidDown++;
      }
      fromOrdPlusOneByFromDoc.add(fromDV.nextOrd() + 1);
      laidDown++;
    }

    this.fromTermsHash = fromTermsHash;
    this.fromOrdByHashOrd = fromOrdByHashOrd;
    this.fromOrdPlusOneByFromDoc = fromOrdPlusOneByFromDoc.build();
    this.fromSideMaxDocs = fromContext.reader().maxDoc();
    this.fromValuesCount = Math.toIntExact(fromDV.getValueCount());
    if (JoinIndexUtils.diagnosticsEnabled(log)) {
      // this constructor is the heavy from-side work (hashes the whole term dictionary), so every
      // line here is one profiler-visible FK load; a segment recurring across queries means its
      // pairs never get persisted and the load is being repeated in vain
      JoinIndexUtils.logDiagnostic(
          log,
          "AUXIJOIN evt=fkload fromSeg={} field={} ord={} maxDoc={} values={} pagedBytes={}"
              + " tookUs={}",
          JoinIndexUtils.segmentName(fromContext),
          fromField,
          fromContext.ord,
          fromContext.reader().maxDoc(),
          fromValuesCount,
          pagedBytesUsed(),
          (System.nanoTime() - startNanos) / 1_000L);
    }
  }

  public int getFromTermOrdOrDashOne(BytesRef value) {
    int hashOrd = this.fromTermsHash.find(value);
    if (hashOrd != -1) {
      return (int) this.fromOrdByHashOrd.get(hashOrd);
    } else {
      return -1;
    }
  }

  /**
   * A fresh walk over this from-segment's from-docs, from doc 0. One instance of this class serves
   * every to-segment paired with this from-segment, and {@link JoinIndexUtils#computeDocMapping}
   * walks it twice per pair -- once to measure, once to lay out -- so the map is read through
   * cursors rather than copied: it used to be cloned per pair and rewritten in place, which cost a
   * full {@code int[maxDoc]} per pair (20MB on a five-million-doc from-segment) regardless of how
   * little the pair matched.
   */
  public FromOrds fromOrds() {
    return new FromOrds(fromOrdPlusOneByFromDoc);
  }

  /**
   * A forward-only walk over from-docs and the from-ord each carries, in doc order from doc 0. The
   * walk ends at the last from-doc with a value: nothing past it has one, and neither of {@link
   * JoinIndexUtils#computeDocMapping}'s passes has anything to do at a doc without one.
   */
  static final class FromOrds {
    private final PackedLongValues.Iterator fromOrdsPlusOne;
    private int fromDoc = -1;
    private int fromOrd = -1;

    FromOrds(PackedLongValues fromOrdPlusOneByFromDoc) {
      this.fromOrdsPlusOne = fromOrdPlusOneByFromDoc.iterator();
    }

    /** Advances to the next from-doc, or returns false once the walk is spent. */
    boolean next() {
      if (!fromOrdsPlusOne.hasNext()) {
        return false;
      }
      fromDoc++;
      fromOrd = (int) fromOrdsPlusOne.next() - 1;
      return true;
    }

    /** The from-doc the walk stands on. */
    int fromDoc() {
      return fromDoc;
    }

    /** Its from-ord, or {@code -1} where the doc is deleted or carries no value. */
    int fromOrd() {
      return fromOrd;
    }
  }

  public int fromSideMaxDocs() {
    return fromSideMaxDocs;
  }

  /**
   * What the two paged maps cost on the heap. Excludes the term hash, whose arrays are {@link
   * BytesRefHash}'s own and still flat.
   */
  public long pagedBytesUsed() {
    return fromOrdByHashOrd.ramBytesUsed() + fromOrdPlusOneByFromDoc.ramBytesUsed();
  }

  public int getFromValuesCount() {
    return fromValuesCount;
  }
}
