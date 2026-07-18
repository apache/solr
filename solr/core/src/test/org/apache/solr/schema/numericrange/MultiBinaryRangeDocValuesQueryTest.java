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
package org.apache.solr.schema.numericrange;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.IntRangeDocValuesField;
import org.apache.lucene.document.RangeFieldQuery.QueryType;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.SolrTestCase;
import org.junit.Test;

/**
 * Unit tests for {@link MultiBinaryRangeDocValuesQuery}, the binary docValues-backed range
 * verifier.
 *
 * <p>Each doc indexes its range(s) both as BKD ({@link IntRange}) and as a single {@code
 * BinaryDocValues} blob (all ranges concatenated, as {@code AbstractNumericRangeField} does),
 * letting us assert the docValues verifier returns exactly the same documents as the trusted BKD
 * query across all four relations - for both single-valued docs (one range) and multiValued docs
 * (several ranges in one blob), where "any range matches" (not the union of ranges) is correct.
 */
public class MultiBinaryRangeDocValuesQueryTest extends SolrTestCase {

  private static final String FIELD = "r";

  /** Encodes a 1-D query range the same way indexed ranges are encoded. */
  private static byte[] encode(int min, int max) {
    BytesRef b = new IntRangeDocValuesField(FIELD, new int[] {min}, new int[] {max}).binaryValue();
    return Arrays.copyOfRange(b.bytes, b.offset, b.offset + b.length);
  }

  private static Query dv(QueryType type, int min, int max) {
    return new MultiBinaryRangeDocValuesQuery(FIELD, encode(min, max), 1, IntRange.BYTES, type);
  }

  private static Query bkd(QueryType type, int min, int max) {
    int[] mn = {min};
    int[] mx = {max};
    return switch (type) {
      case INTERSECTS -> IntRange.newIntersectsQuery(FIELD, mn, mx);
      case CONTAINS -> IntRange.newContainsQuery(FIELD, mn, mx);
      case WITHIN -> IntRange.newWithinQuery(FIELD, mn, mx);
      case CROSSES -> IntRange.newCrossesQuery(FIELD, mn, mx);
    };
  }

  /**
   * Adds a document whose (possibly multiple) ranges are indexed as BKD and as one BinaryDocValues
   * blob holding every range concatenated (a single-range doc is just the one-range case).
   */
  private static void addDoc(RandomIndexWriter w, int[]... ranges) throws IOException {
    Document doc = new Document();
    BytesRefBuilder blob = new BytesRefBuilder();
    for (int[] r : ranges) {
      int[] mn = {r[0]};
      int[] mx = {r[1]};
      doc.add(new IntRange(FIELD, mn, mx)); // BKD (ground truth)
      blob.append(new IntRangeDocValuesField(FIELD, mn, mx).binaryValue()); // concat into one blob
    }
    doc.add(new BinaryDocValuesField(FIELD, blob.toBytesRef()));
    w.addDocument(doc);
  }

  private static FixedBitSet matches(IndexSearcher s, Query q) throws IOException {
    int maxDoc = Math.max(1, s.getIndexReader().maxDoc());
    return s.search(
        q,
        new CollectorManager<BitSetCollector, FixedBitSet>() {
          @Override
          public BitSetCollector newCollector() {
            return new BitSetCollector(maxDoc);
          }

          @Override
          public FixedBitSet reduce(Collection<BitSetCollector> collectors) {
            FixedBitSet merged = new FixedBitSet(maxDoc);
            for (BitSetCollector c : collectors) {
              merged.or(c.bits);
            }
            return merged;
          }
        });
  }

  /**
   * Collects matching doc ids (segment-local -&gt; absolute) into a bitset, per CollectorManager.
   */
  private static final class BitSetCollector extends SimpleCollector {
    final FixedBitSet bits;
    int base;

    BitSetCollector(int maxDoc) {
      this.bits = new FixedBitSet(maxDoc);
    }

    @Override
    public void collect(int doc) {
      bits.set(base + doc);
    }

    @Override
    protected void doSetNextReader(LeafReaderContext ctx) {
      base = ctx.docBase;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  @Test
  public void testMultiValuedPerRangeSemantics() throws Exception {
    try (Directory dir = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      addDoc(w, new int[] {1, 10}, new int[] {5, 20}); // doc: two overlapping ranges
      addDoc(w, new int[] {0, 5}, new int[] {100, 200}); // doc: two disjoint ranges
      addDoc(w, new int[] {130, 160}); // doc: single range
      try (IndexReader reader = w.getReader()) {
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        // CONTAINS [1,15]: neither individual range of the first doc contains it; the *union*
        // [1,20] would, but ranges are never unioned -> zero matches.
        assertEquals(0, searcher.count(dv(QueryType.CONTAINS, 1, 15)));
        assertEquals(0, searcher.count(bkd(QueryType.CONTAINS, 1, 15)));

        // CONTAINS [3,150]: the second doc's [0,5] and [100,200] each satisfy one half-open bound,
        // so a decomposition would falsely match -- native per-range matching must not.
        assertEquals(0, searcher.count(dv(QueryType.CONTAINS, 3, 150)));

        // CONTAINS [1,10]: first doc's [1,10] contains it (any range suffices).
        assertEquals(1, searcher.count(dv(QueryType.CONTAINS, 1, 10)));

        // INTERSECTS [12,14]: first doc's [5,20] overlaps it.
        assertEquals(1, searcher.count(dv(QueryType.INTERSECTS, 12, 14)));

        // WITHIN [0,25]: first doc ([1,10]) and second doc ([0,5]) each have a range within it.
        assertEquals(2, searcher.count(dv(QueryType.WITHIN, 0, 25)));

        // Every case must agree with the BKD ground truth.
        for (QueryType t : QueryType.values()) {
          for (int[] q : new int[][] {{1, 15}, {3, 150}, {1, 10}, {12, 14}, {0, 25}}) {
            assertEquals(
                "relation " + t + " query [" + q[0] + " TO " + q[1] + "]",
                matches(searcher, bkd(t, q[0], q[1])),
                matches(searcher, dv(t, q[0], q[1])));
          }
        }
      }
    }
  }

  @Test
  public void testParityWithBkdRandom() throws Exception {
    try (Directory dir = newDirectory()) {
      RandomIndexWriter w = new RandomIndexWriter(random(), dir);
      int numDocs = atLeast(200);
      for (int i = 0; i < numDocs; i++) {
        int numRanges = TestUtil.nextInt(random(), 1, 3); // single- and multi-valued docs
        int[][] ranges = new int[numRanges][2];
        for (int r = 0; r < numRanges; r++) {
          int a = TestUtil.nextInt(random(), -50, 50);
          int b = TestUtil.nextInt(random(), a, 50); // a <= b
          ranges[r] = new int[] {a, b};
        }
        addDoc(w, ranges);
      }
      try (IndexReader reader = w.getReader()) {
        w.close();
        IndexSearcher searcher = newSearcher(reader);

        for (int iter = 0; iter < 200; iter++) {
          int qa = TestUtil.nextInt(random(), -55, 55);
          int qb = TestUtil.nextInt(random(), qa, 55);
          for (QueryType t : QueryType.values()) {
            assertEquals(
                "relation " + t + " query [" + qa + " TO " + qb + "]",
                matches(searcher, bkd(t, qa, qb)),
                matches(searcher, dv(t, qa, qb)));
          }
        }
      }
    }
  }
}
