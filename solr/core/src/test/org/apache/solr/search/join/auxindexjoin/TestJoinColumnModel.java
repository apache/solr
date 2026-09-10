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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.packed.PackedLongValues;
import org.apache.solr.SolrTestCase;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.Edges;
import org.apache.solr.search.join.auxindexjoin.JoinIndexUtils.JoinColumnModel;

/**
 * The two {@link JoinColumnModel} layouts have to be indistinguishable through the read API, since
 * which one a pair got is decided by its density at build time and neither consumer knows or cares.
 * These tests hold the same from-doc -> to-doc map in both and assert the cursors agree, under the
 * two access patterns the consumers actually use: {@code JoinColumnDocWriter} walking {@code
 * nextDoc()} up the batch, and {@code JoinIndexScorerSupplier.LeafJoin} calling {@code
 * advanceExact} along a forward-only from-doc iterator.
 */
public class TestJoinColumnModel extends SolrTestCase {

  public void testSparseAndDenseWalkAlike() throws IOException {
    for (int iter = 0; iter < 200; iter++) {
      int maxDoc = TestUtil.nextInt(random(), 1, 500);
      int[] toDocByFromDoc = randomMap(maxDoc, random().nextFloat());
      assertWalksAlike(toDocByFromDoc);
      assertSeeksAlike(toDocByFromDoc);
    }
  }

  /** The layouts have to agree at the extremes too: nothing matched, and everything matched. */
  public void testDegenerateColumns() throws IOException {
    int maxDoc = TestUtil.nextInt(random(), 1, 100);
    int[] empty = new int[maxDoc];
    Arrays.fill(empty, -1);
    assertWalksAlike(empty);
    assertSeeksAlike(empty);

    int[] full = new int[maxDoc];
    for (int fromDoc = 0; fromDoc < maxDoc; fromDoc++) {
      full[fromDoc] = maxDoc - 1 - fromDoc;
    }
    assertWalksAlike(full);
    assertSeeksAlike(full);
  }

  /** A tombstone costs no per-doc storage, but still stands for its whole from-segment. */
  public void testTombstoneHoldsNothingButItsWidth() throws IOException {
    JoinColumnModel tombstone = JoinColumnModel.tombstone(5_000_000);
    assertEquals(5_000_000, tombstone.maxDoc());
    assertEquals(0, tombstone.edges().toCount());
    assertTrue(tombstone.isSparse());
    // it costs a pair of empty page arrays, whatever from-segment it stands for
    assertEquals(JoinColumnModel.tombstone(10).ramBytesUsed(), tombstone.ramBytesUsed());
    assertTrue(String.valueOf(tombstone.ramBytesUsed()), tombstone.ramBytesUsed() < 1024);
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, tombstone.toDocByFromDoc().nextDoc());
    assertFalse(tombstone.toDocByFromDoc().advanceExact(random().nextInt(5_000_000)));
  }

  /** The layout is chosen on how many values each remembers, so it flips at exactly 50% density. */
  public void testLayoutFlipsAtHalfDensity() {
    assertTrue(JoinColumnModel.sparseIsSmaller(0, 1_000));
    assertTrue(JoinColumnModel.sparseIsSmaller(499, 1_000));
    assertFalse(JoinColumnModel.sparseIsSmaller(500, 1_000));
    assertFalse(JoinColumnModel.sparseIsSmaller(1_000, 1_000));
    // the shape that motivated the split: a five-million-doc from-segment matching 2,125 docs
    assertTrue(JoinColumnModel.sparseIsSmaller(2_125, 5_031_945));
    assertFalse(JoinColumnModel.sparseIsSmaller(5_031_876, 5_031_945));
  }

  /**
   * Whatever the column -- or the from-side map, or the to-side inversion, which page at the same
   * size -- its widest possible page stays under half of a 1MB G1 region, so none of them is ever
   * allocated humongous, which is what paging them is for. Neither {@link PackedLongValues} nor
   * {@code PagedMutable} lays down a page wider than the page size at the full 64 bits.
   */
  public void testNoPageCanBeHumongous() {
    assertTrue(
        "page size " + JoinIndexUtils.PAGE_SIZE + " can exceed a G1 humongous threshold",
        (long) JoinIndexUtils.PAGE_SIZE * Long.BYTES < 512 * 1024);
  }

  /**
   * A dense column of the width that crashed production costs a fraction of the flat {@code int[]}
   * it replaces, because a to-doc is stored at the bits it needs rather than at 32.
   */
  public void testWideDenseColumnPacksBelowItsIntArray() {
    int maxDoc = 2_000_000;
    JoinColumnModel.Builder column = JoinColumnModel.builder(maxDoc, false);
    for (int fromDoc = 0; fromDoc < maxDoc; fromDoc++) {
      column.add(fromDoc, maxDoc - 1 - fromDoc);
    }
    JoinColumnModel dense =
        column.build(new Edges(new int[] {0, maxDoc - 1}, new int[] {0, maxDoc - 1}, maxDoc));
    assertFalse(dense.isSparse());
    long asIntArray = (long) maxDoc * Integer.BYTES;
    assertTrue(
        "packed " + dense.ramBytesUsed() + " against " + asIntArray + " bytes flat",
        dense.ramBytesUsed() < asIntArray * 3 / 4);
  }

  /** A sparse column really is sized to its matches, not to its from-segment. */
  public void testSparseColumnCostsWhatItHolds() {
    int maxDoc = 1_000_000;
    int[] toDocByFromDoc = new int[maxDoc];
    Arrays.fill(toDocByFromDoc, -1);
    for (int fromDoc = 0; fromDoc < maxDoc; fromDoc += 1_000) {
      toDocByFromDoc[fromDoc] = fromDoc;
    }
    JoinColumnModel sparse = sparse(toDocByFromDoc);
    assertTrue(sparse.isSparse());
    assertEquals(maxDoc, sparse.maxDoc());
    assertTrue(
        "a 0.1% dense column should cost far less than its dense twin: " + sparse.ramBytesUsed(),
        sparse.ramBytesUsed() * 100 < dense(toDocByFromDoc).ramBytesUsed());
  }

  /** Walks both layouts end to end with {@code nextDoc()}, as the batch writer does. */
  private void assertWalksAlike(int[] toDocByFromDoc) throws IOException {
    List<String> denseWalk = walk(dense(toDocByFromDoc).toDocByFromDoc());
    List<String> sparseWalk = walk(sparse(toDocByFromDoc).toDocByFromDoc());
    assertEquals(sparseWalk, denseWalk);
    assertEquals(expectedWalk(toDocByFromDoc), sparseWalk);
  }

  /**
   * Probes both layouts at an ascending sample of from-docs with {@code advanceExact}, as a drain
   * does along its from-doc iterator.
   */
  private void assertSeeksAlike(int[] toDocByFromDoc) throws IOException {
    SortedNumericDocValues denseCursor = dense(toDocByFromDoc).toDocByFromDoc();
    SortedNumericDocValues sparseCursor = sparse(toDocByFromDoc).toDocByFromDoc();
    for (int fromDoc = 0;
        fromDoc < toDocByFromDoc.length;
        fromDoc += TestUtil.nextInt(random(), 1, 8)) {
      boolean denseHit = denseCursor.advanceExact(fromDoc);
      assertEquals("at from-doc " + fromDoc, denseHit, sparseCursor.advanceExact(fromDoc));
      assertEquals("at from-doc " + fromDoc, toDocByFromDoc[fromDoc] >= 0, denseHit);
      if (denseHit) {
        assertEquals(toDocByFromDoc[fromDoc], (int) denseCursor.nextValue());
        assertEquals(toDocByFromDoc[fromDoc], (int) sparseCursor.nextValue());
      }
      assertEquals(fromDoc, sparseCursor.docID());
    }
  }

  private static List<String> walk(SortedNumericDocValues cursor) throws IOException {
    List<String> walked = new ArrayList<>();
    for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
      assertEquals(1, cursor.docValueCount());
      walked.add(doc + "->" + cursor.nextValue());
    }
    return walked;
  }

  private static List<String> expectedWalk(int[] toDocByFromDoc) {
    List<String> expected = new ArrayList<>();
    for (int fromDoc = 0; fromDoc < toDocByFromDoc.length; fromDoc++) {
      if (toDocByFromDoc[fromDoc] >= 0) {
        expected.add(fromDoc + "->" + toDocByFromDoc[fromDoc]);
      }
    }
    return expected;
  }

  /** A from-doc -> to-doc map where each from-doc matches with probability {@code density}. */
  private static int[] randomMap(int maxDoc, float density) {
    int[] toDocByFromDoc = new int[maxDoc];
    for (int fromDoc = 0; fromDoc < maxDoc; fromDoc++) {
      toDocByFromDoc[fromDoc] = random().nextFloat() < density ? random().nextInt(maxDoc) : -1;
    }
    return toDocByFromDoc;
  }

  private static JoinColumnModel dense(int[] toDocByFromDoc) {
    return JoinColumnModel.dense(toDocByFromDoc, edgesOf(toDocByFromDoc));
  }

  /** The same map as {@link #dense}, laid out as matches only. */
  private static JoinColumnModel sparse(int[] toDocByFromDoc) {
    return JoinColumnModel.sparse(toDocByFromDoc, edgesOf(toDocByFromDoc));
  }

  private static Edges edgesOf(int[] toDocByFromDoc) {
    int minFromDoc = -1;
    int maxFromDoc = -1;
    int minToDoc = -1;
    int maxToDoc = -1;
    int toCount = 0;
    for (int fromDoc = 0; fromDoc < toDocByFromDoc.length; fromDoc++) {
      int toDoc = toDocByFromDoc[fromDoc];
      if (toDoc < 0) {
        continue;
      }
      if (toCount == 0) {
        minFromDoc = fromDoc;
        minToDoc = toDoc;
        maxToDoc = toDoc;
      }
      maxFromDoc = fromDoc;
      minToDoc = Math.min(minToDoc, toDoc);
      maxToDoc = Math.max(maxToDoc, toDoc);
      toCount++;
    }
    return new Edges(new int[] {minFromDoc, maxFromDoc}, new int[] {minToDoc, maxToDoc}, toCount);
  }
}
