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

package org.apache.solr.update;

import java.util.Comparator;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.ParallelLeafReader;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;

/**
 * Verifies the time-based leaf (segment) ordering produced by {@link SegmentLeafSorter}. The
 * field-based ordering is exercised where a real schema/core is available (see {@code
 * SegmentSortFieldTest}).
 */
public class SegmentLeafSorterTest extends SolrTestCase {

  public void testNoneReturnsNoSorter() {
    assertNull(SegmentLeafSorter.forConfig(SegmentSort.NONE, null));
  }

  public void testParseRejectsGarbage() {
    expectThrows(IllegalArgumentException.class, () -> SegmentSort.parse("nonsense value here"));
    expectThrows(IllegalArgumentException.class, () -> SegmentSort.parse("field bogusorder"));
  }

  /**
   * A leaf that does not resolve to a SegmentReader must be tolerated (ordered last), not crash the
   * comparator. A {@link ParallelLeafReader} extends {@link LeafReader} directly (it is not a
   * FilterLeafReader), so it does not unwrap to a SegmentReader and has no segment timestamp.
   */
  public void testNonSegmentReaderSortsLastWithoutFailing() throws Exception {
    try (Directory dirA = newDirectory();
        Directory dirB = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dirA, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new StringField("id", "0", Store.YES));
        writer.addDocument(doc);
        writer.commit();
      }
      // A second index with a disjoint field, so the two leaves can be combined into a
      // multi-reader ParallelLeafReader (which reports no core cache key).
      try (IndexWriter writer = new IndexWriter(dirB, new IndexWriterConfig())) {
        Document doc = new Document();
        doc.add(new StringField("other", "0", Store.YES));
        writer.addDocument(doc);
        writer.commit();
      }
      try (DirectoryReader readerA = DirectoryReader.open(dirA);
          DirectoryReader readerB = DirectoryReader.open(dirB)) {
        LeafReader segmentLeaf = readerA.leaves().get(0).reader();
        LeafReader otherSegmentLeaf = readerB.leaves().get(0).reader();
        // A multi-reader ParallelLeafReader does not resolve to a single SegmentReader (and reports
        // no core cache key), so it has no segment timestamp and must sort last.
        try (LeafReader nonSegmentLeaf =
            new ParallelLeafReader(false, segmentLeaf, otherSegmentLeaf)) {
          for (String spec : new String[] {"TIME_ASC", "TIME_DESC"}) {
            Comparator<LeafReader> sorter =
                SegmentLeafSorter.forConfig(SegmentSort.parse(spec), null);
            assertNotNull(sorter);
            // Must not throw for either ordering, regardless of argument order.
            sorter.compare(segmentLeaf, nonSegmentLeaf);
            sorter.compare(nonSegmentLeaf, segmentLeaf);
            // The known-timestamp segment sorts before the unknown one in both directions.
            assertTrue(
                "segment with a timestamp should sort before one without (" + spec + ")",
                sorter.compare(segmentLeaf, nonSegmentLeaf) < 0);
          }
        }
      }
    }
  }

  public void testTimeDescVisitsNewestSegmentsFirst() throws Exception {
    assertLeafOrder("TIME_DESC", /* newestFirst= */ true);
  }

  public void testTimeAscVisitsOldestSegmentsFirst() throws Exception {
    assertLeafOrder("TIME_ASC", /* newestFirst= */ false);
  }

  /**
   * Writes three single-document segments (each commit creates its own segment), reopens the reader
   * with the leaf sorter for the given order, and asserts the segments are visited by creation time
   * as expected.
   */
  private void assertLeafOrder(String spec, boolean newestFirst) throws Exception {
    Comparator<LeafReader> leafSorter = SegmentLeafSorter.forConfig(SegmentSort.parse(spec), null);
    assertNotNull(leafSorter);

    try (Directory dir = newDirectory()) {
      // Disable merging so each commit stays its own segment with a distinct timestamp.
      IndexWriterConfig iwc = new IndexWriterConfig().setLeafSorter(leafSorter);
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 3; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Store.YES));
          writer.addDocument(doc);
          writer.commit();
          // Ensure a strictly increasing per-segment timestamp (diagnostics are millis-resolution).
          Thread.sleep(5);
        }
      }

      try (DirectoryReader reader = DirectoryReader.open(dir, leafSorter)) {
        List<LeafReaderContext> leaves = reader.leaves();
        assertEquals(3, leaves.size());
        long previous = -1;
        for (LeafReaderContext leaf : leaves) {
          long timestamp = segmentTimestamp(leaf.reader());
          if (previous >= 0) {
            if (newestFirst) {
              assertTrue(
                  "expected descending segment timestamps, got " + previous + " then " + timestamp,
                  timestamp <= previous);
            } else {
              assertTrue(
                  "expected ascending segment timestamps, got " + previous + " then " + timestamp,
                  timestamp >= previous);
            }
          }
          previous = timestamp;
        }
      }
    }
  }

  private static long segmentTimestamp(LeafReader reader) {
    SegmentReader segmentReader = (SegmentReader) reader;
    return Long.parseLong(segmentReader.getSegmentInfo().info.getDiagnostics().get("timestamp"));
  }
}
