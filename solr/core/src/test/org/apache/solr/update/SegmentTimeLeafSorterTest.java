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
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.SolrTestCase;

/** Verifies the leaf (segment) ordering produced by {@link SegmentTimeLeafSorter}. */
public class SegmentTimeLeafSorterTest extends SolrTestCase {

  public void testNoneReturnsNoSorter() {
    assertNull(SegmentTimeLeafSorter.forOrder(SegmentSort.NONE));
  }

  public void testTimeDescVisitsNewestSegmentsFirst() throws Exception {
    assertLeafOrder(SegmentSort.TIME_DESC, /* newestFirst= */ true);
  }

  public void testTimeAscVisitsOldestSegmentsFirst() throws Exception {
    assertLeafOrder(SegmentSort.TIME_ASC, /* newestFirst= */ false);
  }

  /**
   * Writes three single-document segments (each commit creates its own segment), reopens the reader
   * with the leaf sorter for the given order, and asserts the segments are visited by creation time
   * as expected. Segment creation time comes from Lucene's per-segment {@code timestamp}
   * diagnostic.
   */
  private void assertLeafOrder(SegmentSort order, boolean newestFirst) throws Exception {
    Comparator<LeafReader> leafSorter = SegmentTimeLeafSorter.forOrder(order);
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
