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
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentReader;

/**
 * Builds a {@link Comparator} that orders a {@link org.apache.lucene.index.DirectoryReader}'s leaf
 * readers by the creation timestamp of their underlying segment, for use with {@link
 * org.apache.lucene.index.IndexWriterConfig#setLeafSorter(Comparator)}.
 *
 * <p>The timestamp is read from the segment's {@link
 * org.apache.lucene.index.SegmentInfo#getDiagnostics() diagnostics}, where Lucene records it under
 * the {@code timestamp} key at flush and merge time. A leaf whose timestamp cannot be determined
 * (for example a reader that does not wrap a {@link SegmentReader}) is ordered last, so an
 * unexpected reader shape degrades the ordering rather than failing the search.
 */
final class SegmentTimeLeafSorter {

  /**
   * Diagnostics key under which Lucene's IndexWriter records the segment creation time (millis).
   */
  private static final String TIMESTAMP_DIAGNOSTIC_KEY = "timestamp";

  private SegmentTimeLeafSorter() {}

  /**
   * Returns the leaf sorter for the given ordering, or null for {@link SegmentSort#NONE} (meaning
   * no leaf sorter should be installed).
   */
  static Comparator<LeafReader> forOrder(SegmentSort order) {
    if (order == SegmentSort.NONE) {
      return null;
    }
    final boolean ascending = order == SegmentSort.TIME_ASC;
    // Unknown timestamps sort last regardless of direction, so they never displace ordered leaves.
    final long missingValue = ascending ? Long.MAX_VALUE : Long.MIN_VALUE;
    Comparator<LeafReader> byTimestamp =
        Comparator.comparingLong(reader -> segmentTimestamp(reader, missingValue));
    return ascending ? byTimestamp : byTimestamp.reversed();
  }

  private static long segmentTimestamp(LeafReader reader, long missingValue) {
    LeafReader unwrapped = FilterLeafReader.unwrap(reader);
    if (unwrapped instanceof SegmentReader segmentReader) {
      String timestamp =
          segmentReader.getSegmentInfo().info.getDiagnostics().get(TIMESTAMP_DIAGNOSTIC_KEY);
      if (timestamp != null) {
        try {
          return Long.parseLong(timestamp);
        } catch (NumberFormatException e) {
          return missingValue;
        }
      }
    }
    return missingValue;
  }
}
