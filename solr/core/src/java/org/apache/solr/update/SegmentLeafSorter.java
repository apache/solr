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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.NumberType;
import org.apache.solr.schema.SchemaField;

/**
 * Builds the {@link Comparator} for the {@code <segmentSort>} configuration, ordering a {@link
 * org.apache.lucene.index.DirectoryReader}'s leaf readers (segments) at open time via {@link
 * org.apache.lucene.index.IndexWriterConfig#setLeafSorter(Comparator)}.
 *
 * <p>For a {@link SegmentSort.Kind#TIME} sort, the key is the segment creation time (from Lucene's
 * per-segment {@code timestamp} diagnostic). For a {@link SegmentSort.Kind#FIELD} sort, the key is
 * the per-segment minimum ({@code asc}) or maximum ({@code desc}) of a numeric docValues field,
 * with each value normalized to an order-preserving long (see {@code toSortableLong}) so the raw
 * {@code long} comparison matches true numeric order for every numeric type. A leaf whose key
 * cannot be determined (e.g. a reader that does not resolve to a {@link SegmentReader}, or a
 * segment with no value for the field) sorts last, so an unexpected reader shape degrades ordering
 * rather than failing the search. (A genuine I/O error while reading docValues still propagates.)
 */
final class SegmentLeafSorter {

  private static final String TIMESTAMP_DIAGNOSTIC_KEY = "timestamp";

  private SegmentLeafSorter() {}

  /**
   * Returns the leaf sorter for the given config, or null for {@link SegmentSort.Kind#NONE} (no
   * leaf sorter installed). For a field sort the field must exist and be a single-valued numeric
   * docValues field.
   */
  static Comparator<LeafReader> forConfig(SegmentSort segmentSort, IndexSchema schema) {
    switch (segmentSort.kind()) {
      case NONE:
        return null;
      case TIME:
        return build(segmentSort.descending(), SegmentLeafSorter::segmentTimestamp);
      case FIELD:
        final SchemaField sf = validateFieldForSort(segmentSort.field(), schema);
        final String field = sf.getName();
        final NumberType numberType = sf.getType().getNumberType();
        final boolean descending = segmentSort.descending();
        // asc orders by each segment's min value; desc by its max value.
        return build(descending, leaf -> segmentFieldExtremum(leaf, field, numberType, descending));
      default:
        return null;
    }
  }

  private static SchemaField validateFieldForSort(String field, IndexSchema schema) {
    SchemaField sf = schema.getFieldOrNull(field);
    if (sf == null) {
      throw new IllegalArgumentException(
          "<segmentSort> field '" + field + "' does not exist in the schema");
    }
    if (!sf.hasDocValues()) {
      throw new IllegalArgumentException(
          "<segmentSort> field '" + field + "' must have docValues=\"true\"");
    }
    if (sf.getType().getNumberType() == null) {
      throw new IllegalArgumentException(
          "<segmentSort> field '" + field + "' must be a numeric field");
    }
    if (sf.multiValued()) {
      throw new IllegalArgumentException(
          "<segmentSort> field '" + field + "' must be single-valued");
    }
    return sf;
  }

  /**
   * Builds a comparator over the given per-leaf key, memoizing the key per segment (the comparator
   * is invoked O(n log n) times while the reader is opened). Unknown keys sort last in both
   * directions.
   */
  private static Comparator<LeafReader> build(boolean descending, KeyFn keyFn) {
    final long missingValue = descending ? Long.MIN_VALUE : Long.MAX_VALUE;
    final Map<Object, Long> cache = new ConcurrentHashMap<>();
    Comparator<LeafReader> byKey =
        Comparator.comparingLong(
            leaf -> {
              Object cacheKey = coreCacheKey(leaf);
              if (cacheKey == null) {
                return safeKey(keyFn, leaf, missingValue);
              }
              return cache.computeIfAbsent(cacheKey, k -> safeKey(keyFn, leaf, missingValue));
            });
    return descending ? byKey.reversed() : byKey;
  }

  private static long safeKey(KeyFn keyFn, LeafReader leaf, long missingValue) {
    try {
      Long key = keyFn.key(leaf);
      return key == null ? missingValue : key;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Object coreCacheKey(LeafReader leaf) {
    var helper = leaf.getCoreCacheHelper();
    return helper == null ? null : helper.getKey();
  }

  private static Long segmentTimestamp(LeafReader reader) {
    LeafReader unwrapped = FilterLeafReader.unwrap(reader);
    if (unwrapped instanceof SegmentReader segmentReader) {
      String timestamp =
          segmentReader.getSegmentInfo().info.getDiagnostics().get(TIMESTAMP_DIAGNOSTIC_KEY);
      if (timestamp != null) {
        try {
          return Long.parseLong(timestamp);
        } catch (NumberFormatException ignored) {
          return null;
        }
      }
    }
    return null;
  }

  /**
   * Per-segment min (asc) or max (desc) of the field's numeric docValues, as sortable-bits long.
   */
  private static Long segmentFieldExtremum(
      LeafReader leaf, String field, NumberType numberType, boolean max) throws IOException {
    // Solr writes single-valued numeric docValues as NumericDocValues (older indexes may use
    // SortedNumericDocValues); read either. Values are normalized to an order-preserving
    // ("sortable")
    // long before comparison -- see toSortableLong.
    NumericDocValues numeric = leaf.getNumericDocValues(field);
    if (numeric != null) {
      return scan(max, numeric::nextDoc, () -> toSortableLong(numeric.longValue(), numberType));
    }
    SortedNumericDocValues sorted = leaf.getSortedNumericDocValues(field);
    if (sorted != null) {
      // Normally single-valued (one value per doc). A legacy segment written while the field was
      // multi-valued may have more; values come in ascending order, so take the first for a min
      // scan (asc) and the last for a max scan (desc).
      return scan(max, sorted::nextDoc, () -> toSortableLong(docValue(sorted, max), numberType));
    }
    return null;
  }

  private static long docValue(SortedNumericDocValues sorted, boolean max) throws IOException {
    int count = sorted.docValueCount();
    long value = sorted.nextValue();
    if (max) {
      for (int i = 1; i < count; i++) {
        value = sorted.nextValue();
      }
    }
    return value;
  }

  /**
   * Normalizes a raw docValues long into an order-preserving long so a raw {@code long} min/max
   * matches the field's numeric order. Single-valued FLOAT/DOUBLE docValues are stored by Solr as
   * raw {@link Float#floatToIntBits}/{@link Double#doubleToLongBits} (not sortable bits), which do
   * not order correctly for negative values, so they are decoded and re-encoded to sortable bits.
   * INTEGER/LONG/DATE are already order-correct as signed longs.
   */
  private static long toSortableLong(long raw, NumberType numberType) {
    switch (numberType) {
      case FLOAT:
        return NumericUtils.floatToSortableInt(Float.intBitsToFloat((int) raw));
      case DOUBLE:
        return NumericUtils.doubleToSortableLong(Double.longBitsToDouble(raw));
      default: // INTEGER, LONG, DATE
        return raw;
    }
  }

  private static Long scan(boolean max, DocIterator advance, ValueSupplier value)
      throws IOException {
    long extremum = max ? Long.MIN_VALUE : Long.MAX_VALUE;
    boolean any = false;
    for (int doc = advance.nextDoc(); doc != NO_MORE; doc = advance.nextDoc()) {
      long v = value.get();
      extremum = max ? Math.max(extremum, v) : Math.min(extremum, v);
      any = true;
    }
    return any ? extremum : null;
  }

  private static final int NO_MORE = DocIdSetIterator.NO_MORE_DOCS;

  @FunctionalInterface
  private interface KeyFn {
    Long key(LeafReader leaf) throws IOException;
  }

  @FunctionalInterface
  private interface DocIterator {
    int nextDoc() throws IOException;
  }

  @FunctionalInterface
  private interface ValueSupplier {
    long get() throws IOException;
  }
}
