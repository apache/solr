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

import java.util.Locale;

/**
 * Parsed value of the {@code <segmentSort>} index configuration, which controls the order in which
 * a {@link org.apache.lucene.index.DirectoryReader}'s leaf readers (segments) are visited at search
 * time (Lucene's "leaf sorter"). This is "between-segment" ordering, distinct from index sort (the
 * "within-segment" order of documents), and has no effect on merging.
 *
 * <p>Two forms are accepted:
 *
 * <ul>
 *   <li>a time preset, {@code TIME_ASC} or {@code TIME_DESC}, ordering by each segment's creation
 *       time (a segment property; used to visit the most recently written NRT segments first); or
 *   <li>a field sort spec, {@code <fieldName> asc|desc}, ordering by each segment's minimum (for
 *       {@code asc}) or maximum (for {@code desc}) value of a numeric docValues field.
 * </ul>
 */
public final class SegmentSort {

  /** No segment ordering is imposed (Lucene's default). */
  public static final SegmentSort NONE = new SegmentSort(Kind.NONE, false, null);

  /** How segments are ordered. */
  public enum Kind {
    NONE,
    TIME,
    FIELD
  }

  private final Kind kind;
  private final boolean descending;
  private final String field; // non-null only for Kind.FIELD

  private SegmentSort(Kind kind, boolean descending, String field) {
    this.kind = kind;
    this.descending = descending;
    this.field = field;
  }

  public Kind kind() {
    return kind;
  }

  public boolean descending() {
    return descending;
  }

  /** The field name for a {@link Kind#FIELD} sort; null otherwise. */
  public String field() {
    return field;
  }

  /**
   * Parses a {@code <segmentSort>} value. Returns {@link #NONE} for null/blank. Accepts the presets
   * {@code TIME_ASC}/{@code TIME_DESC} (case-insensitive), or a {@code <field> asc|desc} spec.
   *
   * @throws IllegalArgumentException if the value is neither a known preset nor a valid {@code
   *     <field> asc|desc} spec
   */
  public static SegmentSort parse(String value) {
    if (value == null || value.isBlank()) {
      return NONE;
    }
    final String trimmed = value.trim();
    final String upper = trimmed.toUpperCase(Locale.ROOT);
    if (upper.equals("NONE")) {
      return NONE;
    }
    if (upper.equals("TIME_ASC")) {
      return new SegmentSort(Kind.TIME, false, null);
    }
    if (upper.equals("TIME_DESC")) {
      return new SegmentSort(Kind.TIME, true, null);
    }
    // Otherwise expect "<field> asc|desc".
    final String[] parts = trimmed.split("\\s+");
    if (parts.length == 2) {
      final String order = parts[1].toLowerCase(Locale.ROOT);
      if (order.equals("asc") || order.equals("desc")) {
        return new SegmentSort(Kind.FIELD, order.equals("desc"), parts[0]);
      }
    }
    throw new IllegalArgumentException(
        "Invalid <segmentSort> value '"
            + value
            + "'; expected NONE, TIME_ASC, TIME_DESC, or '<field> asc|desc'");
  }

  @Override
  public String toString() {
    switch (kind) {
      case NONE:
        return "NONE";
      case TIME:
        return descending ? "TIME_DESC" : "TIME_ASC";
      case FIELD:
        return field + (descending ? " desc" : " asc");
      default:
        return kind.name();
    }
  }
}
