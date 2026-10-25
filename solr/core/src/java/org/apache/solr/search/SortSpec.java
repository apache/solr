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
package org.apache.solr.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.schema.SchemaField;

/** SortSpec encapsulates a Lucene Sort and a count of the number of documents to return. */
public class SortSpec {
  private Sort sort;
  private List<SchemaField> fields;
  private int num = 10;
  private int offset = 0;

  public SortSpec(Sort sort, List<SchemaField> fields, int num, int offset) {
    setSortAndFields(sort, fields);
    this.num = num;
    this.offset = offset;
  }

  public SortSpec(Sort sort, List<SchemaField> fields) {
    setSortAndFields(sort, fields);
  }

  public SortSpec(Sort sort, SchemaField[] fields, int num, int offset) {
    setSortAndFields(sort, Arrays.asList(fields));
    this.num = num;
    this.offset = offset;
  }

  public SortSpec(Sort sort, SchemaField[] fields) {
    setSortAndFields(sort, Arrays.asList(fields));
  }

  /** Returns the inverse order, retaining each field's value selection and missing-value policy. */
  public SortSpec reverse() {
    SortField[] original = (sort == null ? Sort.RELEVANCE : sort).getSort();
    SortField[] reversed = new SortField[original.length];
    for (int i = 0; i < original.length; i++) {
      reversed[i] =
          original[i] instanceof ReverseSortField field
              ? field.original
              : new ReverseSortField(original[i]);
    }
    return new SortSpec(
        new Sort(reversed), sort == null ? Collections.singletonList(null) : fields, num, offset);
  }

  private static final class ReverseSortField extends SortField {
    private final SortField original;

    ReverseSortField(SortField original) {
      super(
          original.getField(),
          original.getType(),
          !original.getReverse(),
          original.getMissingValue());
      this.original = original;
    }

    @Override
    public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
      // Rebuilding the field with the opposite direction can change a multi-valued selector or
      // its missing-value policy. Invert the collector's comparison, not the value being compared.
      // Competitive skipping in the original comparator assumes its original direction.
      FieldComparator<?> comparator = original.getComparator(numHits, Pruning.NONE);
      comparator.disableSkipping();
      return comparator;
    }

    @Override
    @SuppressWarnings("ReferenceEquality") // Lucene rewrite signals no change by returning itself.
    public SortField rewrite(IndexSearcher searcher) throws IOException {
      SortField rewritten = original.rewrite(searcher);
      return rewritten == original ? this : new ReverseSortField(rewritten);
    }

    @Override
    public boolean needsScores() {
      return original.needsScores();
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof ReverseSortField field && original.equals(field.original);
    }

    @Override
    public int hashCode() {
      return 31 * original.hashCode() + ReverseSortField.class.hashCode();
    }
  }

  /**
   * the specified SchemaFields must correspond one to one with the Sort's SortFields, using null
   * where appropriate.
   */
  public void setSortAndFields(Sort s, List<SchemaField> fields) {

    assert null == s || s.getSort().length == fields.size()
        : "SortFields and SchemaFields do not have equal lengths";
    this.sort = s;
    this.fields = Collections.unmodifiableList(fields);
  }

  public static boolean includesScore(Sort sort) {
    if (sort == null) return true;
    for (SortField sf : sort.getSort()) {
      if (sf.getType() == SortField.Type.SCORE) return true;
    }
    return false;
  }

  public boolean includesScore() {
    return includesScore(sort);
  }

  /**
   * Returns whether SortFields for the Sort includes a field besides SortField.Type.SCORE or
   * SortField.Type.DOC
   *
   * @return true if SortFields contains a field besides score or the Lucene doc
   */
  public boolean includesNonScoreOrDocField() {
    return includesNonScoreOrDocField(sort);
  }

  /**
   * Returns whether SortFields for the Sort includes a field besides SortField.Type.SCORE or
   * SortField.Type.DOC
   *
   * @param sort org.apache.lucene.search.Sort
   * @return true if SortFields contains a field besides score or the Lucene doc
   */
  public static boolean includesNonScoreOrDocField(Sort sort) {
    if (sort == null) return false;
    for (SortField sf : sort.getSort()) {
      if (sf.getType() != SortField.Type.SCORE && sf.getType() != SortField.Type.DOC) return true;
    }
    return false;
  }

  /** Gets the Lucene Sort object, or null for the default sort by score descending. */
  public Sort getSort() {
    return sort;
  }

  /**
   * Gets the Solr SchemaFields that correspond to each of the SortFields used in this sort. The
   * array may contain null if a SortField doesn't correspond directly to a SchemaField (ie: score,
   * lucene docid, custom function sorting, etc...)
   *
   * @return an immutable list, may be empty if getSort is null
   */
  public List<SchemaField> getSchemaFields() {
    return fields;
  }

  /** Offset into the list of results. */
  public int getOffset() {
    return offset;
  }

  public void setOffset(int offset) {
    this.offset = offset;
  }

  /**
   * Gets the number of documents to return after sorting.
   *
   * @return number of docs to return, or -1 for no cut off (just sort)
   */
  public int getCount() {
    return num;
  }

  public void setCount(int count) {
    this.num = count;
  }

  @Override
  public String toString() {
    return "start=" + offset + "&rows=" + num + (sort == null ? "" : "&sort=" + sort);
  }
}
