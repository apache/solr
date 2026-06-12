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
package org.apache.solr.schema;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;

/**
 * Utility class for creating DocValues {@link IndexableField} instances with optional skip index
 * support.
 *
 * <p>When {@code hasDocValuesSkipList} is true, this utility creates indexed DocValues fields that
 * include a range skip index for more efficient range queries. Otherwise, it creates standard
 * DocValues fields.
 */
public final class DocValuesFieldUtil {

  private DocValuesFieldUtil() {
    // utility class
  }

  /**
   * Creates a sorted DocValues field for a single-value (non-multi-valued) string/bytes field.
   *
   * @param fieldName the field name
   * @param value the bytes value
   * @param hasDocValuesSkipList whether to include a range skip index
   * @return the created IndexableField
   */
  public static IndexableField createSortedDocValuesField(
      String fieldName, BytesRef value, boolean hasDocValuesSkipList) {
    return hasDocValuesSkipList
        ? SortedDocValuesField.indexedField(fieldName, value)
        : new SortedDocValuesField(fieldName, value);
  }

  /**
   * Creates a sorted set DocValues field for a multi-valued string/bytes field.
   *
   * @param fieldName the field name
   * @param value the bytes value
   * @param hasDocValuesSkipList whether to include a range skip index
   * @return the created IndexableField
   */
  public static IndexableField createSortedSetDocValuesField(
      String fieldName, BytesRef value, boolean hasDocValuesSkipList) {
    return hasDocValuesSkipList
        ? SortedSetDocValuesField.indexedField(fieldName, value)
        : new SortedSetDocValuesField(fieldName, value);
  }

  /**
   * Creates a numeric DocValues field for a single-value numeric field.
   *
   * @param fieldName the field name
   * @param value the long value
   * @param hasDocValuesSkipList whether to include a range skip index
   * @return the created IndexableField
   */
  public static IndexableField createNumericDocValuesField(
      String fieldName, long value, boolean hasDocValuesSkipList) {
    return hasDocValuesSkipList
        ? NumericDocValuesField.indexedField(fieldName, value)
        : new NumericDocValuesField(fieldName, value);
  }

  /**
   * Creates a sorted numeric DocValues field for a multi-valued numeric field.
   *
   * @param fieldName the field name
   * @param value the long value
   * @param hasDocValuesSkipList whether to include a range skip index
   * @return the created IndexableField
   */
  public static IndexableField createSortedNumericDocValuesField(
      String fieldName, long value, boolean hasDocValuesSkipList) {
    return hasDocValuesSkipList
        ? SortedNumericDocValuesField.indexedField(fieldName, value)
        : new SortedNumericDocValuesField(fieldName, value);
  }
}
