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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedIntFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * An {@code NumericField} implementation of a field for {@code Int} values using {@code IntPoint}, {@code StringField}, {@code SortedNumericDocValuesField} and {@code StoredField}.
 *
 * @see PointField
 * @see IntPoint
 */
public class IntField extends NumericField implements IntValueFieldType {

  public IntField() {
    type = NumberType.INTEGER;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).intValue();
    if (val instanceof CharSequence) return Integer.parseInt(val.toString());
    return super.toNativeType(val);
  }

  @Override
  public Query getPointFieldQuery(QParser parser, SchemaField field, String value) {
    return IntPoint.newExactQuery(field.getName(), parseIntFromUser(field.getName(), value));
  }

  @Override
  public Query getDocValuesFieldQuery(QParser parser, SchemaField field, String value) {
    return SortedNumericDocValuesField.newSlowExactQuery(field.getName(), parseIntFromUser(field.getName(), value));
  }

  @Override
  public Query getPointRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    int actualMin, actualMax;
    if (min == null) {
      actualMin = Integer.MIN_VALUE;
    } else {
      actualMin = parseIntFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Integer.MAX_VALUE) return new MatchNoDocsQuery();
        ++actualMin;
      }
    }
    if (max == null) {
      actualMax = Integer.MAX_VALUE;
    } else {
      actualMax = parseIntFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Integer.MIN_VALUE) return new MatchNoDocsQuery();
        --actualMax;
      }
    }
    return IntPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Query getDocValuesRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    int actualMin, actualMax;
    if (min == null) {
      actualMin = Integer.MIN_VALUE;
    } else {
      actualMin = parseIntFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Integer.MAX_VALUE) return new MatchNoDocsQuery();
        ++actualMin;
      }
    }
    if (max == null) {
      actualMax = Integer.MAX_VALUE;
    } else {
      actualMax = parseIntFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Integer.MIN_VALUE) return new MatchNoDocsQuery();
        --actualMax;
      }
    }
    return SortedNumericDocValuesField.newSlowRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Query getPointSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    int[] values = new int[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      values[i++] = parseIntFromUser(field.getName(), val);
    }
    return IntPoint.newSetQuery(field.getName(), values);
  }

  @Override
  public Query getDocValuesSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    long[] points = new long[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      points[i++] = parseIntFromUser(field.getName(), val);
    }
    return SortedNumericDocValuesField.newSlowSetQuery(field.getName(), points);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return IntPoint.decodeDimension(term.bytes, term.offset);
  }

  @Override
  public Object toObject(IndexableField f) {
    final StoredValue storedValue = f.storedValue();
    if (storedValue != null) {
      return storedValue.getIntValue();
    }
    final Number val = f.numericValue();
    if (val != null) {
      return val.intValue();
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  public String storedToReadable(IndexableField f) {
    return Integer.toString(f.storedValue().getIntValue());
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Integer.toString(IntPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Integer.BYTES);
    result.setLength(Integer.BYTES);
    IntPoint.encodeDimension(parseIntFromUser(null, val.toString()), result.bytes(), 0);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return null;
    } else {
      return Type.INTEGER_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new MultiValuedIntFieldSource(field.getName(), SortedNumericSelector.Type.MIN);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField f) {
    return new MultiValuedIntFieldSource(f.getName(), choice);
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    int intValue =
        (value instanceof Number)
            ? ((Number) value).intValue()
            : Integer.parseInt(value.toString());
    return Collections.singletonList(new SolrIntField(sf.getName(), intValue, sf.indexed(), sf.enhancedIndex(), sf.hasDocValues(), sf.stored()));
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    int intValue =
        (value instanceof Number)
            ? ((Number) value).intValue()
            : Integer.parseInt(value.toString());
    return new IntPoint(field.getName(), intValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Integer) this.toNativeType(value));
  }

  /**
   * Wrapper for {@link Integer#parseInt(String)} that throws a BAD_REQUEST error if the input is
   * not valid
   *
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static int parseIntFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException(
          "Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }

  static final class SolrIntField extends Field {

    static org.apache.lucene.document.FieldType getType(boolean rangeIndex, boolean termIndex, boolean docValues, boolean stored) {
      org.apache.lucene.document.FieldType type = new org.apache.lucene.document.FieldType();
      if (rangeIndex) {
        type.setDimensions(1, Integer.BYTES);
      }
      if (termIndex) {
        type.setIndexOptions(IndexOptions.DOCS);
      }
      if (docValues) {
        type.setDocValuesType(DocValuesType.SORTED_NUMERIC);
      }
      type.setTokenized(false);
      type.setStored(stored);
      type.freeze();
      return type;
    }

    private final StoredValue storedValue;

    /**
     * Creates a new IntField, indexing the provided point and term, storing it as a DocValue, and optionally
     * storing it as a stored field.
     *
     * @param name field name
     * @param value the int value
     * @throws IllegalArgumentException if the field name or value is null.
     */
    public SolrIntField(String name, int value, boolean rangeIndex, boolean termIndex, boolean docValues, boolean stored) {
      super(name, getType(rangeIndex, termIndex, docValues, stored));
      fieldsData = (long) value;
      if (stored) {
        storedValue = new StoredValue(value);
      } else {
        storedValue = null;
      }
    }

    @Override
    public InvertableType invertableType() {
      return InvertableType.BINARY;
    }

    @Override
    public BytesRef binaryValue() {
      byte[] encodedPoint = new byte[Integer.BYTES];
      int value = getValueAsInt();
      IntPoint.encodeDimension(value, encodedPoint, 0);
      return new BytesRef(encodedPoint);
    }

    private int getValueAsInt() {
      return numericValue().intValue();
    }

    @Override
    public StoredValue storedValue() {
      return storedValue;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " <" + name + ':' + getValueAsInt() + '>';
    }

    @Override
    public void setIntValue(int value) {
      super.setLongValue(value);
      if (storedValue != null) {
        storedValue.setIntValue(value);
      }
    }

    @Override
    public void setLongValue(long value) {
      throw new IllegalArgumentException("cannot change value type from Int to Long");
    }
  }
}
