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
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedLongFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * An {@code NumericField} implementation of a field for {@code Long} values using {@code
 * LongPoint}, {@code StringField}, {@code SortedNumericDocValuesField} and {@code StoredField}.
 *
 * @see PointField
 * @see LongPoint
 */
public class LongField extends NumericField implements LongValueFieldType {

  public LongField() {
    type = NumberType.LONG;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).longValue();
    if (val instanceof CharSequence) return Long.parseLong(val.toString());
    return super.toNativeType(val);
  }

  @Override
  public Query getPointFieldQuery(QParser parser, SchemaField field, String value) {
    return LongPoint.newExactQuery(field.getName(), parseLongFromUser(field.getName(), value));
  }

  @Override
  public Query getDocValuesFieldQuery(QParser parser, SchemaField field, String value) {
    return SortedNumericDocValuesField.newSlowExactQuery(
        field.getName(), parseLongFromUser(field.getName(), value));
  }

  @Override
  public Query getPointRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    long actualMin, actualMax;
    if (min == null) {
      actualMin = Long.MIN_VALUE;
    } else {
      actualMin = parseLongFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Long.MAX_VALUE) return new MatchNoDocsQuery();
        ++actualMin;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = parseLongFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Long.MIN_VALUE) return new MatchNoDocsQuery();
        --actualMax;
      }
    }
    return LongPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Query getDocValuesRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    long actualMin, actualMax;
    if (min == null) {
      actualMin = Long.MIN_VALUE;
    } else {
      actualMin = parseLongFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Long.MAX_VALUE) return new MatchNoDocsQuery();
        ++actualMin;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = parseLongFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Long.MIN_VALUE) return new MatchNoDocsQuery();
        --actualMax;
      }
    }
    return SortedNumericDocValuesField.newSlowRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Query getPointSetQuery(
      QParser parser, SchemaField field, Collection<String> externalVals) {
    long[] values = new long[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      values[i++] = parseLongFromUser(field.getName(), val);
    }
    return LongPoint.newSetQuery(field.getName(), values);
  }

  @Override
  public Query getDocValuesSetQuery(
      QParser parser, SchemaField field, Collection<String> externalVals) {
    long[] points = new long[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      points[i++] = parseLongFromUser(field.getName(), val);
    }
    return SortedNumericDocValuesField.newSlowSetQuery(field.getName(), points);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return LongPoint.decodeDimension(term.bytes, term.offset);
  }

  @Override
  public Object toObject(IndexableField f) {
    final StoredValue storedValue = f.storedValue();
    if (storedValue != null) {
      return storedValue.getLongValue();
    }
    final Number val = f.numericValue();
    if (val != null) {
      return val.longValue();
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  public String storedToReadable(IndexableField f) {
    return Long.toString(f.storedValue().getLongValue());
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Long.toString(LongPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Long.BYTES);
    result.setLength(Long.BYTES);
    LongPoint.encodeDimension(parseLongFromUser(null, val.toString()), result.bytes(), 0);
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return null;
    } else {
      return Type.LONG_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new MultiValuedLongFieldSource(field.getName(), SortedNumericSelector.Type.MIN);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField f) {
    return new MultiValuedLongFieldSource(f.getName(), choice);
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    long longValue =
        (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
    return Collections.singletonList(
        new SolrLongField(
            sf.getName(),
            longValue,
            sf.indexed(),
            sf.enhancedIndex(),
            sf.hasDocValues(),
            sf.stored()));
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    long longValue =
        (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
    return new LongPoint(field.getName(), longValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Long) this.toNativeType(value));
  }

  /**
   * Wrapper for {@link Long#parseLong(String)} that throws a BAD_REQUEST error if the input is not
   * valid
   *
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static long parseLongFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException(
          "Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }

  static final class SolrLongField extends Field {

    static org.apache.lucene.document.FieldType getType(
        boolean rangeIndex, boolean termIndex, boolean docValues, boolean stored) {
      org.apache.lucene.document.FieldType type = new org.apache.lucene.document.FieldType();
      if (rangeIndex) {
        type.setDimensions(1, Long.BYTES);
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
     * Creates a new LongField, indexing the provided point and term, storing it as a DocValue, and
     * optionally storing it as a stored field.
     *
     * @param name field name
     * @param value the long value
     * @throws IllegalArgumentException if the field name or value is null.
     */
    public SolrLongField(
        String name,
        long value,
        boolean rangeIndex,
        boolean termIndex,
        boolean docValues,
        boolean stored) {
      super(name, getType(rangeIndex, termIndex, docValues, stored));
      fieldsData = value;
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
      byte[] encodedPoint = new byte[Long.BYTES];
      long value = getValueAsLong();
      LongPoint.encodeDimension(value, encodedPoint, 0);
      return new BytesRef(encodedPoint);
    }

    private long getValueAsLong() {
      return numericValue().longValue();
    }

    @Override
    public StoredValue storedValue() {
      return storedValue;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " <" + name + ':' + getValueAsLong() + '>';
    }

    @Override
    public void setLongValue(long value) {
      super.setLongValue(value);
      if (storedValue != null) {
        storedValue.setLongValue(value);
      }
    }
  }
}
