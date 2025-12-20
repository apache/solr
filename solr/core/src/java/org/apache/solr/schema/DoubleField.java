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

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedDoubleFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An {@code NumericField} implementation of a field for {@code Double} values using {@code DoublePoint}, {@code StringField}, {@code SortedNumericDocValuesField} and {@code StoredField}.
 *
 * @see PointField
 * @see DoublePoint
 */
public class DoubleField extends NumericField implements DoubleValueFieldType {

  public DoubleField() {
    type = NumberType.DOUBLE;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).doubleValue();
    if (val instanceof CharSequence) return Double.parseDouble(val.toString());
    return super.toNativeType(val);
  }

  @Override
  public Query getPointFieldQuery(QParser parser, SchemaField field, String value) {
    return DoublePoint.newExactQuery(field.getName(), parseDoubleFromUser(field.getName(), value));
  }

  @Override
  public Query getDocValuesFieldQuery(QParser parser, SchemaField field, String value) {
    return SortedNumericDocValuesField.newSlowExactQuery(field.getName(), NumericUtils.doubleToSortableLong(parseDoubleFromUser(field.getName(), value)));
  }

  @Override
  public Query getPointRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    double actualMin, actualMax;
    if (min == null) {
      actualMin = Double.NEGATIVE_INFINITY;
    } else {
      actualMin = parseDoubleFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Double.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        actualMin = DoublePoint.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Double.POSITIVE_INFINITY;
    } else {
      actualMax = parseDoubleFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Double.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        actualMax = DoublePoint.nextDown(actualMax);
      }
    }
    return DoublePoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Query getDocValuesRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    double actualMin, actualMax;
    if (min == null) {
      actualMin = Double.NEGATIVE_INFINITY;
    } else {
      actualMin = parseDoubleFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Double.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        actualMin = DoublePoint.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Double.POSITIVE_INFINITY;
    } else {
      actualMax = parseDoubleFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Double.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        actualMax = DoublePoint.nextDown(actualMax);
      }
    }
    return SortedNumericDocValuesField.newSlowRangeQuery(field.getName(), NumericUtils.doubleToSortableLong(actualMin), NumericUtils.doubleToSortableLong(actualMax));
  }

  @Override
  public Query getPointSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    double[] values = new double[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      values[i++] = parseDoubleFromUser(field.getName(), val);
    }
    return DoublePoint.newSetQuery(field.getName(), values);
  }

  @Override
  public Query getDocValuesSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    long[] points = new long[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      points[i++] = NumericUtils.doubleToSortableLong(parseDoubleFromUser(field.getName(), val));
    }
    return SortedNumericDocValuesField.newSlowSetQuery(field.getName(), points);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return DoublePoint.decodeDimension(term.bytes, term.offset);
  }

  @Override
  public Object toObject(IndexableField f) {
    final StoredValue storedValue = f.storedValue();
    if (storedValue != null) {
      return storedValue.getDoubleValue();
    }
    final Number val = f.numericValue();
    if (val != null) {
      return NumericUtils.sortableLongToDouble(val.longValue());
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  public String storedToReadable(IndexableField f) {
    return Double.toString(f.storedValue().getDoubleValue());
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Double.toString(DoublePoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Double.BYTES);
    result.setLength(Double.BYTES);
    DoublePoint.encodeDimension(parseDoubleFromUser(null, val.toString()), result.bytes(), 0);
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
    return new MultiValuedDoubleFieldSource(field.getName(), SortedNumericSelector.Type.MIN);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField f) {
    return new MultiValuedDoubleFieldSource(f.getName(), choice);
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    double doubleValue =
        (value instanceof Number)
            ? ((Number) value).doubleValue()
            : Double.parseDouble(value.toString());
    return Collections.singletonList(new SolrDoubleField(sf.getName(), doubleValue, sf.indexed(), sf.enhancedIndex(), sf.hasDocValues(), sf.stored()));
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    double doubleValue =
        (value instanceof Number)
            ? ((Number) value).doubleValue()
            : Double.parseDouble(value.toString());
    return new DoublePoint(field.getName(), doubleValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Double) this.toNativeType(value));
  }

  @Override
  public Query getSpecializedExistenceQuery(QParser parser, SchemaField field) {
    return DoublePoint.newRangeQuery(field.getName(), Double.NEGATIVE_INFINITY, Double.NaN);
  }

  /**
   * Wrapper for {@link Double#parseDouble(String)} that throws a BAD_REQUEST error if the input is
   * not valid
   *
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static double parseDoubleFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException(
          "Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Double.parseDouble(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }

  static final class SolrDoubleField extends Field {

    static org.apache.lucene.document.FieldType getType(boolean rangeIndex, boolean termIndex, boolean docValues, boolean stored) {
      org.apache.lucene.document.FieldType type = new org.apache.lucene.document.FieldType();
      if (rangeIndex) {
        type.setDimensions(1, Double.BYTES);
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
     * Creates a new DoubleField, indexing the provided point and term, storing it as a DocValue, and optionally
     * storing it as a stored field.
     *
     * @param name field name
     * @param value the double value
     * @throws IllegalArgumentException if the field name or value is null.
     */
    public SolrDoubleField(String name, double value, boolean rangeIndex, boolean termIndex, boolean docValues, boolean stored) {
      super(name, getType(rangeIndex, termIndex, docValues, stored));
      fieldsData = NumericUtils.doubleToSortableLong(value);
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
      byte[] encodedPoint = new byte[Double.BYTES];
      double value = getValueAsDouble();
      DoublePoint.encodeDimension(value, encodedPoint, 0);
      return new BytesRef(encodedPoint);
    }

    private double getValueAsDouble() {
      return NumericUtils.sortableLongToDouble(numericValue().longValue());
    }

    @Override
    public StoredValue storedValue() {
      return storedValue;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " <" + name + ':' + getValueAsDouble() + '>';
    }

    @Override
    public void setDoubleValue(double value) {
      super.setLongValue(NumericUtils.doubleToSortableLong(value));
      if (storedValue != null) {
        storedValue.setDoubleValue(value);
      }
    }

    @Override
    public void setLongValue(long value) {
      throw new IllegalArgumentException("cannot change value type from Double to Long");
    }
  }
}
