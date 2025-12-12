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

import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedFloatFieldSource;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Selector;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * An {@code PointField} implementation of a field for {@code Float} values using {@code FloatPoint}, {@code StringField}, {@code NumericDocValuesField}, {@code SortedNumericDocValuesField} and {@code StoredField}.
 *
 * @see PointField
 * @see FloatPoint
 */
public class FloatField extends NumericField implements FloatValueFieldType {

  public FloatField() {
    type = NumberType.FLOAT;
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).floatValue();
    if (val instanceof CharSequence) return Float.parseFloat(val.toString());
    return super.toNativeType(val);
  }

  @Override
  public Query getPointFieldQuery(QParser parser, SchemaField field, String value) {
    return FloatPoint.newExactQuery(field.getName(), parseFloatFromUser(field.getName(), value));
  }

  @Override
  public Query getDocValuesFieldQuery(QParser parser, SchemaField field, String value) {
    return SortedNumericDocValuesField.newSlowExactQuery(field.getName(), NumericUtils.floatToSortableInt(parseFloatFromUser(field.getName(), value)));
  }

  @Override
  public Query getPointRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    float actualMin, actualMax;
    if (min == null) {
      actualMin = Float.NEGATIVE_INFINITY;
    } else {
      actualMin = parseFloatFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Float.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        actualMin = FloatPoint.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Float.POSITIVE_INFINITY;
    } else {
      actualMax = parseFloatFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Float.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        actualMax = FloatPoint.nextDown(actualMax);
      }
    }
    return FloatPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Query getDocValuesRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    float actualMin, actualMax;
    if (min == null) {
      actualMin = Float.NEGATIVE_INFINITY;
    } else {
      actualMin = parseFloatFromUser(field.getName(), min);
      if (!minInclusive) {
        if (actualMin == Float.POSITIVE_INFINITY) return new MatchNoDocsQuery();
        actualMin = FloatPoint.nextUp(actualMin);
      }
    }
    if (max == null) {
      actualMax = Float.POSITIVE_INFINITY;
    } else {
      actualMax = parseFloatFromUser(field.getName(), max);
      if (!maxInclusive) {
        if (actualMax == Float.NEGATIVE_INFINITY) return new MatchNoDocsQuery();
        actualMax = FloatPoint.nextDown(actualMax);
      }
    }
    return SortedNumericDocValuesField.newSlowRangeQuery(field.getName(), NumericUtils.floatToSortableInt(actualMin), NumericUtils.floatToSortableInt(actualMax));
  }

  @Override
  public Query getPointSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    float[] values = new float[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      values[i++] = parseFloatFromUser(field.getName(), val);
    }
    return FloatPoint.newSetQuery(field.getName(), values);
  }

  @Override
  public Query getDocValuesSetQuery(QParser parser, SchemaField field, Collection<String> externalVals) {
    long[] points = new long[externalVals.size()];
    int i = 0;
    for (String val : externalVals) {
      points[i++] = NumericUtils.floatToSortableInt(parseFloatFromUser(field.getName(), val));
    }
    return SortedNumericDocValuesField.newSlowSetQuery(field.getName(), points);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return FloatPoint.decodeDimension(term.bytes, term.offset);
  }

  @Override
  public Object toObject(IndexableField f) {
    final StoredValue storedValue = f.storedValue();
    if (storedValue != null) {
      return storedValue.getFloatValue();
    }
    final Number val = f.numericValue();
    if (val != null) {
      return NumericUtils.sortableIntToFloat(val.intValue());
    } else {
      throw new AssertionError("Unexpected state. Field: '" + f + "'");
    }
  }

  @Override
  public String storedToReadable(IndexableField f) {
    return Float.toString(f.storedValue().getFloatValue());
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return Float.toString(FloatPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Float.BYTES);
    result.setLength(Float.BYTES);
    FloatPoint.encodeDimension(parseFloatFromUser(null, val.toString()), result.bytes(), 0);
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
    return new MultiValuedFloatFieldSource(field.getName(), SortedNumericSelector.Type.MIN);
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField f) {
    return new MultiValuedFloatFieldSource(f.getName(), choice);
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value) {
    float floatValue =
        (value instanceof Number)
            ? ((Number) value).floatValue()
            : Float.parseFloat(value.toString());
    return Collections.singletonList(new SolrFloatField(sf.getName(), floatValue, sf.indexed(), sf.enhancedIndex(), sf.hasDocValues(), sf.stored()));
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    float floatValue =
        (value instanceof Number)
            ? ((Number) value).floatValue()
            : Float.parseFloat(value.toString());
    return new FloatPoint(field.getName(), floatValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Float) this.toNativeType(value));
  }

  @Override
  public Query getSpecializedExistenceQuery(QParser parser, SchemaField field) {
    return FloatPoint.newRangeQuery(field.getName(), Float.NEGATIVE_INFINITY, Float.NaN);
  }

  /**
   * Wrapper for {@link Float#parseFloat(String)} that throws a BAD_REQUEST error if the input is
   * not valid
   *
   * @param fieldName used in any exception, may be null
   * @param val string to parse, NPE if null
   */
  static float parseFloatFromUser(String fieldName, String val) {
    if (val == null) {
      throw new NullPointerException(
          "Invalid input" + (null == fieldName ? "" : " for field " + fieldName));
    }
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException e) {
      String msg = "Invalid Number: " + val + (null == fieldName ? "" : " for field " + fieldName);
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, msg);
    }
  }
}
