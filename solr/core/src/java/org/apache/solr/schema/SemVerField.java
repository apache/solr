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
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.queries.function.valuesource.MultiValuedLongFieldSource;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.search.QParser;

/**
 * A field type for semantic versioning strings with up to 6 dot-separated parts (e.g. "2.3.1").
 * Each part must be in the range [0, 999].
 *
 * <p>Internally stored as a {@code long} by encoding each part as: {@code part[i] * 1000^(5-i)}.
 * For example, "2.3.1" encodes as {@code 2*1000^5 + 3*1000^4 + 1*1000^3 = 2_003_001_000_000}.
 *
 * <p>This encoding preserves version ordering, so range queries, sorting, and comparisons all work
 * naturally. Missing trailing parts are treated as zero, so "2.3" and "2.3.0" are equivalent.
 */
public class SemVerField extends LongPointField {

  static final int MAX_PARTS = 6;
  static final long PART_MULTIPLIER = 1000L;
  static final int MAX_PART_VALUE = 999;

  static final long[] POWERS = new long[MAX_PARTS];

  static {
    POWERS[MAX_PARTS - 1] = 1L;
    for (int i = MAX_PARTS - 2; i >= 0; i--) {
      POWERS[i] = POWERS[i + 1] * PART_MULTIPLIER;
    }
  }

  public static String decodeDocValue(long value) {
    return longToSemVer(value);
  }

  static long parseSemVer(String semver) {
    if (semver == null || semver.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Empty semver value");
    }
    String[] parts = semver.split("\\.", -1);
    if (parts.length < 1 || parts.length > MAX_PARTS) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Invalid semver '" + semver + "': must have 1 to " + MAX_PARTS + " dot-separated parts");
    }
    long result = 0;
    for (int i = 0; i < parts.length; i++) {
      int val;
      try {
        val = Integer.parseInt(parts[i]);
      } catch (NumberFormatException e) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Invalid semver part '" + parts[i] + "' in '" + semver + "'");
      }
      if (val < 0 || val > MAX_PART_VALUE) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Semver part " + val + " out of range [0, " + MAX_PART_VALUE + "] in '" + semver + "'");
      }
      result += val * POWERS[i];
    }
    return result;
  }

  static String longToSemVer(long value) {
    if (value < 0) {
      return Long.toString(value);
    }
    int[] parts = new int[MAX_PARTS];
    long remaining = value;
    for (int i = 0; i < MAX_PARTS; i++) {
      parts[i] = (int) (remaining / POWERS[i]);
      remaining %= POWERS[i];
    }
    int lastNonZero = 0;
    for (int i = MAX_PARTS - 1; i > 0; i--) {
      if (parts[i] != 0) {
        lastNonZero = i;
        break;
      }
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i <= lastNonZero; i++) {
      if (i > 0) sb.append('.');
      sb.append(parts[i]);
    }
    return sb.toString();
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof Number) return ((Number) val).longValue();
    if (val instanceof CharSequence) return parseSemVer(val.toString());
    return super.toNativeType(val);
  }

  @Override
  protected Query getDocValuesRangeQuery(
      QParser parser,
      SchemaField field,
      String min,
      String max,
      boolean minInclusive,
      boolean maxInclusive) {
    return numericDocValuesRangeQuery(
        field.getName(),
        min == null ? null : parseSemVer(min),
        max == null ? null : parseSemVer(max),
        minInclusive,
        maxInclusive,
        field.multiValued());
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
      actualMin = parseSemVer(min);
      if (!minInclusive) {
        if (actualMin == Long.MAX_VALUE) return new MatchNoDocsQuery();
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Long.MAX_VALUE;
    } else {
      actualMax = parseSemVer(max);
      if (!maxInclusive) {
        if (actualMax == Long.MIN_VALUE) return new MatchNoDocsQuery();
        actualMax--;
      }
    }
    return LongPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return longToSemVer(LongPoint.decodeDimension(term.bytes, term.offset));
  }

  @Override
  public Object toObject(IndexableField f) {
    final Number val = f.numericValue();
    if (val != null) {
      return longToSemVer(val.longValue());
    }
    throw new AssertionError("Unexpected state. Field: '" + f + "'");
  }

  @Override
  protected Query getExactQuery(SchemaField field, String externalVal) {
    return LongPoint.newExactQuery(field.getName(), parseSemVer(externalVal));
  }

  @Override
  public Query getSetQuery(QParser parser, SchemaField field, Collection<String> externalVal) {
    assert externalVal.size() > 0;
    if (!field.indexed()) {
      return super.getSetQuery(parser, field, externalVal);
    }
    long[] values = new long[externalVal.size()];
    int i = 0;
    for (String val : externalVal) {
      values[i++] = parseSemVer(val);
    }
    if (field.hasDocValues()) {
      return LongField.newSetQuery(field.getName(), values);
    } else {
      return LongPoint.newSetQuery(field.getName(), values);
    }
  }

  @Override
  protected String indexedToReadable(BytesRef indexedForm) {
    return longToSemVer(LongPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
  }

  @Override
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Long.BYTES);
    result.setLength(Long.BYTES);
    LongPoint.encodeDimension(parseSemVer(val.toString()), result.bytes(), 0);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new SemVerFieldSource(field.getName());
  }

  @Override
  protected ValueSource getSingleValueSource(SortedNumericSelector.Type choice, SchemaField field) {
    return new MultiValuedLongFieldSource(field.getName(), choice);
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    long longValue =
        (value instanceof Number) ? ((Number) value).longValue() : parseSemVer(value.toString());
    return new LongPoint(field.getName(), longValue);
  }

  @Override
  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), (Long) this.toNativeType(value));
  }

  private static class SemVerFieldSource extends LongFieldSource {

    public SemVerFieldSource(String field) {
      super(field);
    }

    @Override
    public String description() {
      return "semver(" + field + ')';
    }

    @Override
    public String longToString(long val) {
      return longToSemVer(val);
    }

    @Override
    public long externalToLong(String extVal) {
      return parseSemVer(extVal);
    }
  }
}
