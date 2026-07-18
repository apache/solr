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
package org.apache.solr.schema.numericrange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.RangeFieldQuery.QueryType;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.PrimitiveFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * Abstract base class for numeric range field types that wrap Lucene's multi-dimensional range
 * fields (e.g., {@link org.apache.lucene.document.IntRange}, {@link
 * org.apache.lucene.document.LongRange}).
 *
 * <p>Provides common infrastructure for range field types including:
 *
 * <ul>
 *   <li>Configurable number of dimensions (1–4) via the {@code numDimensions} schema attribute
 *   <li>Shared regex patterns for parsing range value strings
 *   <li>Standard field lifecycle methods (init, createFields, write, etc.)
 * </ul>
 *
 * <p>Concrete subclasses must implement {@link #parseRangeValue(String)} to parse the string
 * representation into a type-specific range value, and {@link #createField(SchemaField, Object)} to
 * produce the underlying Lucene {@link IndexableField}.
 *
 * @see IntRangeField
 * @see LongRangeField
 * @see FloatRangeField
 */
public abstract class AbstractNumericRangeField extends PrimitiveFieldType {

  /**
   * Marker interface for parsed range value objects. Implemented by the inner {@code RangeValue}
   * classes of concrete subclasses so that {@link #toNativeType(Object)} can identify already-
   * parsed values without knowing the concrete type.
   *
   * <p>Concrete subclasses override {@link #parseRangeValue(String)} with a covariant return type
   * so callers within the subclass receive the concrete type directly (e.g. {@code
   * IntRangeField.RangeValue}) with no casting required.
   */
  public interface NumericRangeValue {
    int getDimensions();
  }

  /** Regex fragment matching a comma-separated list of signed integers (no decimal points). */
  protected static final String COMMA_DELIMITED_NUMS = "-?\\d+(?:\\s*,\\s*-?\\d+)*";

  private static final String RANGE_PATTERN_STR =
      "\\[\\s*(" + COMMA_DELIMITED_NUMS + ")\\s+TO\\s+(" + COMMA_DELIMITED_NUMS + ")\\s*\\]";

  /** Pre-compiled pattern matching {@code [min1,min2,... TO max1,max2,...]} range syntax. */
  protected static final Pattern RANGE_PATTERN_REGEX = Pattern.compile(RANGE_PATTERN_STR);

  /** Pre-compiled pattern matching a single (multi-dimensional) bound, e.g. {@code 1,2,3}. */
  protected static final Pattern SINGLE_BOUND_PATTERN =
      Pattern.compile("^" + COMMA_DELIMITED_NUMS + "$");

  /**
   * Regex fragment matching a comma-separated list of signed floating-point numbers (integers,
   * floating-point literals, or values in scientific notation such as {@code 1.2e3} or {@code
   * -4.5E-6}).
   */
  protected static final String COMMA_DELIMITED_FP_NUMS =
      "-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?(?:\\s*,\\s*-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)*";

  private static final String FP_RANGE_PATTERN_STR =
      "\\[\\s*(" + COMMA_DELIMITED_FP_NUMS + ")\\s+TO\\s+(" + COMMA_DELIMITED_FP_NUMS + ")\\s*\\]";

  /**
   * Pre-compiled pattern matching {@code [min1,min2,... TO max1,max2,...]} range syntax where
   * values may be floating-point numbers.
   */
  protected static final Pattern FP_RANGE_PATTERN_REGEX = Pattern.compile(FP_RANGE_PATTERN_STR);

  /**
   * Pre-compiled pattern matching a single (multi-dimensional) floating-point bound, e.g. {@code
   * 1.5,2.0,3.14}.
   */
  protected static final Pattern FP_SINGLE_BOUND_PATTERN =
      Pattern.compile("^" + COMMA_DELIMITED_FP_NUMS + "$");

  /** Configured number of dimensions for this field type; defaults to 1. */
  protected int numDimensions = 1;

  /**
   * Returns the regex {@link Pattern} used to match a full range value string of the form {@code
   * [min TO max]}. Subclasses may override to use an alternative pattern (e.g. one that accepts
   * floating-point numbers).
   *
   * @return the range pattern for this field type
   */
  protected Pattern getRangePattern() {
    return RANGE_PATTERN_REGEX;
  }

  /**
   * Returns the regex {@link Pattern} used to match a single multi-dimensional bound (e.g. {@code
   * 1,2,3}). Subclasses may override to use an alternative pattern (e.g. one that accepts
   * floating-point numbers).
   *
   * @return the single-bound pattern for this field type
   */
  protected Pattern getSingleBoundPattern() {
    return SINGLE_BOUND_PATTERN;
  }

  @Override
  protected boolean enableDocValuesByDefault() {
    // DocValues are supported for both single- and multiValued range fields, but are opt-in: they
    // add an extra (binary) docValues field, and range docValues only help queries that combine
    // the range clause with a more selective clause, so we don't enable them by default.
    return false;
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

    // Force useDocValuesAsStored off; it otherwise defaults on for schema, which
    // would make fl=* responses fail on a docValues-enabled range field.
    properties &= ~USE_DOCVALUES_AS_STORED;

    String numDimensionsStr = args.remove("numDimensions");
    if (numDimensionsStr != null) {
      numDimensions = Integer.parseInt(numDimensionsStr);
      if (numDimensions < 1 || numDimensions > 4) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "numDimensions must be between 1 and 4, but was ["
                + numDimensions
                + "] for field type "
                + typeName);
      }
    }
  }

  @Override
  protected void checkSupportsDocValues() {
    // DocValues are supported for both single- and multiValued range fields (backed by binary
    // docValues).
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    List<IndexableField> fields = new ArrayList<>();

    IndexableField indexedField = createField(field, value);
    if (indexedField != null) {
      fields.add(indexedField);
    }

    if (field.hasDocValues() && !field.multiValued()) {
      // Single-valued: one flat BinaryDocValues blob (read directly, no dictionary). multiValued
      // docValues are built from all values at once in createFieldsFromAllValues (one blob holding
      // every range), since BinaryDocValues holds only one value per document.
      NumericRangeValue rv = parseRangeValue(value.toString());
      fields.add(
          new BinaryDocValuesField(
              field.getName(), BytesRef.deepCopyOf(encodePackedValue(field.getName(), rv))));
    }

    if (field.stored()) {
      fields.add(getStoredField(field, value.toString()));
    }

    return fields;
  }

  @Override
  public boolean createsFieldsFromAllValues() {
    // multiValued docValues range fields pack every range of a document into ONE BinaryDocValues
    // blob (flat, no dictionary), so the type needs all values together to build it.
    return true;
  }

  @Override
  public List<IndexableField> createFieldsFromAllValues(
      SchemaField field, Collection<Object> values) {
    List<IndexableField> fields = new ArrayList<>();
    // Indexed (BKD) and stored fields: one per value (same as the per-value path).
    for (Object value : values) {
      IndexableField indexedField = createField(field, value);
      if (indexedField != null) {
        fields.add(indexedField);
      }
      if (field.stored()) {
        fields.add(getStoredField(field, value.toString()));
      }
    }
    // docValues: a single BinaryDocValues value holding all of the document's ranges.
    if (field.hasDocValues()) {
      fields.add(
          new BinaryDocValuesField(field.getName(), encodePackedValues(field.getName(), values)));
    }
    return fields;
  }

  /**
   * Encodes several ranges into one {@code BinaryDocValues} blob: each range's fixed-width {@code
   * [min... | max...]} bytes concatenated. All ranges of a field share the same width, so the query
   * recovers the count as {@code blob.length / stride}.
   */
  protected BytesRef encodePackedValues(String field, Collection<Object> values) {
    BytesRefBuilder builder = new BytesRefBuilder();
    for (Object value : values) {
      builder.append(encodePackedValue(field, parseRangeValue(value.toString())));
    }
    return builder.toBytesRef();
  }

  /**
   * Encodes a range value into the packed {@code [min... | max...]} byte representation used by
   * both the indexed docValues and the query. Reuses Lucene's own encoder (via the type-specific
   * {@code *RangeDocValuesField}) so the docValues and BKD encodings stay identical.
   */
  protected abstract BytesRef encodePackedValue(String field, NumericRangeValue rangeValue);

  /** Number of bytes per dimension value for this type (e.g. {@code Integer.BYTES}). */
  protected abstract int bytesPerDimension();

  /**
   * If the field has docValues, wraps the BKD query in an {@link IndexOrDocValuesQuery} whose
   * docValues clause ({@link MultiBinaryRangeDocValuesQuery}) can cheaply verify candidates when a
   * more selective clause leads iteration; otherwise returns the BKD query unchanged. It reads a
   * flat per-doc blob of one (single-valued) or several (multiValued) ranges, avoiding the
   * dictionary/ordinal overhead of SortedSet docValues.
   */
  protected Query maybeWrapWithDocValues(
      SchemaField field, QueryType type, NumericRangeValue rangeValue, Query bkdQuery) {
    if (!field.hasDocValues()) {
      return bkdQuery;
    }
    BytesRef packed = encodePackedValue(field.getName(), rangeValue);
    byte[] queryPackedValue =
        Arrays.copyOfRange(packed.bytes, packed.offset, packed.offset + packed.length);
    Query dv =
        new MultiBinaryRangeDocValuesQuery(
            field.getName(),
            queryPackedValue,
            rangeValue.getDimensions(),
            bytesPerDimension(),
            type);
    if (!field.indexed()) {
      return dv;
    }
    return new IndexOrDocValuesQuery(bkdQuery, dv);
  }

  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), value.toString());
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), false);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(
        ErrorCode.BAD_REQUEST,
        "Cannot sort on " + getClass().getSimpleName() + ": " + field.getName());
  }

  @Override
  public Type getUninversionType(SchemaField sf) {
    return null; // No field cache support
  }

  @Override
  public String toInternal(String val) {
    // Validate format and return as-is
    parseRangeValue(val);
    return val;
  }

  @Override
  public String toExternal(IndexableField f) {
    return f.stringValue();
  }

  @Override
  public Object toNativeType(Object val) {
    if (val == null) return null;
    if (val instanceof NumericRangeValue) return val;
    return parseRangeValue(val.toString());
  }

  /**
   * Parse a range value string into a type-specific range value object.
   *
   * <p>Implementations should accept the {@code [min1,min2,... TO max1,max2,...]} bracket notation
   * (using {@link #RANGE_PATTERN_REGEX}) and validate that:
   *
   * <ul>
   *   <li>The format matches the expected pattern
   *   <li>The number of dimensions in the value matches {@link #numDimensions}
   *   <li>Each min value is less than or equal to the corresponding max value
   * </ul>
   *
   * <p>Subclasses should override this with a covariant return type (their concrete inner {@code
   * RangeValue} class) so that internal callers receive the fully-typed value without casting.
   *
   * @param value the string value in bracket notation
   * @return a {@link NumericRangeValue} holding the parsed min/max arrays
   * @throws SolrException if value format is invalid
   */
  public abstract NumericRangeValue parseRangeValue(String value);

  /**
   * Parses a single N-dimensional point expressed as a comma-separated string (e.g. {@code "5"} or
   * {@code "5,10"}) into a {@link NumericRangeValue} where both mins and maxs are set to the parsed
   * bound.
   *
   * <p>This is used by {@link #getFieldQuery} to support the "single bound" query shorthand, where
   * a bare coordinate is treated as a degenerate range {@code [p TO p]}. Dimension-count validation
   * against {@link #numDimensions} is performed by the caller and does not need to be repeated
   * here.
   *
   * <p>Subclasses should override with a covariant return type so that internal callers receive the
   * concrete {@code RangeValue} type without casting.
   *
   * @param value a comma-separated numeric string (e.g. {@code "5,10"} for a 2D point)
   * @return a {@link NumericRangeValue} with mins and maxs both equal to the parsed bound
   * @throws SolrException if the string contains non-numeric values
   */
  public abstract NumericRangeValue parseSingleBound(String value);

  /**
   * Creates a Lucene query that matches indexed documents whose stored range <em>contains</em> the
   * query range described by {@code rangeValue}.
   *
   * <p>This is the default query semantics used by {@link #getFieldQuery}. Queries with other match
   * semantics (intersects, within, crosses) are available via {@link
   * org.apache.solr.search.numericrange.NumericRangeQParserPlugin}.
   *
   * <p>The {@code rangeValue} argument may originate from either {@link #parseRangeValue} (full
   * {@code [min TO max]} syntax) or {@link #parseSingleBound} (point query shorthand). In the point
   * case, mins and maxs are equal, so the query finds documents whose range contains that exact
   * point.
   *
   * @param field the name of the field to query
   * @param rangeValue a pre-parsed range value produced by this field type
   * @return a contains query for the given field and range
   */
  public abstract Query newContainsQuery(SchemaField field, NumericRangeValue rangeValue);

  /**
   * Creates a Lucene query that matches indexed documents whose stored range <em>intersects</em>
   * the query range described by {@code rangeValue}.
   *
   * @param field the name of the field to query
   * @param rangeValue a pre-parsed range value produced by this field type
   * @return an intersects query for the given field and range
   */
  public abstract Query newIntersectsQuery(SchemaField field, NumericRangeValue rangeValue);

  /**
   * Creates a Lucene query that matches indexed documents whose stored range is <em>within</em> the
   * query range described by {@code rangeValue}.
   *
   * @param field the name of the field to query
   * @param rangeValue a pre-parsed range value produced by this field type
   * @return a within query for the given field and range
   */
  public abstract Query newWithinQuery(SchemaField field, NumericRangeValue rangeValue);

  /**
   * Creates a Lucene query that matches indexed documents whose stored range <em>crosses</em> the
   * boundaries of the query range described by {@code rangeValue}.
   *
   * @param field the name of the field to query
   * @param rangeValue a pre-parsed range value produced by this field type
   * @return a crosses query for the given field and range
   */
  public abstract Query newCrossesQuery(SchemaField field, NumericRangeValue rangeValue);

  /**
   * Creates a query for this field that matches docs where the query-range is fully contained by
   * the field value.
   *
   * <p>Queries requiring other match semantics can use {@link
   * org.apache.solr.search.numericrange.NumericRangeQParserPlugin}
   *
   * @param parser The {@link org.apache.solr.search.QParser} calling the method
   * @param field The {@link org.apache.solr.schema.SchemaField} of the field to search
   * @param externalVal The String representation of the value to search. Supports both a
   *     (multi-)dimensional range of the form [1,2 TO 3,4], or a single (multi-)dimensional bound
   *     (e.g. 1,2). In the latter case, the single bound will be used as both the min and max. Both
   *     formats use "contains" query semantics to find indexed ranges that contain the query range.
   * @return Query for this field using contains semantics
   */
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    if (externalVal == null || externalVal.trim().isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Query value cannot be null or empty");
    }

    String trimmed = externalVal.trim();

    // Check if it's the full range syntax: [min1,min2 TO max1,max2]
    if (getRangePattern().matcher(trimmed).matches()) {
      final var rangeValue = parseRangeValue(trimmed);
      return newContainsQuery(field, rangeValue);
    }

    // Syntax sugar: also accept a single-bound (i.e pX,pY,pZ)
    if (getSingleBoundPattern().matcher(trimmed).matches()) {
      final var singleBoundRange = parseSingleBound(trimmed);

      if (singleBoundRange.getDimensions() != numDimensions) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Single bound dimensions ("
                + singleBoundRange.getDimensions()
                + ") do not match field type numDimensions ("
                + numDimensions
                + ")");
      }

      // A single bound is the degenerate range [p,p]; "contains p" is equivalent to
      // "intersects [p,p]", so route it through the intersects path to pick up the docValues
      // optimization for point queries.
      return newIntersectsQuery(field, singleBoundRange);
    }

    throw new SolrException(
        ErrorCode.BAD_REQUEST,
        "Invalid query format. Expected either a range [min TO max] or a single bound to search for, got: "
            + externalVal);
  }
}
