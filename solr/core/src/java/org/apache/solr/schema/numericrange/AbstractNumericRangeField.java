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
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.PrimitiveFieldType;
import org.apache.solr.schema.SchemaField;
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
 * representation into a type-specific range value, and {@link #createField(SchemaField, Object)}
 * to produce the underlying Lucene {@link IndexableField}.
 *
 * @see IntRangeField
 * @see LongRangeField
 */
public abstract class AbstractNumericRangeField extends PrimitiveFieldType {

  /**
   * Marker interface for parsed range value objects. Implemented by the inner {@code RangeValue}
   * classes of concrete subclasses so that {@link #toNativeType(Object)} can identify already-
   * parsed values without knowing the concrete type.
   *
   * <p>Concrete subclasses override {@link #parseRangeValue(String)} with a covariant return type
   * so callers within the subclass receive the concrete type directly (e.g.
   * {@code IntRangeField.RangeValue}) with no casting required.
   */
  public interface NumericRangeValue {}

  /** Regex fragment matching a comma-separated list of signed integers (no decimal points). */
  protected static final String COMMA_DELIMITED_NUMS = "-?\\d+(?:\\s*,\\s*-?\\d+)*";

  private static final String RANGE_PATTERN_STR =
      "\\[\\s*(" + COMMA_DELIMITED_NUMS + ")\\s+TO\\s+(" + COMMA_DELIMITED_NUMS + ")\\s*\\]";

  /** Pre-compiled pattern matching {@code [min1,min2,... TO max1,max2,...]} range syntax. */
  protected static final Pattern RANGE_PATTERN_REGEX = Pattern.compile(RANGE_PATTERN_STR);

  /** Pre-compiled pattern matching a single (multi-dimensional) bound, e.g. {@code 1,2,3}. */
  protected static final Pattern SINGLE_BOUND_PATTERN =
      Pattern.compile("^" + COMMA_DELIMITED_NUMS + "$");

  /** Configured number of dimensions for this field type; defaults to 1. */
  protected int numDimensions = 1;

  @Override
  protected boolean enableDocValuesByDefault() {
    return false; // Range fields do not support docValues
  }

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);

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

    // Range fields do not support docValues - validate this wasn't explicitly enabled
    if (hasProperty(DOC_VALUES)) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "docValues=true enabled but "
              + getClass().getSimpleName()
              + " does not support docValues for field type "
              + typeName);
    }
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    IndexableField indexedField = createField(field, value);
    List<IndexableField> fields = new ArrayList<>();

    if (indexedField != null) {
      fields.add(indexedField);
    }

    if (field.stored()) {
      fields.add(getStoredField(field, value.toString()));
    }

    return fields;
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
}
