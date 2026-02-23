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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.PrimitiveFieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;

/**
 * Field type for integer ranges with support for 1-4 dimensions.
 *
 * <p>This field type wraps Lucene's {@link IntRange} to provide storage and querying of integer
 * range values. Ranges can be 1-dimensional (simple ranges), 2-dimensional (bounding boxes),
 * 3-dimensional (bounding cubes), or 4-dimensional (tesseracts).
 *
 * <h2>Value Format</h2>
 *
 * Values are specified using bracket notation with a TO keyword separator:
 *
 * <ul>
 *   <li>1D: {@code [10 TO 20]}
 *   <li>2D: {@code [10,20 TO 30,40]}
 *   <li>3D: {@code [10,20,30 TO 40,50,60]}
 *   <li>4D: {@code [10,20,30,40 TO 50,60,70,80]}
 * </ul>
 *
 * As the name suggests minimum values (those on the left) must always be less than or equal to the
 * maximum value for the corresponding dimension.
 *
 * <h2>Schema Configuration</h2>
 *
 * <pre>
 * &lt;fieldType name="intrange" class="org.apache.solr.schema.numericrange.IntRangeField" numDimensions="1"/&gt;
 * &lt;fieldType name="intrange2d" class="org.apache.solr.schema.numericrange.IntRangeField" numDimensions="2"/&gt;
 * &lt;field name="price_range" type="intrange" indexed="true" stored="true"/&gt;
 * &lt;field name="bbox" type="intrange2d" indexed="true" stored="true"/&gt;
 * </pre>
 *
 * <h2>Querying</h2>
 *
 * Use the {@code numericRange} query parser for range queries with support for different query
 * types:
 *
 * <ul>
 *   <li>Intersects: {@code {!numericRange criteria="intersects" field=price_range}[100 TO 200]}
 *   <li>Within: {@code {!numericRange criteria="within" field=price_range}[0 TO 300]}
 *   <li>Contains: {@code {!numericRange criteria="contains" field=price_range}[150 TO 175]}
 *   <li>Crosses: {@code {!numericRange criteria="crosses" field=price_range}[150 TO 250]}
 * </ul>
 *
 * <h2>Limitations</h2>
 *
 * The main limitation of this field type is that it doesn't support docValues or uninversion, and
 * therefore can't be used for sorting, faceting, etc.
 *
 * @see IntRange
 * @see org.apache.solr.search.numericrange.IntRangeQParserPlugin
 */
public class IntRangeField extends PrimitiveFieldType {

  private static final String COMMA_DELIMITED_INTS = "-?\\d+(?:\\s*,\\s*-?\\d+)*";
  private static final String RANGE_PATTERN =
      "\\[\\s*(" + COMMA_DELIMITED_INTS + ")\\s+TO\\s+(" + COMMA_DELIMITED_INTS + ")\\s*\\]";
  private static final Pattern RANGE_PATTERN_REGEX = Pattern.compile(RANGE_PATTERN);
  private static final Pattern SINGLE_BOUND_PATTERN =
      Pattern.compile("^" + COMMA_DELIMITED_INTS + "$");

  private int numDimensions = 1;

  @Override
  protected boolean enableDocValuesByDefault() {
    return false; // IntRange does not support docValues
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

    // IntRange does not support docValues - validate this wasn't explicitly set
    if (hasProperty(DOC_VALUES)) {
      throw new SolrException(
          ErrorCode.SERVER_ERROR,
          "docValues=true enabled but IntRangeField does not support docValues for field type "
              + typeName);
    }
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    if (!field.indexed() && !field.stored()) {
      return null;
    }

    String valueStr = value.toString();
    RangeValue rangeValue = parseRangeValue(valueStr);

    return new IntRange(field.getName(), rangeValue.mins, rangeValue.maxs);
  }

  @Override
  public List<IndexableField> createFields(SchemaField field, Object value) {
    IndexableField indexedField = createField(field, value);
    List<IndexableField> fields = new java.util.ArrayList<>();

    if (indexedField != null) {
      fields.add(indexedField);
    }

    if (field.stored()) {
      String valueStr = value.toString();
      fields.add(getStoredField(field, valueStr));
    }

    return fields;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeStr(name, toExternal(f), false);
  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    throw new SolrException(
        ErrorCode.BAD_REQUEST, "Cannot sort on IntRangeField: " + field.getName());
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
    if (val instanceof RangeValue) return val;
    return parseRangeValue(val.toString());
  }

  /**
   * Parse a range value string into a RangeValue object.
   *
   * @param value the string value in format "[min1,min2,... TO max1,max2,...]"
   * @return parsed RangeValue
   * @throws SolrException if value format is invalid
   */
  public RangeValue parseRangeValue(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range value cannot be null or empty");
    }

    Matcher matcher = RANGE_PATTERN_REGEX.matcher(value.trim());
    if (!matcher.matches()) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid range format. Expected: [min1,min2,... TO max1,max2,...] where min and max values are ints, but got: "
              + value);
    }

    String minPart = matcher.group(1).trim();
    String maxPart = matcher.group(2).trim();

    int[] mins = parseIntArray(minPart, "min values");
    int[] maxs = parseIntArray(maxPart, "max values");

    if (mins.length != maxs.length) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Min and max dimensions must match. Min dimensions: "
              + mins.length
              + ", max dimensions: "
              + maxs.length);
    }

    if (mins.length != numDimensions) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Range dimensions ("
              + mins.length
              + ") do not match field type numDimensions ("
              + numDimensions
              + ")");
    }

    // Validate that min <= max for each dimension
    for (int i = 0; i < mins.length; i++) {
      if (mins[i] > maxs[i]) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Min value must be <= max value for dimension "
                + i
                + ". Min: "
                + mins[i]
                + ", Max: "
                + maxs[i]);
      }
    }

    return new RangeValue(mins, maxs);
  }

  /**
   * Parse a comma-separated string of integers into an array.
   *
   * @param str the string to parse
   * @param description description for error messages
   * @return array of parsed integers
   */
  private int[] parseIntArray(String str, String description) {
    String[] parts = str.split(",");
    int[] result = new int[parts.length];

    for (int i = 0; i < parts.length; i++) {
      try {
        result[i] = Integer.parseInt(parts[i].trim());
      } catch (NumberFormatException e) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Invalid integer in " + description + ": '" + parts[i].trim() + "'",
            e);
      }
    }

    return result;
  }

  protected StoredField getStoredField(SchemaField sf, Object value) {
    return new StoredField(sf.getName(), value.toString());
  }

  /**
   * Creates a query for this field that matches docs where the query-range is fully contained by
   * the field value.
   *
   * <p>Queries requiring other match semantics can use {@link
   * org.apache.solr.search.numericrange.IntRangeQParserPlugin}
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
    if (RANGE_PATTERN_REGEX.matcher(trimmed).matches()) {
      RangeValue rangeValue = parseRangeValue(trimmed);
      return IntRange.newContainsQuery(field.getName(), rangeValue.mins, rangeValue.maxs);
    }

    // Syntax sugar: also accept a single-bound (i.e pX,pY,pZ)
    if (SINGLE_BOUND_PATTERN.matcher(trimmed).matches()) {
      int[] bound = parseIntArray(trimmed, "single bound values");

      if (bound.length != numDimensions) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Single bound dimensions ("
                + bound.length
                + ") do not match field type numDimensions ("
                + numDimensions
                + ")");
      }

      return IntRange.newContainsQuery(field.getName(), bound, bound);
    }

    throw new SolrException(
        ErrorCode.BAD_REQUEST,
        "Invalid query format. Expected either a range [min TO max] or a single bound to search for, got: "
            + externalVal);
  }

  @Override
  protected Query getSpecializedRangeQuery(
      QParser parser,
      SchemaField field,
      String part1,
      String part2,
      boolean minInclusive,
      boolean maxInclusive) {
    // For standard range syntax field:[value TO value], default to intersects query
    if (part1 == null || part2 == null) {
      return super.getSpecializedRangeQuery(
          parser, field, part1, part2, minInclusive, maxInclusive);
    }

    // Parse the range bounds as single-dimensional values
    int min, max;
    try {
      min = Integer.parseInt(part1.trim());
      max = Integer.parseInt(part2.trim());
    } catch (NumberFormatException e) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid integer values in range query: [" + part1 + " TO " + part2 + "]",
          e);
    }

    if (!minInclusive) {
      min = (min == Integer.MAX_VALUE) ? min : min + 1;
    }
    if (!maxInclusive) {
      max = (max == Integer.MIN_VALUE) ? max : max - 1;
    }

    // Build arrays for the query based on configured dimensions
    int[] mins = new int[numDimensions];
    int[] maxs = new int[numDimensions];

    // For now, only support 1D range syntax with field:[X TO Y]
    if (numDimensions == 1) {
      mins[0] = min;
      maxs[0] = max;
      return IntRange.newIntersectsQuery(field.getName(), mins, maxs);
    } else {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Standard range query syntax only supports 1D ranges. "
              + "Use {!numericRange ...} for multi-dimensional queries.");
    }
  }

  /** Simple holder class for parsed range values. */
  public static class RangeValue {
    public final int[] mins;
    public final int[] maxs;

    public RangeValue(int[] mins, int[] maxs) {
      this.mins = mins;
      this.maxs = maxs;
    }

    public int getDimensions() {
      return mins.length;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("[");
      for (int i = 0; i < mins.length; i++) {
        if (i > 0) sb.append(",");
        sb.append(mins[i]);
      }
      sb.append(" TO ");
      for (int i = 0; i < maxs.length; i++) {
        if (i > 0) sb.append(",");
        sb.append(maxs[i]);
      }
      sb.append("]");
      return sb.toString();
    }
  }
}
