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

import java.util.regex.Matcher;
import org.apache.lucene.document.LongRange;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;

/**
 * Field type for long ranges with support for 1-4 dimensions.
 *
 * <p>This field type wraps Lucene's {@link LongRange} to provide storage and querying of long range
 * values. Ranges can be 1-dimensional (simple ranges), 2-dimensional (bounding boxes),
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
 * maximum value for the corresponding dimension. Long values outside the range of {@code int} are
 * fully supported (e.g. {@code [3000000000 TO 4000000000]}).
 *
 * <h2>Schema Configuration</h2>
 *
 * <pre>
 * &lt;fieldType name="longrange" class="solr.LongRangeField" numDimensions="1"/&gt;
 * &lt;fieldType name="longrange2d" class="solr.LongRangeField" numDimensions="2"/&gt;
 * &lt;field name="long_range" type="longrange" indexed="true" stored="true"/&gt;
 * &lt;field name="long_range_2d" type="longrange2d" indexed="true" stored="true"/&gt;
 * </pre>
 *
 * <h2>Querying</h2>
 *
 * Use the {@code numericRange} query parser for range queries with support for different query
 * types:
 *
 * <ul>
 *   <li>Intersects: {@code {!numericRange criteria="intersects" field=long_range}[100 TO 200]}
 *   <li>Within: {@code {!numericRange criteria="within" field=long_range}[0 TO 300]}
 *   <li>Contains: {@code {!numericRange criteria="contains" field=long_range}[150 TO 175]}
 *   <li>Crosses: {@code {!numericRange criteria="crosses" field=long_range}[150 TO 250]}
 * </ul>
 *
 * <h2>Limitations</h2>
 *
 * The main limitation of this field type is that it doesn't support docValues or uninversion, and
 * therefore can't be used for sorting, faceting, etc.
 *
 * @see LongRange
 * @see org.apache.solr.search.numericrange.NumericRangeQParserPlugin
 */
public class LongRangeField extends AbstractNumericRangeField {

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    if (!field.indexed() && !field.stored()) {
      return null;
    }

    String valueStr = value.toString();
    RangeValue rangeValue = parseRangeValue(valueStr);

    return new LongRange(field.getName(), rangeValue.mins, rangeValue.maxs);
  }

  /**
   * Parse a range value string into a RangeValue object.
   *
   * @param value the string value in format "[min1,min2,... TO max1,max2,...]"
   * @return parsed RangeValue
   * @throws SolrException if value format is invalid
   */
  @Override
  public RangeValue parseRangeValue(String value) {
    if (value == null || value.trim().isEmpty()) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Range value cannot be null or empty");
    }

    Matcher matcher = RANGE_PATTERN_REGEX.matcher(value.trim());
    if (!matcher.matches()) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid range format. Expected: [min1,min2,... TO max1,max2,...] where min and max values are longs, but got: "
              + value);
    }

    String minPart = matcher.group(1).trim();
    String maxPart = matcher.group(2).trim();

    long[] mins = parseLongArray(minPart, "min values");
    long[] maxs = parseLongArray(maxPart, "max values");

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

  @Override
  public NumericRangeValue parseSingleBound(String value) {
    final var singleBoundTyped = parseLongArray(value, "single bound values");
    return new RangeValue(singleBoundTyped, singleBoundTyped);
  }

  /**
   * Parse a comma-separated string of longs into an array.
   *
   * @param str the string to parse
   * @param description description for error messages
   * @return array of parsed longs
   */
  private long[] parseLongArray(String str, String description) {
    String[] parts = str.split(",");
    long[] result = new long[parts.length];

    for (int i = 0; i < parts.length; i++) {
      try {
        result[i] = Long.parseLong(parts[i].trim());
      } catch (NumberFormatException e) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Invalid long in " + description + ": '" + parts[i].trim() + "'",
            e);
      }
    }

    return result;
  }

  @Override
  public Query newContainsQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rangeValueTyped = (RangeValue) rangeValue;
    return LongRange.newContainsQuery(fieldName, rangeValueTyped.mins, rangeValueTyped.maxs);
  }

  @Override
  public Query newIntersectsQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return LongRange.newIntersectsQuery(fieldName, rv.mins, rv.maxs);
  }

  @Override
  public Query newWithinQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return LongRange.newWithinQuery(fieldName, rv.mins, rv.maxs);
  }

  @Override
  public Query newCrossesQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return LongRange.newCrossesQuery(fieldName, rv.mins, rv.maxs);
  }

  @Override
  protected Query getSpecializedRangeQuery(
      QParser parser,
      SchemaField field,
      String part1,
      String part2,
      boolean minInclusive,
      boolean maxInclusive) {
    // For standard range syntax field:[value TO value], default to contains query
    if (part1 == null || part2 == null) {
      return super.getSpecializedRangeQuery(
          parser, field, part1, part2, minInclusive, maxInclusive);
    }

    // Parse the range bounds as single-dimensional values
    long min, max;
    try {
      min = Long.parseLong(part1.trim());
      max = Long.parseLong(part2.trim());
    } catch (NumberFormatException e) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid long values in range query: [" + part1 + " TO " + part2 + "]",
          e);
    }

    if (!minInclusive) {
      min = (min == Long.MAX_VALUE) ? min : min + 1;
    }
    if (!maxInclusive) {
      max = (max == Long.MIN_VALUE) ? max : max - 1;
    }

    // Build arrays for the query based on configured dimensions
    long[] mins = new long[numDimensions];
    long[] maxs = new long[numDimensions];

    // For now, only support 1D range syntax with field:[X TO Y]
    if (numDimensions == 1) {
      mins[0] = min;
      maxs[0] = max;
      return LongRange.newContainsQuery(field.getName(), mins, maxs);
    } else {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Standard range query syntax only supports 1D ranges. "
              + "Use {!numericRange ...} for multi-dimensional queries.");
    }
  }

  /** Simple holder class for parsed long range values. */
  public static class RangeValue implements AbstractNumericRangeField.NumericRangeValue {
    public final long[] mins;
    public final long[] maxs;

    public RangeValue(long[] mins, long[] maxs) {
      this.mins = mins;
      this.maxs = maxs;
    }

    @Override
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
