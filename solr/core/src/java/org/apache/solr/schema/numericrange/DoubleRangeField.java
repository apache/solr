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
import java.util.regex.Pattern;
import org.apache.lucene.document.DoubleRange;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;

/**
 * Field type for double ranges with support for 1-4 dimensions.
 *
 * <p>This field type wraps Lucene's {@link DoubleRange} to provide storage and querying of
 * double-precision floating-point range values. Ranges can be 1-dimensional (simple ranges),
 * 2-dimensional (bounding boxes), 3-dimensional (bounding cubes), or 4-dimensional (tesseracts).
 *
 * <h2>Value Format</h2>
 *
 * Values are specified using bracket notation with a TO keyword separator:
 *
 * <ul>
 *   <li>1D: {@code [1.5 TO 2.5]}
 *   <li>2D: {@code [1.0,2.0 TO 3.0,4.0]}
 *   <li>3D: {@code [1.0,2.0,3.0 TO 4.0,5.0,6.0]}
 *   <li>4D: {@code [1.0,2.0,3.0,4.0 TO 5.0,6.0,7.0,8.0]}
 * </ul>
 *
 * As the name suggests minimum values (those on the left) must always be less than or equal to the
 * maximum value for the corresponding dimension. Integer values (e.g. {@code [10 TO 20]}) are also
 * accepted and parsed as doubles.
 *
 * <h2>Schema Configuration</h2>
 *
 * <pre>
 * &lt;fieldType name="doublerange" class="solr.DoubleRangeField" numDimensions="1"/&gt;
 * &lt;fieldType name="doublerange2d" class="solr.DoubleRangeField" numDimensions="2"/&gt;
 * &lt;field name="price_range" type="doublerange" indexed="true" stored="true"/&gt;
 * &lt;field name="my_2d_range" type="doublerange2d" indexed="true" stored="true"/&gt;
 * </pre>
 *
 * <h2>Querying</h2>
 *
 * Use the {@code numericRange} query parser for range queries with support for different query
 * types:
 *
 * <ul>
 *   <li>Intersects: {@code {!numericRange criteria="intersects" field=price_range}[1.0 TO 2.0]}
 *   <li>Within: {@code {!numericRange criteria="within" field=price_range}[0.0 TO 3.0]}
 *   <li>Contains: {@code {!numericRange criteria="contains" field=price_range}[1.5 TO 1.75]}
 *   <li>Crosses: {@code {!numericRange criteria="crosses" field=price_range}[1.5 TO 2.5]}
 * </ul>
 *
 * <h2>Limitations</h2>
 *
 * The main limitation of this field type is that it doesn't support docValues or uninversion, and
 * therefore can't be used for sorting, faceting, etc.
 *
 * @see DoubleRange
 * @see org.apache.solr.search.numericrange.NumericRangeQParserPlugin
 */
public class DoubleRangeField extends AbstractNumericRangeField {

  @Override
  protected Pattern getRangePattern() {
    return FP_RANGE_PATTERN_REGEX;
  }

  @Override
  protected Pattern getSingleBoundPattern() {
    return FP_SINGLE_BOUND_PATTERN;
  }

  @Override
  public IndexableField createField(SchemaField field, Object value) {
    if (!field.indexed() && !field.stored()) {
      return null;
    }

    String valueStr = value.toString();
    RangeValue rangeValue = parseRangeValue(valueStr);

    return new DoubleRange(field.getName(), rangeValue.mins, rangeValue.maxs);
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

    Matcher matcher = FP_RANGE_PATTERN_REGEX.matcher(value.trim());
    if (!matcher.matches()) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid range format. Expected: [min1,min2,... TO max1,max2,...] where min and max values are doubles, but got: "
              + value);
    }

    String minPart = matcher.group(1).trim();
    String maxPart = matcher.group(2).trim();

    double[] mins = parseDoubleArray(minPart, "min values");
    double[] maxs = parseDoubleArray(maxPart, "max values");

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
    final var singleBoundTyped = parseDoubleArray(value, "single bound values");
    return new RangeValue(singleBoundTyped, singleBoundTyped);
  }

  /**
   * Parse a comma-separated string of doubles into an array.
   *
   * @param str the string to parse
   * @param description description for error messages
   * @return array of parsed doubles
   */
  private double[] parseDoubleArray(String str, String description) {
    String[] parts = str.split(",");
    double[] result = new double[parts.length];

    for (int i = 0; i < parts.length; i++) {
      try {
        result[i] = Double.parseDouble(parts[i].trim());
      } catch (NumberFormatException e) {
        throw new SolrException(
            ErrorCode.BAD_REQUEST,
            "Invalid double in " + description + ": '" + parts[i].trim() + "'",
            e);
      }
    }

    return result;
  }

  @Override
  public Query newContainsQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return DoubleRange.newContainsQuery(fieldName, rv.mins, rv.maxs);
  }

  @Override
  public Query newIntersectsQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return DoubleRange.newIntersectsQuery(fieldName, rv.mins, rv.maxs);
  }

  @Override
  public Query newWithinQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return DoubleRange.newWithinQuery(fieldName, rv.mins, rv.maxs);
  }

  @Override
  public Query newCrossesQuery(String fieldName, NumericRangeValue rangeValue) {
    final var rv = (RangeValue) rangeValue;
    return DoubleRange.newCrossesQuery(fieldName, rv.mins, rv.maxs);
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

    // Parse the range bounds as single-dimensional double values
    double min, max;
    try {
      min = Double.parseDouble(part1.trim());
      max = Double.parseDouble(part2.trim());
    } catch (NumberFormatException e) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Invalid double values in range query: [" + part1 + " TO " + part2 + "]",
          e);
    }

    // For exclusive bounds, step to the next representable double value
    if (!minInclusive) {
      min = Math.nextUp(min);
    }
    if (!maxInclusive) {
      max = Math.nextDown(max);
    }

    // Build arrays for the query based on configured dimensions
    double[] mins = new double[numDimensions];
    double[] maxs = new double[numDimensions];

    // For now, only support 1D range syntax with field:[X TO Y]
    if (numDimensions == 1) {
      mins[0] = min;
      maxs[0] = max;
      return DoubleRange.newContainsQuery(field.getName(), mins, maxs);
    } else {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Standard range query syntax only supports 1D ranges. "
              + "Use {!numericRange ...} for multi-dimensional queries.");
    }
  }

  /** Simple holder class for parsed double range values. */
  public static class RangeValue implements AbstractNumericRangeField.NumericRangeValue {
    public final double[] mins;
    public final double[] maxs;

    public RangeValue(double[] mins, double[] maxs) {
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
