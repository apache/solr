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
package org.apache.solr.search.numericrange;

import java.util.Locale;
import org.apache.lucene.document.IntRange;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.numericrange.IntRangeField;
import org.apache.solr.schema.numericrange.IntRangeField.RangeValue;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SyntaxError;

/**
 * Query parser for IntRangeField with support for different query relationship types.
 *
 * <p>This parser enables queries against {@link IntRangeField} fields with explicit control over
 * the query relationship type (intersects, within, contains, crosses).
 *
 * <h2>Parameters</h2>
 *
 * <ul>
 *   <li><b>field</b> (required): The IntRangeField to query
 *   <li><b>criteria</b> (required): Query relationship criteria. One of: intersects, within,
 *       contains, crosses
 * </ul>
 *
 * <h2>Query Types</h2>
 *
 * <ul>
 *   <li><b>intersects</b>: Matches ranges that overlap with the query range (most permissive)
 *   <li><b>within</b>: Matches ranges completely contained by the query range
 *   <li><b>contains</b>: Matches ranges that completely contain the query range
 *   <li><b>crosses</b>: Matches ranges that cross the query range boundaries (not disjoint, not
 *       wholly contained)
 * </ul>
 *
 * <h2>Example Usage</h2>
 *
 * <pre>
 * // 1D range queries
 * {!numericRange criteria="intersects" field=price_range}[100 TO 200]
 * {!numericRange criteria="within" field=price_range}[0 TO 300]
 * {!numericRange criteria="contains" field=price_range}[150 TO 175]
 * {!numericRange criteria="crosses" field=price_range}[150 TO 250]
 *
 * // 2D range queries (bounding boxes)
 * {!numericRange criteria="intersects" field=bbox}[0,0 TO 10,10]
 * {!numericRange criteria="within" field=bbox}[-10,-10 TO 20,20]
 *
 * // 3D range queries (bounding cubes)
 * {!numericRange criteria="intersects" field=cube}[0,0,0 TO 10,10,10]
 *
 * // 4D range queries (tesseracts)
 * {!numericRange criteria="intersects" field=tesseract}[0,0,0,0 TO 10,10,10,10]
 * </pre>
 *
 * @see IntRangeField
 * @see IntRange
 * @lucene.experimental
 */
public class IntRangeQParserPlugin extends QParserPlugin {

  /** Query relationship criteria for range queries. */
  public enum QueryCriteria {
    /** Matches ranges that overlap with the query range (most permissive). */
    INTERSECTS("intersects"),

    /** Matches ranges completely contained by the query range. */
    WITHIN("within"),

    /** Matches ranges that completely contain the query range. */
    CONTAINS("contains"),

    /**
     * Matches ranges that cross the query range boundaries (not disjoint, not wholly contained).
     */
    CROSSES("crosses");

    private final String name;

    QueryCriteria(String name) {
      this.name = name;
    }

    /**
     * Parse a criteria string into a QueryCriteria enum value.
     *
     * @param criteriaStr the criteria string (case-insensitive)
     * @return the corresponding QueryCriteria
     * @throws SolrException if the criteria string is not recognized
     */
    public static QueryCriteria fromString(String criteriaStr) {
      if (criteriaStr == null || criteriaStr.trim().isEmpty()) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "Query criteria cannot be null or empty");
      }

      String normalized = criteriaStr.trim().toLowerCase(Locale.ROOT);
      for (QueryCriteria criteria : values()) {
        if (criteria.name.equals(normalized)) {
          return criteria;
        }
      }

      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "Unknown query criteria: '"
              + criteriaStr
              + "'. Valid criteria are: intersects, within, contains, crosses");
    }

    @Override
    public String toString() {
      return name;
    }
  }

  /** Parser name used in local params syntax: {@code {!numericRange ...}} */
  public static final String NAME = "numericRange";

  /** Parameter name for the field to query */
  public static final String FIELD_PARAM = "field";

  /** Parameter name for the query criteria (intersects, within, contains, crosses) */
  public static final String CRITERIA_PARAM = "criteria";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() throws SyntaxError {
        // Get required field parameter
        String fieldName = localParams.get(FIELD_PARAM);
        if (fieldName == null || fieldName.trim().isEmpty()) {
          throw new SolrException(
              ErrorCode.BAD_REQUEST, "Missing required parameter: " + FIELD_PARAM);
        }

        // Get required query criteria parameter and parse to enum
        String criteriaStr = localParams.get(CRITERIA_PARAM);
        if (criteriaStr == null || criteriaStr.trim().isEmpty()) {
          throw new SolrException(
              ErrorCode.BAD_REQUEST, "Missing required parameter: " + CRITERIA_PARAM);
        }
        QueryCriteria criteria = QueryCriteria.fromString(criteriaStr);

        // Get the range value from the query string or 'v' param
        String rangeValue = localParams.get(QueryParsing.V, qstr);
        if (rangeValue == null || rangeValue.trim().isEmpty()) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Range value cannot be empty");
        }

        // Validate field exists and is an IntRangeField
        SchemaField schemaField;
        try {
          schemaField = req.getSchema().getField(fieldName);
        } catch (SolrException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Field not found: " + fieldName, e);
        }

        if (!(schemaField.getType() instanceof IntRangeField)) {
          throw new SolrException(
              ErrorCode.BAD_REQUEST,
              "Field '"
                  + fieldName
                  + "' must be of type IntRangeField, but is: "
                  + schemaField.getType().getTypeName());
        }

        IntRangeField fieldType = (IntRangeField) schemaField.getType();

        // Parse the range value
        RangeValue range;
        try {
          range = fieldType.parseRangeValue(rangeValue);
        } catch (SolrException e) {
          throw new SolrException(ErrorCode.BAD_REQUEST, "Invalid range value: " + rangeValue, e);
        }

        // Create appropriate query based on criteria
        return createRangeQuery(fieldName, range.mins, range.maxs, criteria);
      }

      /**
       * Create the appropriate Lucene query based on the query criteria.
       *
       * @param fieldName the field to query
       * @param mins minimum values for each dimension
       * @param maxs maximum values for each dimension
       * @param criteria the query relationship criteria
       * @return the created Lucene Query
       */
      private Query createRangeQuery(
          String fieldName, int[] mins, int[] maxs, QueryCriteria criteria) {
        switch (criteria) {
          case INTERSECTS:
            return IntRange.newIntersectsQuery(fieldName, mins, maxs);

          case WITHIN:
            return IntRange.newWithinQuery(fieldName, mins, maxs);

          case CONTAINS:
            return IntRange.newContainsQuery(fieldName, mins, maxs);

          case CROSSES:
            return IntRange.newCrossesQuery(fieldName, mins, maxs);

          default:
            throw new AssertionError("Unhandled QueryCriteria: " + criteria);
        }
      }
    };
  }
}
