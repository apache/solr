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
package org.apache.solr.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.json.JsonConsumerQParserPlugin;
import org.apache.solr.request.json.RequestUtil;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.schema.TextField;

/**
 * A query parser that builds interval queries from a JSON DSL description. Invoked with the syntax
 * {@code {!intervals }<JSON>}.
 */
public class IntervalsQParserPlugin extends QParserPlugin implements JsonConsumerQParserPlugin {
  public static final String NAME = "intervals";
  private static final int DEFAULT_FUZZY_MAX_EXPANSIONS = Intervals.DEFAULT_MAX_EXPANSIONS;

  /** Syntax reminder included in exceptions thrown for malformed/missing input. */
  private static final String SYNTAX_HELP =
      "Expected syntax '{!intervals}<JSON>'";
  private static final String ERROR_MSG = "Specify an intervals field with 'use_field' string property or via local param or a query param";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        if (qstr == null || qstr.isEmpty() || qstr.charAt(0) != '{') {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, SYNTAX_HELP);
        }
        Map<String, Object> queryDefMap = asStringObjectMap(Utils.fromJSONString(qstr), "intervals query");
        Object fieldVal = queryDefMap.remove("use_field");
        String field;
        if (fieldVal != null) {
          if (fieldVal instanceof String) {
            field = (String) fieldVal;
          } else {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ERROR_MSG);
          }
        } else {
          field = getParam(CommonParams.DF);
        }
        if (field == null || field.isEmpty()) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, ERROR_MSG);
        }

        SchemaField defaultField = req.getSchema().getField(field);

        IntervalsSource source = parseRuleObject(queryDefMap, defaultField);
        return new IntervalQuery(defaultField.getName(), source);
      }

      private IntervalsSource parseRuleObject(
          Map<String, Object> ruleObject, SchemaField defaultField) {
        if (ruleObject.size() != 1) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Each rule object must contain exactly one rule key, got " + ruleObject.keySet());
        }
        Map.Entry<String, Object> entry = ruleObject.entrySet().iterator().next();
        String ruleName = entry.getKey();
        Map<String, Object> ruleParams =
            asStringObjectMap(entry.getValue(), "rule '" + ruleName + "'");

        return switch (ruleName) {
          case "match" -> parseMatchRule(ruleParams, defaultField);
          case "prefix" -> parsePrefixRule(ruleParams, defaultField);
          case "wildcard" -> parseWildcardRule(ruleParams, defaultField);
          case "fuzzy" -> parseFuzzyRule(ruleParams, defaultField);
          case "all_of" -> parseAllOfRule(ruleParams, defaultField);
          case "any_of" -> parseAnyOfRule(ruleParams, defaultField);
          case "term" -> parseTermRule(ruleParams, defaultField);
          case "phrase" -> parsePhraseRule(ruleParams, defaultField);
          case "regexp" -> parseRegexpRule(ruleParams, defaultField);
          case "range" -> parseRangeRule(ruleParams, defaultField);
          case "max_width" -> parseMaxWidthRule(ruleParams, defaultField);
          case "extend" -> parseExtendRule(ruleParams, defaultField);
          case "unordered_no_overlaps" -> parseUnorderedNoOverlapsRule(ruleParams, defaultField);
          case "not_within" -> parseNotWithinRule(ruleParams, defaultField);
          case "within" -> parseWithinRule(ruleParams, defaultField);
          case "at_least" -> parseAtLeastRule(ruleParams, defaultField);
          case "no_intervals" -> parseNoIntervalsRule(ruleParams);
          default ->
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Unsupported intervals rule: " + ruleName);
        };
      }

      private IntervalsSource parseMatchRule(Map<String, Object> params, SchemaField defaultField) {
        String queryText = requireString(params, "query", "match");
        int maxGaps = getInt(params, "max_gaps", -1, "match");
        boolean ordered = getBoolean(params, "ordered", false, "match");
        String useField = getOptionalString(params, "use_field", "match");
        SchemaField analysisField = resolveField(useField, defaultField);

        Analyzer analyzer = resolveAnalyzer(params, analysisField, "match");
        IntervalsSource source;
        try {
          source =
              Intervals.analyzedText(
                  queryText, analyzer, analysisField.getName(), maxGaps, ordered);
        } catch (IOException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Failed to analyze match query text for field '" + analysisField.getName() + "'",
              e);
        }
        if (useField != null) {
          source = Intervals.fixField(analysisField.getName(), source);
        }
        return applyFilter(source, params.get("filter"), defaultField);
      }

      private IntervalsSource parsePrefixRule(
          Map<String, Object> params, SchemaField defaultField) {
        String prefix = requireString(params, "prefix", "prefix");
        String useField = getOptionalString(params, "use_field", "prefix");
        SchemaField field = resolveField(useField, defaultField);
        Analyzer analyzer = resolveMultiTermAnalyzer(params, field, "prefix");
        String normalizedPrefix = normalizeMultiTerm(field.getName(), prefix, analyzer);
        IntervalsSource source = Intervals.prefix(new BytesRef(normalizedPrefix));
        if (useField != null) {
          source = Intervals.fixField(field.getName(), source);
        }
        return source;
      }

      private IntervalsSource parseWildcardRule(
          Map<String, Object> params, SchemaField defaultField) {
        String pattern = requireString(params, "pattern", "wildcard");
        String useField = getOptionalString(params, "use_field", "wildcard");
        SchemaField field = resolveField(useField, defaultField);
        Analyzer analyzer = resolveMultiTermAnalyzer(params, field, "wildcard");
        String normalizedPattern = normalizeMultiTerm(field.getName(), pattern, analyzer);
        IntervalsSource source = Intervals.wildcard(new BytesRef(normalizedPattern));
        if (useField != null) {
          source = Intervals.fixField(field.getName(), source);
        }
        return source;
      }

      private IntervalsSource parseFuzzyRule(Map<String, Object> params, SchemaField defaultField) {
        String term = requireString(params, "term", "fuzzy");
        String useField = getOptionalString(params, "use_field", "fuzzy");
        SchemaField field = resolveField(useField, defaultField);
        Analyzer analyzer = resolveMultiTermAnalyzer(params, field, "fuzzy");
        String normalizedTerm = normalizeMultiTerm(field.getName(), term, analyzer);

        String fuzziness = getOptionalString(params, "fuzziness", "fuzzy");
        int maxEdits = resolveFuzziness(fuzziness, normalizedTerm);
        int prefixLength = getInt(params, "prefix_length", 0, "fuzzy");
        if (prefixLength < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'fuzzy' requires a non-negative integer 'prefix_length'");
        }
        boolean transpositions = getBoolean(params, "transpositions", true, "fuzzy");

        IntervalsSource source =
            Intervals.fuzzyTerm(
                normalizedTerm,
                maxEdits,
                prefixLength,
                transpositions,
                DEFAULT_FUZZY_MAX_EXPANSIONS);
        if (useField != null) {
          source = Intervals.fixField(field.getName(), source);
        }
        return source;
      }

      private IntervalsSource parseAllOfRule(Map<String, Object> params, SchemaField defaultField) {
        List<IntervalsSource> intervals = parseIntervalsArray(params, defaultField, "all_of");
        boolean ordered = getBoolean(params, "ordered", false, "all_of");
        int maxGaps = getInt(params, "max_gaps", -1, "all_of");

        IntervalsSource source =
            ordered
                ? Intervals.ordered(intervals.toArray(IntervalsSource[]::new))
                : Intervals.unordered(intervals.toArray(IntervalsSource[]::new));
        if (maxGaps >= 0) {
          source = Intervals.maxgaps(maxGaps, source);
        }
        return applyFilter(source, params.get("filter"), defaultField);
      }

      private IntervalsSource parseAnyOfRule(Map<String, Object> params, SchemaField defaultField) {
        List<IntervalsSource> intervals = parseIntervalsArray(params, defaultField, "any_of");
        IntervalsSource source = Intervals.or(intervals);
        return applyFilter(source, params.get("filter"), defaultField);
      }

      private IntervalsSource parseTermRule(Map<String, Object> params, SchemaField defaultField) {
        String value = requireString(params, "value", "term");
        String useField = getOptionalString(params, "use_field", "term");
        IntervalsSource source = Intervals.term(value);
        if (useField != null) {
          source = Intervals.fixField(resolveField(useField, defaultField).getName(), source);
        }
        return source;
      }

      private IntervalsSource parsePhraseRule(
          Map<String, Object> params, SchemaField defaultField) {
        Object termsObj = params.get("terms");
        Object intervalsObj = params.get("intervals");
        if (termsObj != null && intervalsObj != null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'phrase' cannot specify both 'terms' and 'intervals'");
        }
        if (termsObj == null && intervalsObj == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'phrase' requires either 'terms' (string array) or 'intervals' (rule array)");
        }
        if (termsObj != null) {
          if (!(termsObj instanceof List<?>)) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Rule 'phrase' requires 'terms' to be an array of strings");
          }
          List<?> rawTerms = (List<?>) termsObj;
          String[] terms = new String[rawTerms.size()];
          for (int i = 0; i < rawTerms.size(); i++) {
            Object t = rawTerms.get(i);
            if (!(t instanceof String)) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST,
                  "Rule 'phrase' requires all 'terms' elements to be strings, got "
                      + describeType(t));
            }
            terms[i] = (String) t;
          }
          return Intervals.phrase(terms);
        } else {
          List<IntervalsSource> intervals = parseIntervalsArray(params, defaultField, "phrase");
          return Intervals.phrase(intervals.toArray(IntervalsSource[]::new));
        }
      }

      private IntervalsSource parseRegexpRule(
          Map<String, Object> params, SchemaField defaultField) {
        String pattern = requireString(params, "pattern", "regexp");
        String useField = getOptionalString(params, "use_field", "regexp");
        int maxExpansions =
            getInt(params, "max_expansions", Intervals.DEFAULT_MAX_EXPANSIONS, "regexp");
        if (maxExpansions < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'regexp' requires a non-negative integer 'max_expansions', got "
                  + maxExpansions);
        }
        IntervalsSource source = Intervals.regexp(new BytesRef(pattern), maxExpansions);
        if (useField != null) {
          source = Intervals.fixField(resolveField(useField, defaultField).getName(), source);
        }
        return source;
      }

      private IntervalsSource parseRangeRule(Map<String, Object> params, SchemaField defaultField) {
        String lowerTermStr = getOptionalString(params, "lower_term", "range");
        String upperTermStr = getOptionalString(params, "upper_term", "range");
        boolean includeLower = getBoolean(params, "include_lower", true, "range");
        boolean includeUpper = getBoolean(params, "include_upper", false, "range");
        int maxExpansions =
            getInt(params, "max_expansions", Intervals.DEFAULT_MAX_EXPANSIONS, "range");
        if (maxExpansions < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'range' requires a non-negative integer 'max_expansions', got "
                  + maxExpansions);
        }
        BytesRef lowerTerm = lowerTermStr == null ? null : new BytesRef(lowerTermStr);
        BytesRef upperTerm = upperTermStr == null ? null : new BytesRef(upperTermStr);
        return Intervals.range(lowerTerm, upperTerm, includeLower, includeUpper, maxExpansions);
      }

      private IntervalsSource parseMaxWidthRule(
          Map<String, Object> params, SchemaField defaultField) {
        int width = getInt(params, "width", -1, "max_width");
        if (width < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'max_width' requires a non-negative integer 'width'");
        }
        IntervalsSource source = parseNestedRule(params, "source", "max_width", defaultField);
        return Intervals.maxwidth(width, source);
      }

      private IntervalsSource parseExtendRule(
          Map<String, Object> params, SchemaField defaultField) {
        int before = getInt(params, "before", 0, "extend");
        int after = getInt(params, "after", 0, "extend");
        IntervalsSource source = parseNestedRule(params, "source", "extend", defaultField);
        return Intervals.extend(source, before, after);
      }

      private IntervalsSource parseUnorderedNoOverlapsRule(
          Map<String, Object> params, SchemaField defaultField) {
        List<IntervalsSource> intervals =
            parseIntervalsArray(params, defaultField, "unordered_no_overlaps");
        if (intervals.size() != 2) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'unordered_no_overlaps' requires exactly 2 intervals, got " + intervals.size());
        }
        return Intervals.unorderedNoOverlaps(intervals.get(0), intervals.get(1));
      }

      private IntervalsSource parseNotWithinRule(
          Map<String, Object> params, SchemaField defaultField) {
        IntervalsSource source = parseNestedRule(params, "source", "not_within", defaultField);
        int positions = getInt(params, "positions", -1, "not_within");
        if (positions < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'not_within' requires a non-negative integer 'positions'");
        }
        IntervalsSource reference =
            parseNestedRule(params, "reference", "not_within", defaultField);
        return Intervals.notWithin(source, positions, reference);
      }

      private IntervalsSource parseWithinRule(
          Map<String, Object> params, SchemaField defaultField) {
        IntervalsSource source = parseNestedRule(params, "source", "within", defaultField);
        int positions = getInt(params, "positions", -1, "within");
        if (positions < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'within' requires a non-negative integer 'positions'");
        }
        IntervalsSource reference = parseNestedRule(params, "reference", "within", defaultField);
        return Intervals.within(source, positions, reference);
      }

      private IntervalsSource parseAtLeastRule(
          Map<String, Object> params, SchemaField defaultField) {
        int minShouldMatch = getInt(params, "min_should_match", -1, "at_least");
        if (minShouldMatch < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'at_least' requires a non-negative integer 'min_should_match'");
        }
        List<IntervalsSource> intervals = parseIntervalsArray(params, defaultField, "at_least");
        return Intervals.atLeast(minShouldMatch, intervals.toArray(IntervalsSource[]::new));
      }

      private IntervalsSource parseNoIntervalsRule(Map<String, Object> params) {
        String reason = getOptionalString(params, "reason", "no_intervals");
        return Intervals.noIntervals(reason == null ? "no_intervals rule" : reason);
      }

      private IntervalsSource parseNestedRule(
          Map<String, Object> params, String key, String ruleName, SchemaField defaultField) {
        Object nested = params.get(key);
        if (nested == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule '" + ruleName + "' requires '" + key + "' parameter");
        }
        return parseRuleObject(
            asStringObjectMap(nested, "'" + key + "' in rule '" + ruleName + "'"), defaultField);
      }

      private List<IntervalsSource> parseIntervalsArray(
          Map<String, Object> params, SchemaField defaultField, String ruleName) {
        Object intervalsObj = params.get("intervals");
        if (!(intervalsObj instanceof List<?>)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule '" + ruleName + "' requires an 'intervals' array");
        }
        List<?> rawIntervals = (List<?>) intervalsObj;
        if (rawIntervals.isEmpty()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule '" + ruleName + "' requires at least one interval rule");
        }
        List<IntervalsSource> parsed = new ArrayList<>(rawIntervals.size());
        for (Object intervalObj : rawIntervals) {
          parsed.add(
              parseRuleObject(
                  asStringObjectMap(intervalObj, "intervals array element"), defaultField));
        }
        return parsed;
      }

      private IntervalsSource applyFilter(
          IntervalsSource source, Object filterObj, SchemaField defaultField) {
        if (filterObj == null) {
          return source;
        }
        Map<String, Object> filterMap = asStringObjectMap(filterObj, "filter");
        if (filterMap.size() != 1) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Filter must contain exactly one operator, got " + filterMap.keySet());
        }

        Map.Entry<String, Object> entry = filterMap.entrySet().iterator().next();
        String op = entry.getKey();
        if ("script".equals(op)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Filter operator 'script' is not supported");
        }
        IntervalsSource other =
            parseRuleObject(
                asStringObjectMap(entry.getValue(), "filter '" + op + "'"), defaultField);
        return switch (op) {
          case "after" -> Intervals.after(source, other);
          case "before" -> Intervals.before(source, other);
          case "contained_by" -> Intervals.containedBy(source, other);
          case "containing" -> Intervals.containing(source, other);
          case "not_contained_by" -> Intervals.notContainedBy(source, other);
          case "not_containing" -> Intervals.notContaining(source, other);
          case "not_overlapping" -> Intervals.nonOverlapping(source, other);
          case "overlapping" -> Intervals.overlapping(source, other);
          default ->
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Unsupported filter operator: " + op);
        };
      }

      /**
       * Resolves the field referenced by an optional {@code use_field} rule parameter, falling back
       * to the query's default field when absent. Throws BAD_REQUEST if {@code useField} names a
       * field that doesn't exist in the schema.
       */
      private SchemaField resolveField(String useField, SchemaField defaultField) {
        return useField == null ? defaultField : req.getSchema().getField(useField);
      }

      private Analyzer resolveAnalyzer(
          Map<String, Object> params, SchemaField field, String ruleName) {
        String analyzerName = getOptionalString(params, "analyzer", ruleName);
        if (analyzerName == null) {
          return field.getType().getQueryAnalyzer();
        }
        return resolveFieldType(analyzerName, ruleName).getQueryAnalyzer();
      }

      /**
       * Resolves the analyzer to use for normalizing prefix/wildcard/fuzzy term text. Unlike {@link
       * #resolveAnalyzer}, this uses the field type's multi-term analyzer rather than its regular
       * query analyzer, since the term text here is a single already-tokenized value, not free text
       * to be tokenized.
       */
      private Analyzer resolveMultiTermAnalyzer(
          Map<String, Object> params, SchemaField field, String ruleName) {
        String analyzerName = getOptionalString(params, "analyzer", ruleName);
        FieldType fieldType =
            analyzerName == null ? field.getType() : resolveFieldType(analyzerName, ruleName);
        if (fieldType instanceof TextField textField) {
          return textField.getMultiTermAnalyzer();
        }
        return null;
      }

      private FieldType resolveFieldType(String analyzerName, String ruleName) {
        FieldType fieldType = req.getSchema().getFieldTypeByName(analyzerName);
        if (fieldType == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Unknown analyzer '"
                  + analyzerName
                  + "' for rule '"
                  + ruleName
                  + "'. In Solr this value must match a field type name.");
        }
        return fieldType;
      }

      private String normalizeMultiTerm(String field, String term, Analyzer analyzer) {
        if (analyzer == null) {
          return term;
        }
        BytesRef analyzed = TextField.analyzeMultiTerm(field, term, analyzer);
        return analyzed == null ? term : analyzed.utf8ToString();
      }

      private int resolveFuzziness(String fuzziness, String term) {
        if (fuzziness == null || "AUTO".equals(fuzziness)) {
          return resolveAutoFuzziness(term, 3, 6);
        }
        if (fuzziness.startsWith("AUTO:")) {
          String thresholds = fuzziness.substring("AUTO:".length());
          String[] parts = thresholds.split(",");
          if (parts.length != 2) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Invalid fuzziness value: " + fuzziness + ". Expected AUTO:<low>,<high>");
          }
          int low;
          int high;
          try {
            low = Integer.parseInt(parts[0].trim());
            high = Integer.parseInt(parts[1].trim());
          } catch (NumberFormatException e) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Invalid fuzziness value: " + fuzziness + ". Expected AUTO:<low>,<high>",
                e);
          }
          return resolveAutoFuzziness(term, low, high);
        }
        try {
          int edits = Integer.parseInt(fuzziness);
          if (edits < 0 || edits > 2) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "fuzziness must be between 0 and 2, got " + edits);
          }
          return edits;
        } catch (NumberFormatException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Invalid fuzziness value: " + fuzziness, e);
        }
      }

      private int resolveAutoFuzziness(String term, int low, int high) {
        int length = term.codePointCount(0, term.length());
        if (length < low) {
          return 0;
        }
        if (length < high) {
          return 1;
        }
        return 2;
      }

      private Map<String, Object> asStringObjectMap(Object obj, String context) {
        if (!(obj instanceof Map<?, ?> mapObj)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Expected object for " + context + ", got " + describeType(obj));
        }
        List<String> badKeys = new ArrayList<>();
        for (Object key : mapObj.keySet()) {
          if (!(key instanceof String)) {
            badKeys.add(String.valueOf(key));
          }
        }
        if (!badKeys.isEmpty()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Expected string keys for " + context + ", got keys " + badKeys);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> casted = (Map<String, Object>) mapObj;
        return casted;
      }

      private String requireString(Map<String, Object> map, String key, String context) {
        Object val = map.get(key);
        if (!(val instanceof String)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule '"
                  + context
                  + "' requires string parameter '"
                  + key
                  + "', got "
                  + describeType(val));
        }
        return (String) val;
      }

      private String getOptionalString(Map<String, Object> map, String key, String context) {
        Object val = map.get(key);
        if (val == null) {
          return null;
        }
        if (!(val instanceof String)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule '"
                  + context
                  + "' expects string parameter '"
                  + key
                  + "', got "
                  + describeType(val));
        }
        return (String) val;
      }

      private boolean getBoolean(
          Map<String, Object> map, String key, boolean defaultValue, String context) {
        Object val = map.get(key);
        if (val == null) {
          return defaultValue;
        }
        if (val instanceof Boolean b) {
          return b;
        }
        if (val instanceof String s) {
          return Boolean.parseBoolean(s);
        }
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Rule '"
                + context
                + "' expects boolean parameter '"
                + key
                + "', got "
                + describeType(val));
      }

      private int getInt(Map<String, Object> map, String key, int defaultValue, String context) {
        Object val = map.get(key);
        if (val == null) {
          return defaultValue;
        }
        if (val instanceof Number n) {
          return n.intValue();
        }
        if (val instanceof String s) {
          try {
            return Integer.parseInt(s);
          } catch (NumberFormatException e) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "Rule '" + context + "' expects integer parameter '" + key + "', got " + s,
                e);
          }
        }
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Rule '"
                + context
                + "' expects integer parameter '"
                + key
                + "', got "
                + describeType(val));
      }

      private String describeType(Object value) {
        return value == null ? "null" : value.getClass().getName();
      }
    };
  }
}
