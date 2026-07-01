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
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TextField;

/**
 * A query parser that builds interval queries from a JSON DSL description. Invoked with the syntax
 * {@code {!intervals json_query=foobar df=title}}.
 *
 * <p>The {@code json_query} local param names an entry in the {@code json_queries} map (passed via
 * the JSON DSL). The top-level key of the named query must be a rule name (e.g., {@code match},
 * {@code all_of}). The target field is read from the {@code df} local param, falling back to the
 * {@code df} query param. Example: {@code {all_of: {...}}} with {@code df=title}.
 */
public class IntervalsQParserPlugin extends QParserPlugin {
  public static final String NAME = "intervals";
  private static final int DEFAULT_FUZZY_MAX_EXPANSIONS = Intervals.DEFAULT_MAX_EXPANSIONS;

  /** Local param that names the entry in {@code json_queries} to use. */
  public static final String JSON_QUERY_PARAM = "json_query";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new QParser(qstr, localParams, params, req) {
      @Override
      public Query parse() {
        String jsonQueryName = localParams.get(JSON_QUERY_PARAM);
        if (jsonQueryName == null) {
          return new MatchNoDocsQuery("No " + JSON_QUERY_PARAM + " parameter specified");
        }

        Map<String, Object> json = req.getJSON();
        if (json == null) {
          return new MatchNoDocsQuery("No JSON parameters found");
        }

        Object jsonQueriesObj = json.get("json_queries");
        if (!(jsonQueriesObj instanceof Map)) {
          return new MatchNoDocsQuery("No json_queries map found in JSON parameters");
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> jsonQueries = (Map<String, Object>) jsonQueriesObj;
        Object queryDef = jsonQueries.get(jsonQueryName);

        if (!(queryDef instanceof Map)) {
          return new MatchNoDocsQuery(
              "Query '" + jsonQueryName + "' not found in json_queries or is not a map");
        }

        Map<String, Object> queryDefMap = asStringObjectMap(queryDef, "json query definition");

        String field = getParam(CommonParams.DF);
        if (field == null || field.isEmpty()) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "json_query '" + jsonQueryName + "' requires a 'df' parameter to specify the field");
        }

        IntervalsSource source = parseRuleObject(queryDefMap, field);
        return new IntervalQuery(field, source);
      }

      private IntervalsSource parseRuleObject(Map<String, Object> ruleObject, String topField) {
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
          case "match" -> parseMatchRule(ruleParams, topField);
          case "prefix" -> parsePrefixRule(ruleParams, topField);
          case "wildcard" -> parseWildcardRule(ruleParams, topField);
          case "fuzzy" -> parseFuzzyRule(ruleParams, topField);
          case "all_of" -> parseAllOfRule(ruleParams, topField);
          case "any_of" -> parseAnyOfRule(ruleParams, topField);
          case "term" -> parseTermRule(ruleParams, topField);
          case "phrase" -> parsePhraseRule(ruleParams, topField);
          case "regexp" -> parseRegexpRule(ruleParams, topField);
          case "range" -> parseRangeRule(ruleParams, topField);
          case "max_width" -> parseMaxWidthRule(ruleParams, topField);
          case "extend" -> parseExtendRule(ruleParams, topField);
          case "unordered_no_overlaps" -> parseUnorderedNoOverlapsRule(ruleParams, topField);
          case "not_within" -> parseNotWithinRule(ruleParams, topField);
          case "within" -> parseWithinRule(ruleParams, topField);
          case "at_least" -> parseAtLeastRule(ruleParams, topField);
          case "no_intervals" -> parseNoIntervalsRule(ruleParams);
          default -> throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Unsupported intervals rule: " + ruleName);
        };
      }

      private IntervalsSource parseMatchRule(Map<String, Object> params, String topField) {
        String queryText = requireString(params, "query", "match");
        int maxGaps = getInt(params, "max_gaps", -1, "match");
        boolean ordered = getBoolean(params, "ordered", false, "match");
        String useField = getOptionalString(params, "use_field", "match");
        String analysisField = useField == null ? topField : useField;

        Analyzer analyzer = resolveAnalyzer(params, analysisField, "match");
        IntervalsSource source;
        try {
          source = Intervals.analyzedText(queryText, analyzer, analysisField, maxGaps, ordered);
        } catch (IOException e) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Failed to analyze match query text for field '" + analysisField + "'",
              e);
        }
        if (useField != null) {
          source = Intervals.fixField(useField, source);
        }
        return applyFilter(source, params.get("filter"), topField);
      }

      private IntervalsSource parsePrefixRule(Map<String, Object> params, String topField) {
        String prefix = requireString(params, "prefix", "prefix");
        String useField = getOptionalString(params, "use_field", "prefix");
        String field = useField == null ? topField : useField;
        Analyzer analyzer = resolveAnalyzer(params, field, "prefix");
        String normalizedPrefix = normalizeMultiTerm(field, prefix, analyzer);
        IntervalsSource source = Intervals.prefix(new BytesRef(normalizedPrefix));
        if (useField != null) {
          source = Intervals.fixField(useField, source);
        }
        return source;
      }

      private IntervalsSource parseWildcardRule(Map<String, Object> params, String topField) {
        String pattern = requireString(params, "pattern", "wildcard");
        String useField = getOptionalString(params, "use_field", "wildcard");
        String field = useField == null ? topField : useField;
        Analyzer analyzer = resolveAnalyzer(params, field, "wildcard");
        String normalizedPattern = normalizeMultiTerm(field, pattern, analyzer);
        IntervalsSource source = Intervals.wildcard(new BytesRef(normalizedPattern));
        if (useField != null) {
          source = Intervals.fixField(useField, source);
        }
        return source;
      }

      private IntervalsSource parseFuzzyRule(Map<String, Object> params, String topField) {
        String term = requireString(params, "term", "fuzzy");
        String useField = getOptionalString(params, "use_field", "fuzzy");
        String field = useField == null ? topField : useField;
        Analyzer analyzer = resolveAnalyzer(params, field, "fuzzy");
        String normalizedTerm = normalizeMultiTerm(field, term, analyzer);

        String fuzziness = getOptionalString(params, "fuzziness", "fuzzy");
        int maxEdits = resolveFuzziness(fuzziness, normalizedTerm);
        int prefixLength = getInt(params, "prefix_length", 0, "fuzzy");
        boolean transpositions = getBoolean(params, "transpositions", true, "fuzzy");

        IntervalsSource source =
            Intervals.fuzzyTerm(
                normalizedTerm,
                maxEdits,
                prefixLength,
                transpositions,
                DEFAULT_FUZZY_MAX_EXPANSIONS);
        if (useField != null) {
          source = Intervals.fixField(useField, source);
        }
        return source;
      }

      private IntervalsSource parseAllOfRule(Map<String, Object> params, String topField) {
        List<IntervalsSource> intervals = parseIntervalsArray(params, topField, "all_of");
        boolean ordered = getBoolean(params, "ordered", false, "all_of");
        int maxGaps = getInt(params, "max_gaps", -1, "all_of");

        IntervalsSource source =
            ordered
                ? Intervals.ordered(intervals.toArray(IntervalsSource[]::new))
                : Intervals.unordered(intervals.toArray(IntervalsSource[]::new));
        if (maxGaps >= 0) {
          source = Intervals.maxgaps(maxGaps, source);
        }
        return applyFilter(source, params.get("filter"), topField);
      }

      private IntervalsSource parseAnyOfRule(Map<String, Object> params, String topField) {
        List<IntervalsSource> intervals = parseIntervalsArray(params, topField, "any_of");
        IntervalsSource source = Intervals.or(intervals);
        return applyFilter(source, params.get("filter"), topField);
      }

      private IntervalsSource parseTermRule(Map<String, Object> params, String topField) {
        String value = requireString(params, "value", "term");
        String useField = getOptionalString(params, "use_field", "term");
        IntervalsSource source = Intervals.term(value);
        if (useField != null) {
          source = Intervals.fixField(useField, source);
        }
        return source;
      }

      private IntervalsSource parsePhraseRule(Map<String, Object> params, String topField) {
        Object termsObj = params.get("terms");
        Object intervalsObj = params.get("intervals");
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
          List<IntervalsSource> intervals = parseIntervalsArray(params, topField, "phrase");
          return Intervals.phrase(intervals.toArray(IntervalsSource[]::new));
        }
      }

      private IntervalsSource parseRegexpRule(Map<String, Object> params, String topField) {
        String pattern = requireString(params, "pattern", "regexp");
        String useField = getOptionalString(params, "use_field", "regexp");
        int maxExpansions =
            getInt(params, "max_expansions", Intervals.DEFAULT_MAX_EXPANSIONS, "regexp");
        IntervalsSource source = Intervals.regexp(new BytesRef(pattern), maxExpansions);
        if (useField != null) {
          source = Intervals.fixField(useField, source);
        }
        return source;
      }

      private IntervalsSource parseRangeRule(Map<String, Object> params, String topField) {
        String lowerTermStr = getOptionalString(params, "lower_term", "range");
        String upperTermStr = getOptionalString(params, "upper_term", "range");
        boolean includeLower = getBoolean(params, "include_lower", true, "range");
        boolean includeUpper = getBoolean(params, "include_upper", false, "range");
        int maxExpansions =
            getInt(params, "max_expansions", Intervals.DEFAULT_MAX_EXPANSIONS, "range");
        BytesRef lowerTerm = lowerTermStr == null ? null : new BytesRef(lowerTermStr);
        BytesRef upperTerm = upperTermStr == null ? null : new BytesRef(upperTermStr);
        return Intervals.range(lowerTerm, upperTerm, includeLower, includeUpper, maxExpansions);
      }

      private IntervalsSource parseMaxWidthRule(Map<String, Object> params, String topField) {
        int width = getInt(params, "width", -1, "max_width");
        if (width < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'max_width' requires a non-negative integer 'width'");
        }
        IntervalsSource source = parseNestedRule(params, "source", "max_width", topField);
        return Intervals.maxwidth(width, source);
      }

      private IntervalsSource parseExtendRule(Map<String, Object> params, String topField) {
        int before = getInt(params, "before", 0, "extend");
        int after = getInt(params, "after", 0, "extend");
        IntervalsSource source = parseNestedRule(params, "source", "extend", topField);
        return Intervals.extend(source, before, after);
      }

      private IntervalsSource parseUnorderedNoOverlapsRule(
          Map<String, Object> params, String topField) {
        List<IntervalsSource> intervals =
            parseIntervalsArray(params, topField, "unordered_no_overlaps");
        if (intervals.size() != 2) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'unordered_no_overlaps' requires exactly 2 intervals, got " + intervals.size());
        }
        return Intervals.unorderedNoOverlaps(intervals.get(0), intervals.get(1));
      }

      private IntervalsSource parseNotWithinRule(Map<String, Object> params, String topField) {
        IntervalsSource source = parseNestedRule(params, "source", "not_within", topField);
        int positions = getInt(params, "positions", -1, "not_within");
        if (positions < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'not_within' requires a non-negative integer 'positions'");
        }
        IntervalsSource reference = parseNestedRule(params, "reference", "not_within", topField);
        return Intervals.notWithin(source, positions, reference);
      }

      private IntervalsSource parseWithinRule(Map<String, Object> params, String topField) {
        IntervalsSource source = parseNestedRule(params, "source", "within", topField);
        int positions = getInt(params, "positions", -1, "within");
        if (positions < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'within' requires a non-negative integer 'positions'");
        }
        IntervalsSource reference = parseNestedRule(params, "reference", "within", topField);
        return Intervals.within(source, positions, reference);
      }

      private IntervalsSource parseAtLeastRule(Map<String, Object> params, String topField) {
        int minShouldMatch = getInt(params, "min_should_match", -1, "at_least");
        if (minShouldMatch < 0) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule 'at_least' requires a non-negative integer 'min_should_match'");
        }
        List<IntervalsSource> intervals = parseIntervalsArray(params, topField, "at_least");
        return Intervals.atLeast(minShouldMatch, intervals.toArray(IntervalsSource[]::new));
      }

      private IntervalsSource parseNoIntervalsRule(Map<String, Object> params) {
        String reason = getOptionalString(params, "reason", "no_intervals");
        return Intervals.noIntervals(reason == null ? "no_intervals rule" : reason);
      }

      private IntervalsSource parseNestedRule(
          Map<String, Object> params, String key, String ruleName, String topField) {
        Object nested = params.get(key);
        if (nested == null) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Rule '" + ruleName + "' requires '" + key + "' parameter");
        }
        return parseRuleObject(
            asStringObjectMap(nested, "'" + key + "' in rule '" + ruleName + "'"), topField);
      }

      private List<IntervalsSource> parseIntervalsArray(
          Map<String, Object> params, String topField, String ruleName) {
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
              parseRuleObject(asStringObjectMap(intervalObj, "intervals array element"), topField));
        }
        return parsed;
      }

      private IntervalsSource applyFilter(
          IntervalsSource source, Object filterObj, String topField) {
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
            parseRuleObject(asStringObjectMap(entry.getValue(), "filter '" + op + "'"), topField);
        return switch (op) {
          case "after" -> Intervals.after(source, other);
          case "before" -> Intervals.before(source, other);
          case "contained_by" -> Intervals.containedBy(source, other);
          case "containing" -> Intervals.containing(source, other);
          case "not_contained_by" -> Intervals.notContainedBy(source, other);
          case "not_containing" -> Intervals.notContaining(source, other);
          case "not_overlapping" -> Intervals.nonOverlapping(source, other);
          case "overlapping" -> Intervals.overlapping(source, other);
          default -> throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Unsupported filter operator: " + op);
        };
      }

      private Analyzer resolveAnalyzer(Map<String, Object> params, String field, String ruleName) {
        String analyzerName = getOptionalString(params, "analyzer", ruleName);
        if (analyzerName == null) {
          return req.getSchema().getQueryAnalyzer();
        }
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
        return fieldType.getQueryAnalyzer();
      }

      private String normalizeMultiTerm(String field, String term, Analyzer analyzer) {
        Analyzer effective = analyzer;
        if (effective == null) {
          FieldType fieldType = req.getSchema().getFieldTypeNoEx(field);
          if (fieldType instanceof TextField textField) {
            effective = textField.getMultiTermAnalyzer();
          }
        }
        if (effective == null) {
          return term;
        }
        BytesRef analyzed = TextField.analyzeMultiTerm(field, term, effective);
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
