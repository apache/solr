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

import java.util.Map;
import org.apache.lucene.queries.intervals.IntervalQuery;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A query parser that builds interval queries from a JSON DSL description. Invoked with the syntax
 * {@code {!intervals json_query=foobar}}.
 *
 * <p>The {@code json_query} local param names an entry in the {@code json_queries} map (passed via
 * the JSON DSL) that describes the intervals to match. The simplest supported form is a single
 * field-to-term mapping, e.g. {@code {field_name: "term_value"}}, which produces {@code new
 * IntervalQuery(field_name, Intervals.term(term_value))}.
 */
public class IntervalsQParserPlugin extends QParserPlugin {
  public static final String NAME = "intervals";

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

        @SuppressWarnings("unchecked")
        Map<String, Object> queryDefMap = (Map<String, Object>) queryDef;

        if (queryDefMap.size() != 1) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Expected exactly one {field: term} entry in json_query '"
                  + jsonQueryName
                  + "', got "
                  + queryDefMap.size());
        }

        Map.Entry<String, Object> entry = queryDefMap.entrySet().iterator().next();
        String field = entry.getKey();
        Object termValue = entry.getValue();

        if (!(termValue instanceof String)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Expected a string term value in json_query '"
                  + jsonQueryName
                  + "', got "
                  + (termValue == null ? "null" : termValue.getClass().getName()));
        }

        return new IntervalQuery(field, Intervals.term((String) termValue));
      }
    };
  }
}
