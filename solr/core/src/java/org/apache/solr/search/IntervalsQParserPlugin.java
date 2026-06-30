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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;

/**
 * A query parser that will eventually build interval queries from a JSON DSL description. Invoked
 * with the syntax {@code {!intervals json_query=foobar}}.
 *
 * <p>The {@code json_query} local param names an entry in the {@code json_queries} map (passed via
 * the JSON DSL) that describes the intervals to match. Processing of that map is not yet
 * implemented; this parser currently always returns {@link MatchNoDocsQuery}.
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
        // json_query names the json_queries entry to process – not yet implemented
        return new MatchNoDocsQuery();
      }
    };
  }
}
