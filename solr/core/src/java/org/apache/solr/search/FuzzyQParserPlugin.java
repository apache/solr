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

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.surround.parser.QueryParser;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.TextField;

/**
 * Plugin for parsing and creating Lucene {@link FuzzyQuery}s with all customizations available. All
 * custom options, <code>maxEdits</code>, <code>prefixLength</code>, <code>maxExpansions</code>, and
 * <code>transpositions</code> are optional and use the {@link FuzzyQuery} defaults if not provided.
 *
 * <p>Example: <code>{!fuzzy f=myfield maxEdits=1 prefixLength=3 maxExpansions=2}foobar</code>
 *
 * @see QueryParser
 * @since 9.9
 */
public class FuzzyQParserPlugin extends QParserPlugin {
  public static final String NAME = "fuzzy";

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return new FuzzyQParser(qstr, localParams, params, req);
  }

  static class FuzzyQParser extends QParser {
    static final String MAX_EDITS_PARAM = "maxEdits";
    static final String PREFIX_LENGTH_PARAM = "prefixLength";
    static final String MAX_EXPANSIONS_PARAM = "maxExpansions";
    static final String TRANSPOSITIONS_PARAM = "transpositions";

    public FuzzyQParser(
        String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
      super(qstr, localParams, params, req);
    }

    @Override
    public Query parse() throws SyntaxError {
      String termStr = getParam(QueryParsing.V);
      String field = getParam(QueryParsing.F);
      termStr = analyzeIfMultitermTermText(field, termStr);
      Term t = new Term(field, termStr);

      String maxEditsRaw = getParam(MAX_EDITS_PARAM);
      int maxEdits =
          (maxEditsRaw != null) ? Integer.parseInt(maxEditsRaw) : FuzzyQuery.defaultMaxEdits;
      String prefixLengthRaw = getParam(PREFIX_LENGTH_PARAM);
      int prefixLength =
          (prefixLengthRaw != null)
              ? Integer.parseInt(prefixLengthRaw)
              : FuzzyQuery.defaultPrefixLength;
      String maxExpansionsRaw = getParam(MAX_EXPANSIONS_PARAM);
      int maxExpansions =
          (maxExpansionsRaw != null)
              ? Integer.parseInt(maxExpansionsRaw)
              : FuzzyQuery.defaultMaxExpansions;
      String transpositionsRaw = getParam(TRANSPOSITIONS_PARAM);
      boolean transpositions =
          (transpositionsRaw != null)
              ? Boolean.parseBoolean(transpositionsRaw)
              : FuzzyQuery.defaultTranspositions;

      return new FuzzyQuery(t, maxEdits, prefixLength, maxExpansions, transpositions);
    }

    protected String analyzeIfMultitermTermText(String field, String part) {
      FieldType fieldType = req.getSchema().getFieldTypeNoEx(field);
      if (part == null
          || !(fieldType instanceof TextField)
          || ((TextField) fieldType).getMultiTermAnalyzer() == null) return part;

      BytesRef out =
          TextField.analyzeMultiTerm(field, part, ((TextField) fieldType).getMultiTermAnalyzer());
      return out == null ? part : out.utf8ToString();
    }
  }
}
