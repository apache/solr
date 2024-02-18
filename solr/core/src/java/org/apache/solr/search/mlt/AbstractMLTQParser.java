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
package org.apache.solr.search.mlt;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.util.SolrPluginUtils;

abstract class AbstractMLTQParser extends QParser {
  // Pattern is thread safe -- TODO? share this with general 'fl' param
  private static final Pattern splitList = Pattern.compile(",| ");

  /** Retrieves text and string fields fom the schema */
  protected String[] getFieldsFromSchema() {
    Map<String, SchemaField> fieldDefinitions = req.getSearcher().getSchema().getFields();
    ArrayList<String> fields = new ArrayList<>();
    for (Map.Entry<String, SchemaField> entry : fieldDefinitions.entrySet()) {
      if (entry.getValue().indexed() && entry.getValue().stored())
        if (entry.getValue().getType().getNumberType() == null) fields.add(entry.getKey());
    }
    return fields.toArray(new String[0]);
  }

  /**
   * Constructor for the QParser
   *
   * @param qstr The part of the query string specific to this parser
   * @param localParams The set of parameters that are specific to this QParser. See
   *     https://solr.apache.org/guide/solr/latest/query-guide/local-params.html
   * @param params The rest of the {@link SolrParams}
   * @param req The original {@link SolrQueryRequest}.
   */
  AbstractMLTQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  /** exclude current document from results */
  public BooleanQuery exclude(BooleanQuery boostedMLTQuery, Query docIdQuery) {
    BooleanQuery.Builder realMLTQuery = new BooleanQuery.Builder();
    realMLTQuery.add(boostedMLTQuery, BooleanClause.Occur.MUST);
    realMLTQuery.add(docIdQuery, BooleanClause.Occur.MUST_NOT);
    return realMLTQuery.build();
  }

  @FunctionalInterface
  protected interface MLTInvoker {
    Query invoke(MoreLikeThis mlt) throws IOException;
  }

  protected BooleanQuery parseMLTQuery(
      Supplier<String[]> fieldsFallback, MLTInvoker invoker, Query docIdQuery) throws IOException {
    return exclude(parseMLTQuery(fieldsFallback, invoker), docIdQuery);
  }

  protected BooleanQuery parseMLTQuery(Supplier<String[]> fieldsFallback, MLTInvoker invoker)
      throws IOException {
    Map<String, Float> boostFields = new HashMap<>();
    MoreLikeThis mlt = new MoreLikeThis(req.getSearcher().getIndexReader());

    mlt.setMinTermFreq(localParams.getInt("mintf", MoreLikeThis.DEFAULT_MIN_TERM_FREQ));
    // TODO def mindf was 0 for cloud, 5 for standalone
    mlt.setMinDocFreq(localParams.getInt("mindf", MoreLikeThis.DEFAULT_MIN_DOC_FREQ));
    mlt.setMinWordLen(localParams.getInt("minwl", MoreLikeThis.DEFAULT_MIN_WORD_LENGTH));
    mlt.setMaxWordLen(localParams.getInt("maxwl", MoreLikeThis.DEFAULT_MAX_WORD_LENGTH));
    mlt.setMaxQueryTerms(localParams.getInt("maxqt", MoreLikeThis.DEFAULT_MAX_QUERY_TERMS));
    mlt.setMaxNumTokensParsed(
        localParams.getInt("maxntp", MoreLikeThis.DEFAULT_MAX_NUM_TOKENS_PARSED));
    mlt.setMaxDocFreq(localParams.getInt("maxdf", MoreLikeThis.DEFAULT_MAX_DOC_FREQ));

    final boolean boost = localParams.getBool("boost", MoreLikeThis.DEFAULT_BOOST);
    mlt.setBoost(boost);

    mlt.setAnalyzer(req.getSchema().getIndexAnalyzer());

    final String[] fieldNames;
    String[] qf = localParams.getParams("qf");
    if (qf != null) {
      ArrayList<String> fields = new ArrayList<>();
      for (String fieldName : qf) {
        if (StrUtils.isNotNullOrEmpty(fieldName)) {
          String[] strings = splitList.split(fieldName);
          for (String string : strings) {
            if (StrUtils.isNotNullOrEmpty(string)) {
              fields.add(string);
            }
          }
        }
      }
      // Parse field names and boosts from the fields
      boostFields.putAll(SolrPluginUtils.parseFieldBoosts(fields.toArray(new String[0])));
      fieldNames = boostFields.keySet().toArray(new String[0]);
    } else {
      fieldNames = fieldsFallback.get();
    }
    if (fieldNames.length < 1) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "MoreLikeThis requires at least one similarity field: qf");
    }
    mlt.setFieldNames(fieldNames);
    final BooleanQuery rawMLTQuery = (BooleanQuery) invoker.invoke(mlt);

    if (boost && boostFields.size() > 0) {
      BooleanQuery.Builder newQ = new BooleanQuery.Builder();
      newQ.setMinimumNumberShouldMatch(rawMLTQuery.getMinimumNumberShouldMatch());

      for (BooleanClause clause : rawMLTQuery) {
        Query q = clause.getQuery();
        float originalBoost = 1f;
        if (q instanceof BoostQuery) {
          BoostQuery bq = (BoostQuery) q;
          q = bq.getQuery();
          originalBoost = bq.getBoost();
        }
        Float fieldBoost = boostFields.get(((TermQuery) q).getTerm().field());
        q =
            ((fieldBoost != null)
                ? new BoostQuery(q, fieldBoost * originalBoost)
                : clause.getQuery());
        newQ.add(q, clause.getOccur());
      }
      return QueryUtils.build(newQ, this);
    }
    return rawMLTQuery;
  }
}
