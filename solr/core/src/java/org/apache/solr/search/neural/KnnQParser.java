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
package org.apache.solr.search.neural;

import java.io.IOException;
import java.util.List;
import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.DenseVectorField;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.QueryUtils;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SyntaxError;

public class KnnQParser extends QParser {

  // retrieve the top K results based on the distance similarity function
  static final String TOP_K = "topK";
  static final int DEFAULT_TOP_K = 10;

  /**
   * Constructor for the QParser
   *
   * @param qstr The part of the query string specific to this parser
   * @param localParams The set of parameters that are specific to this QParser. See
   *     https://solr.apache.org/guide/solr/latest/query-guide/local-params.html
   * @param params The rest of the {@link SolrParams}
   * @param req The original {@link SolrQueryRequest}.
   */
  public KnnQParser(String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() throws SyntaxError {
    String denseVectorField = localParams.get(QueryParsing.F);
    String vectorToSearch = localParams.get(QueryParsing.V);
    int topK = localParams.getInt(TOP_K, DEFAULT_TOP_K);

    if (denseVectorField == null || denseVectorField.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "the Dense Vector field 'f' is missing");
    }

    if (vectorToSearch == null || vectorToSearch.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "the Dense Vector value 'v' to search is missing");
    }

    SchemaField schemaField = req.getCore().getLatestSchema().getField(denseVectorField);
    FieldType fieldType = schemaField.getType();
    if (!(fieldType instanceof DenseVectorField)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "only DenseVectorField is compatible with Knn Query Parser");
    }

    DenseVectorField denseVectorType = (DenseVectorField) fieldType;

    return denseVectorType.getKnnVectorQuery(
        schemaField.getName(), vectorToSearch, topK, getFilterQuery());
  }

  private Query getFilterQuery() throws SolrException, SyntaxError {
    boolean isSubQuery = recurseCount != 0;
    if (!isFilter() && !isSubQuery) {
      String[] filterQueries = req.getParams().getParams(CommonParams.FQ);
      if (filterQueries != null && filterQueries.length != 0) {
        try {
          List<Query> filters = QueryUtils.parseFilterQueries(req);
          SolrIndexSearcher.ProcessedFilter processedFilter =
              req.getSearcher().getProcessedFilter(null, filters);
          return processedFilter.filter;
        } catch (IOException e) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
        }
      }
    }
    return null;
  }
}
