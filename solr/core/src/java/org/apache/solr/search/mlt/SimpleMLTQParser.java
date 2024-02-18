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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.legacy.LegacyNumericUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QueryParsing;
import org.apache.solr.search.SolrIndexSearcher;

public class SimpleMLTQParser extends AbstractMLTQParser {

  public SimpleMLTQParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    super(qstr, localParams, params, req);
  }

  @Override
  public Query parse() {

    String uniqueValue = localParams.get(QueryParsing.V);

    SolrIndexSearcher searcher = req.getSearcher();
    Query docIdQuery = createIdQuery(req.getSchema().getUniqueKeyField().getName(), uniqueValue);

    try {
      TopDocs td = searcher.search(docIdQuery, 2);
      if (td.totalHits.value != 1)
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Error completing MLT request. Could not fetch "
                + "document with id ["
                + uniqueValue
                + "]");
      ScoreDoc[] scoreDocs = td.scoreDocs;
      return parseMLTQuery(
          this::getFieldsFromSchema,
          moreLikeThis -> moreLikeThis.like(scoreDocs[0].doc),
          docIdQuery);
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Error completing MLT request" + e.getMessage());
    }
  }

  protected Query createIdQuery(String defaultField, String uniqueValue) {
    return new TermQuery(
        req.getSchema().getField(defaultField).getType().getNumberType() != null
            ? createNumericTerm(defaultField, uniqueValue)
            : new Term(defaultField, uniqueValue));
  }

  private Term createNumericTerm(String field, String uniqueValue) {
    BytesRefBuilder bytesRefBuilder = new BytesRefBuilder();
    bytesRefBuilder.grow(LegacyNumericUtils.BUF_SIZE_INT);
    LegacyNumericUtils.intToPrefixCoded(Integer.parseInt(uniqueValue), 0, bytesRefBuilder);
    return new Term(field, bytesRefBuilder);
  }
}
