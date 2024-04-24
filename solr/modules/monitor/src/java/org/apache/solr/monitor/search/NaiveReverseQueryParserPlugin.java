/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.monitor.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.monitor.Monitor;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.Utils;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.QParser;
import org.apache.solr.search.QParserPlugin;
import org.apache.solr.update.DocumentBuilder;

public class NaiveReverseQueryParserPlugin extends QParserPlugin implements ResourceLoaderAware {

  public static final String NAME = "reverse";

  private Monitor monitor;

  @Override
  public QParser createParser(
      String qstr, SolrParams localParams, SolrParams params, SolrQueryRequest req) {
    return null; // TODO
  }

  @Override
  public void inform(ResourceLoader loader) throws IOException {
    monitor = new Monitor(new WhitespaceAnalyzer());
  }

  @SuppressWarnings("unchecked")
  public void process(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException {

    List<String> matchingQueryIds = new ArrayList<>();

    String docStr = req.getParams().get("doc");
    Object docObj = Utils.fromJSONString(docStr);

    if (docObj instanceof Map) {
      SolrInputDocument solrInputDocument = JsonLoader.buildDoc((Map<String, Object>) docObj);
      rsp.add("solrInputDocument", solrInputDocument);
      Document document = DocumentBuilder.toDocument(solrInputDocument, req.getSchema());

      for (QueryMatch queryMatch :
          monitor.match(document, QueryMatch.SIMPLE_MATCHER).getMatches()) {
        matchingQueryIds.add(queryMatch.getQueryId());
      }
    }

    rsp.add("matchingQueryIds", matchingQueryIds);
  }

  public void add(SolrInputDocument solrInputDocument) throws IOException {
    String id = (String) solrInputDocument.getFieldValue("id");
    String q = (String) solrInputDocument.getFieldValue("q_s");

    final Query query;
    if ("text_ws:fish".equals(q)) {
      query = new TermQuery(new Term("text_ws", "fish"));
    } else if ("text_ws:pie".equals(q)) {
      query = new TermQuery(new Term("text_ws", "pie"));
    } else if ("*:*".equals(q)) {
      query = new MatchAllDocsQuery();
    } else {
      query = null;
    }

    if (query != null) {
      monitor.register(new MonitorQuery(id, query));
    }
  }

  public void delete(String id) throws IOException {
    monitor.deleteById(id);
  }
}
