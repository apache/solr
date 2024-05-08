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
package org.apache.solr.handler.component;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.ResultContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Inspired by the ResponseLogComponent.
 *
 * <p>Adds to the .ubi_queries system collection the original user query and the document IDs that
 * are sent in the query response.
 *
 * <p>Tracks the collection name, the end user query, as json blob, and the resulting document id's.
 *
 * <p>Add it to a requestHandler in solrconfig.xml like this:
 *
 * <pre class="prettyprint">
 * &lt;searchComponent name="ubi" class="solr.UBIComponent"/&gt;
 *
 * &lt;requestHandler name="/select" class="solr.SearchHandler"&gt;
 *   &lt;lst name="defaults"&gt;
 *
 *     ...
 *
 *   &lt;/lst&gt;
 *   &lt;arr name="components"&gt;
 *     &lt;str&gt;ubi&lt;/str&gt;
 *   &lt;/arr&gt;
 * &lt;/requestHandler&gt;</pre>
 *
 * It can then be enabled at query time by supplying
 *
 * <pre>ubi=true</pre>
 *
 * query parameter.
 */
public class UBIComponent extends SearchComponent {

  public static final String COMPONENT_NAME = "ubi";
  public static final String QUERY_ID = "query_id";

  private static final Logger ubiRequestLogger =
      LoggerFactory.getLogger(SolrCore.class.getName() + ".UBIRequest");

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {}

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }

    SolrIndexSearcher searcher = rb.req.getSearcher();
    IndexSchema schema = searcher.getSchema();
    if (schema.getUniqueKeyField() == null) {
      return;
    }

    String queryId = params.get(QUERY_ID, null);

    if (queryId == null) {
      queryId = "1234";
    }

    ResultContext rc = (ResultContext) rb.rsp.getResponse();

    DocList docs = rc.getDocList();

    processIds(rb, docs, queryId, schema, searcher);
  }

  protected void processIds(
      ResponseBuilder rb,
      DocList dl,
      String queryId,
      IndexSchema schema,
      SolrIndexSearcher searcher)
      throws IOException {

    StringBuilder sb = new StringBuilder();

    Set<String> fields = Collections.singleton(schema.getUniqueKeyField().getName());
    for (DocIterator iter = dl.iterator(); iter.hasNext(); ) {

      sb.append(schema.printableUniqueKey(searcher.doc(iter.nextDoc(), fields))).append(',');
    }
    String docIds = sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";

    ubiRequestLogger.error("bob dole");
    ubiRequestLogger.info("docIds: {}", docIds);
    System.out.println("<UBI> docIds:" + docIds);
    SimpleOrderedMap<String> ubiInfo = new SimpleOrderedMap<>();
    ubiInfo.add("query_id", queryId);
    rb.rsp.add("ubi", ubiInfo);
  }

  @Override
  public String getDescription() {
    return "A component that inserts the retrieved documents into the response log entry";
  }
}
