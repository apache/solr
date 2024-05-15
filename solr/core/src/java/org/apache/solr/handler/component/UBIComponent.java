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

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrCore;
import org.apache.solr.response.ResultContext;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/**
 * User Behavior Insights (UBI) is a open standard for gathering query and event data from users and
 * storing it in a structured format. UBI can be used for in session personalization, implicit
 * judgements, powering recommendation systems among others. Learn more about the UBI standard at <a
 * href="https://github.com/o19s/ubi">https://github.com/o19s/ubi</a>.
 *
 * <p>Query data is gathered by this component. Data tracked is the collection name, the end user
 * query, as json blob, and the resulting document id's.
 *
 * <p>Data is written out data to "ubi_queries.jsonl", a JSON with Lines formatted file, or you can
 * provide a streaming expression that is parsed and loaded by the component to stream query data to
 * a target of your choice.
 *
 * <p>Event data is tracked by letting the user write events directly to the event repository of
 * your choice, it could be a Solr collection, it could be a file or S3 bucket.
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
 *
 * <p>Ideally this component is used with the JSON Query syntax, as that facilitates passing in the
 * additional data to be tracked with a query. Here is an example:
 *
 * <pre>
 *     {
 *     "query" : "apple AND ipod",
 *     "limit":2,
 *     "start":2,
 *     "filter": [
 *        "inStock:true"
 *      ]
 *     params: {
 *       "ubi": "true"
 *       "query_id": "xyz890",
 *       "user_query": {
 *         "query": "Apple iPod",
 *         "page": 2,
 *         "in_stock": "true"
 *       }
 *     }
 *   }
 * </pre>
 *
 * Notice that we are enabling UBI query tracking, we are providing an explicit query_id and passing
 * in the user's specific choices for querying. The user_query parameters are not specific to Solr
 * syntax, they are defined by the front end user interface.
 */
public class UBIComponent extends SearchComponent implements SolrCoreAware {

  public static final String COMPONENT_NAME = "ubi";
  public static final String QUERY_ID = "query_id";
  public static final String USER_QUERY = "user_query";
  public static final String UBI_QUERY_JSONL_LOG = "ubi_queries.jsonl";

  protected PluginInfo info = PluginInfo.EMPTY_INFO;

  private final CharArr charArr = new CharArr(1024 * 2);
  JSONWriter jsonWriter = new JSONWriter(charArr, -1);
  private Writer writer;
  OutputStream fos;

  @Override
  public void inform(SolrCore core) {
    List<PluginInfo> children = info.getChildren("ubi");
    String j = EnvUtils.getProperty("solr.log.dir");
    String ubiQueryJSONLLog = EnvUtils.getProperty("solr.log.dir") + "/" + UBI_QUERY_JSONL_LOG;
    try {
      fos = new BufferedOutputStream(new FileOutputStream(ubiQueryJSONLLog));
    } catch (FileNotFoundException exception) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Error creating file  " + ubiQueryJSONLLog,
          exception);
    }
    writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8);

    if (children.isEmpty()) {
      // DefaultSolrHighlighter defHighlighter = new DefaultSolrHighlighter(core);
      // defHighlighter.init(PluginInfo.EMPTY_INFO);
      // solrConfigHighlighter = defHighlighter;
    } else {
      // solrConfigHighlighter =
      //         core.createInitInstance(
      //                 children.get(0), SolrHighlighter.class, null,
      // DefaultSolrHighlighter.class.getName());
    }
  }

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

    // See if the user passed in a user query as a query parameter
    Object userQuery = params.get(USER_QUERY);

    if (userQuery != null && userQuery.toString().startsWith("{")) {
      // Look up the original nested JSON format, typically passed in
      // via the JSON formatted query.
      @SuppressWarnings("rawtypes")
      Map jsonProperties = rb.req.getJSON();
      if (jsonProperties.containsKey("params")) {
        @SuppressWarnings("rawtypes")
        Map paramsProperties = (Map) jsonProperties.get("params");
        if (paramsProperties.containsKey("user_query")) {
          userQuery = paramsProperties.get("user_query");
        }
      }
    }

    ResultContext rc = (ResultContext) rb.rsp.getResponse();

    DocList docs = rc.getDocList();

    processIds(rb, docs, queryId, userQuery, schema, searcher);
  }

  protected void processIds(
      ResponseBuilder rb,
      DocList dl,
      String queryId,
      Object userQuery,
      IndexSchema schema,
      SolrIndexSearcher searcher)
      throws IOException {
    charArr.reset();
    StringBuilder sb = new StringBuilder();

    Set<String> fields = Collections.singleton(schema.getUniqueKeyField().getName());
    for (DocIterator iter = dl.iterator(); iter.hasNext(); ) {

      sb.append(schema.printableUniqueKey(searcher.doc(iter.nextDoc(), fields))).append(',');
    }
    String docIds = sb.length() > 0 ? sb.substring(0, sb.length() - 1) : "";
    SimpleOrderedMap<String> ubiResponseInfo = new SimpleOrderedMap<>();
    SimpleOrderedMap<Object> ubiQueryLogInfo = new SimpleOrderedMap<>();
    ubiResponseInfo.add("query_id", queryId);
    rb.rsp.add("ubi", ubiResponseInfo);

    ubiQueryLogInfo.add("query_id", queryId);
    ubiQueryLogInfo.add("user_query", userQuery);
    ubiQueryLogInfo.add("doc_ids", docIds);

    jsonWriter.write(ubiQueryLogInfo);
    writer.write(charArr.getArray(), charArr.getStart(), charArr.getEnd());
    writer.append('\n');
    writer.flush();
  }

  @Override
  public String getDescription() {
    return "A component that tracks original user query and the resulting documents returned.";
  }
}
