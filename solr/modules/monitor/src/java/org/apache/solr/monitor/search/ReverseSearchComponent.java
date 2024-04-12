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

import static org.apache.solr.monitor.MonitorConstants.DOCUMENT_BATCH_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_DOCUMENTS_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_OUTPUT_KEY;
import static org.apache.solr.monitor.MonitorConstants.QUERY_MATCH_TYPE_KEY;
import static org.apache.solr.monitor.MonitorConstants.REVERSE_SEARCH_PARAM_NAME;
import static org.apache.solr.monitor.MonitorConstants.SOLR_MONITOR_CACHE_NAME;
import static org.apache.solr.monitor.MonitorConstants.WRITE_TO_DOC_LIST_KEY;
import static org.apache.solr.monitor.search.QueryMatchResponseCodec.mergeResponses;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.monitor.DocumentBatchVisitor;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;
import org.apache.solr.monitor.cache.MonitorQueryCache;
import org.apache.solr.monitor.cache.SharedMonitorCache;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SyntaxError;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.util.plugin.SolrCoreAware;

public class ReverseSearchComponent extends SearchComponent implements SolrCoreAware {

  private static final String MATCHER_THREAD_COUNT_KEY = "threadCount";

  private SolrMatcherSinkFactory solrMatcherSinkFactory = new SolrMatcherSinkFactory();

  private ExecutorService executor;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    Object threadCount = args.get(MATCHER_THREAD_COUNT_KEY);
    if (threadCount instanceof Integer) {
      executor =
          ExecutorUtil.newMDCAwareFixedThreadPool(
              (Integer) threadCount, new NamedThreadFactory("monitor-matcher"));
      solrMatcherSinkFactory = new SolrMatcherSinkFactory(executor);
    }
  }

  @Override
  public void prepare(ResponseBuilder rb) {
    if (skipReverseSearch(rb)) {
      return;
    }
    var req = rb.req;
    var documentBatch = documentBatch(req);
    req.getContext().put(DOCUMENT_BATCH_KEY, documentBatch);
    var writeToDocListRaw = req.getParams().get(WRITE_TO_DOC_LIST_KEY, "false");
    boolean writeToDocList = Boolean.parseBoolean(writeToDocListRaw);
    var matchType = QueryMatchType.fromString(req.getParams().get(QUERY_MATCH_TYPE_KEY));
    Map<String, Object> monitorResult = new HashMap<>();
    var matcherSink = solrMatcherSinkFactory.build(matchType, documentBatch, monitorResult);
    if (matcherSink.isParallel() && writeToDocList) {
      throw new SolrException(
          ErrorCode.BAD_REQUEST,
          "writeToDocList is not supported for parallel matcher. Consider adding more shards/cores instead of parallel matcher.");
    }
    try {
      Query preFilterQuery =
          QParser.getParser("{!" + ReverseQueryParserPlugin.NAME + "}", req).parse();
      List<Query> mutableFilters =
          Optional.ofNullable(rb.getFilters()).map(ArrayList::new).orElseGet(ArrayList::new);
      rb.setQuery(new MatchAllDocsQuery());
      mutableFilters.add(preFilterQuery);
      var searcher = req.getSearcher();
      MonitorQueryCache solrMonitorCache =
          (SharedMonitorCache) searcher.getCache(SOLR_MONITOR_CACHE_NAME);
      SolrMonitorQueryDecoder queryDecoder = SolrMonitorQueryDecoder.fromCore(req.getCore());
      mutableFilters.add(
          new MonitorPostFilter(
              new SolrMonitorQueryCollector.CollectorContext(
                  solrMonitorCache, queryDecoder, matcherSink, writeToDocList, matchType)));
      rb.setFilters(mutableFilters);
      rb.rsp.add(QUERY_MATCH_TYPE_KEY, matchType.name());
      if (matchType != QueryMatchType.NONE) {
        rb.rsp.add(MONITOR_OUTPUT_KEY, monitorResult);
      }
    } catch (SyntaxError e) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Syntax error in query: " + e.getMessage());
    }
  }

  @SuppressWarnings({"unchecked"})
  private DocumentBatchVisitor documentBatch(SolrQueryRequest req) {
    Object jsonParams = req.getJSON().get("params");
    if (!(jsonParams instanceof Map)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "need params");
    }
    var paramMap = (Map<?, ?>) jsonParams;
    var documents = paramMap.get(MONITOR_DOCUMENTS_KEY);
    if (!(documents instanceof List)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "need documents list");
    }
    List<Document> luceneDocs = new ArrayList<>();
    for (var document : (List<?>) documents) {
      if (!(document instanceof Map)
          || !((Map<?, ?>) document).keySet().stream().allMatch(key -> key instanceof String)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "document needs to be a string-keyed map");
      }
      var solrInputDoc = JsonLoader.buildDoc((Map<String, Object>) document);
      var luceneDoc = DocumentBuilder.toDocument(solrInputDoc, req.getSchema());
      luceneDocs.add(luceneDoc);
    }
    return DocumentBatchVisitor.of(req.getSchema().getIndexAnalyzer(), luceneDocs);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    /* no-op */
  }

  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if (skipReverseSearch(rb) || (sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) == 0) {
      return;
    }
    var responses =
        sreq.responses.stream()
            .map(shardResponse -> shardResponse.getSolrResponse().getResponse())
            .collect(Collectors.toList());
    rb.rsp.getValues().removeAll(MONITOR_OUTPUT_KEY);
    var matchType = QueryMatchType.fromString(rb.req.getParams().get(QUERY_MATCH_TYPE_KEY));
    if (matchType != QueryMatchType.NONE) {
      var finalOutput = mergeResponses(responses, matchType);
      rb.rsp.add(MONITOR_OUTPUT_KEY, finalOutput);
    }
  }

  @Override
  public void finishStage(ResponseBuilder rb) {
    super.finishStage(rb);
  }

  @Override
  public String getDescription() {
    return "Component that integrates with lucene monitor for reverse search.";
  }

  private static boolean skipReverseSearch(ResponseBuilder rb) {
    return !rb.req.getParams().getBool(REVERSE_SEARCH_PARAM_NAME, false);
  }

  @Override
  public void inform(SolrCore core) {
    core.addCloseHook(
        new CloseHook() {
          @Override
          public void preClose(SolrCore core) {
            ExecutorUtil.shutdownAndAwaitTermination(executor);
          }
        });
  }
}
