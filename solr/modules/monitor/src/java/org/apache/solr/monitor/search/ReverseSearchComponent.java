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

import static org.apache.solr.monitor.MonitorConstants.MONITOR_DOCUMENTS_KEY;
import static org.apache.solr.monitor.MonitorConstants.MONITOR_OUTPUT_KEY;
import static org.apache.solr.monitor.MonitorConstants.QUERY_MATCH_TYPE_KEY;
import static org.apache.solr.monitor.MonitorConstants.WRITE_TO_DOC_LIST_KEY;
import static org.apache.solr.monitor.search.PresearcherFactory.DEFAULT_ALIAS_PREFIX;
import static org.apache.solr.monitor.search.QueryMatchResponseCodec.mergeResponses;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.monitor.DocumentBatchVisitor;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.QueryDecomposer;
import org.apache.lucene.monitor.TermFilteredPresearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.monitor.AliasingPresearcher;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;
import org.apache.solr.monitor.cache.MonitorQueryCache;
import org.apache.solr.monitor.cache.SharedMonitorCache;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

public class ReverseSearchComponent extends QueryComponent implements SolrCoreAware {

  public static final String COMPONENT_NAME = "reverseSearch";
  private static final String MATCHER_THREAD_COUNT_KEY = "threadCount";

  private static final String SOLR_MONITOR_CACHE_NAME_KEY = "solrMonitorCacheName";
  private static final String SOLR_MONITOR_CACHE_NAME_DEFAULT = "solrMonitorCache";
  private String solrMonitorCacheName = SOLR_MONITOR_CACHE_NAME_DEFAULT;

  private QueryDecomposer queryDecomposer;
  private Presearcher presearcher;
  private final SolrMatcherSinkFactory solrMatcherSinkFactory = new SolrMatcherSinkFactory();
  private PresearcherFactory.PresearcherParameters presearcherParameters;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    Object solrMonitorCacheName = args.remove(SOLR_MONITOR_CACHE_NAME_KEY);
    if (solrMonitorCacheName != null) {
      this.solrMonitorCacheName = (String) solrMonitorCacheName;
    }
    presearcherParameters = new PresearcherFactory.PresearcherParameters();
    SolrPluginUtils.invokeSetters(presearcherParameters, args);
  }

  @Override
  public void prepare(ResponseBuilder rb) {
    var req = rb.req;
    var documentBatch = documentBatch(req);
    boolean writeToDocList = req.getParams().getBool(WRITE_TO_DOC_LIST_KEY, false);
    var matchType = QueryMatchType.fromString(req.getParams().get(QUERY_MATCH_TYPE_KEY));
    Map<String, Object> monitorResult = new HashMap<>();
    var matcherSink = solrMatcherSinkFactory.build(matchType, documentBatch, monitorResult);
    Query preFilterQuery = presearcher.buildQuery(documentBatch.get(), getTermAcceptor(rb.req));
    List<Query> mutableFilters =
        Optional.ofNullable(rb.getFilters()).map(ArrayList::new).orElseGet(ArrayList::new);
    rb.setQuery(new MatchAllDocsQuery());
    mutableFilters.add(preFilterQuery);
    var searcher = req.getSearcher();
    MonitorQueryCache solrMonitorCache =
        (SharedMonitorCache) searcher.getCache(this.solrMonitorCacheName);
    SolrMonitorQueryDecoder queryDecoder = new SolrMonitorQueryDecoder(req.getCore());
    mutableFilters.add(
        new MonitorPostFilter(
            new SolrMonitorQueryCollector.CollectorContext(
                solrMonitorCache, queryDecoder, matcherSink, writeToDocList, matchType)));
    rb.setFilters(mutableFilters);
    rb.rsp.add(QUERY_MATCH_TYPE_KEY, matchType.name());
    if (matchType != QueryMatchType.NONE) {
      rb.rsp.add(MONITOR_OUTPUT_KEY, monitorResult);
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
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    if ((sreq.purpose & ShardRequest.PURPOSE_GET_TOP_IDS) == 0) {
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
  public String getDescription() {
    return "Component that integrates with lucene monitor for reverse search.";
  }

  @Override
  public void inform(SolrCore core) {
    queryDecomposer = new QueryDecomposer();
    presearcher = PresearcherFactory.build(core.getResourceLoader(), presearcherParameters);
    var schema = core.getLatestSchema();
    if (presearcher instanceof AliasingPresearcher) {
      String prefix = ((AliasingPresearcher) presearcher).getPrefix() + "*";
      String maxLengthPattern =
          Arrays.stream(schema.getDynamicFieldPrototypes())
              .map(SchemaField::getName)
              .max(Comparator.comparingInt(String::length))
              .orElse("");
      if (!maxLengthPattern.equals(prefix) && prefix.length() <= maxLengthPattern.length()) {
        throw new IllegalStateException(
            "Dynamic field pattern "
                + maxLengthPattern
                + " is incompatible with presearcher alias pattern "
                + prefix
                + ". Increase the length of presearcher alias prefix to be at least"
                + (maxLengthPattern.length() + 1)
                + " characters, i.e. set aliasPrefix to "
                + "_".repeat(maxLengthPattern.length() - DEFAULT_ALIAS_PREFIX.length() + 1)
                + DEFAULT_ALIAS_PREFIX
                + " and update the schema accordingly. Refer to documentation on overriding aliasPrefix in "
                + this.getClass().getSimpleName());
      }
      var fieldType = schema.getDynamicFieldType(prefix);
      if (!fieldType.isTokenized() || !fieldType.isMultiValued()) {
        throw new IllegalStateException(
            "presearcher-aliased field must be tokenized and multi-valued.");
      }
      if (!fieldType.isTokenized() || !fieldType.isMultiValued()) {
        throw new IllegalStateException(
            "presearcher-aliased field must be tokenized and multi-valued.");
      }
    }

    for (String fieldName : MonitorFields.REQUIRED_MONITOR_SCHEMA_FIELDS) {
      var field = schema.getFieldOrNull(fieldName);
      if (field == null) {
        throw new IllegalStateException(
            fieldName + " must be defined in schema for saved search to work.");
      }
    }

    if (presearcher instanceof TermFilteredPresearcher
        || presearcher instanceof AliasingPresearcher) {
      var anyTokenField = schema.getFieldOrNull(TermFilteredPresearcher.ANYTOKEN_FIELD);
      if (anyTokenField == null || !anyTokenField.tokenized() || !anyTokenField.multiValued()) {
        throw new IllegalStateException(
            TermFilteredPresearcher.ANYTOKEN_FIELD + " field must be tokenized and multi-valued.");
      }
    }
  }

  public QueryDecomposer getQueryDecomposer() {
    return queryDecomposer;
  }

  public Presearcher getPresearcher() {
    return presearcher;
  }

  private BiPredicate<String, BytesRef> getTermAcceptor(SolrQueryRequest req) {
    var searcher = req.getSearcher();
    MonitorQueryCache cache = (MonitorQueryCache) searcher.getCache(this.solrMonitorCacheName);
    if (cache == null) {
      return (__, ___) -> true;
    }
    return cache::acceptTerm;
  }
}
