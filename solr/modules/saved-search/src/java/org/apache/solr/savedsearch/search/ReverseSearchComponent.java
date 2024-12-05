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

package org.apache.solr.savedsearch.search;

import static org.apache.solr.savedsearch.search.PresearcherFactory.DEFAULT_ALIAS_PREFIX;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import java.util.function.Function;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.monitor.CandidateMatcher;
import org.apache.lucene.monitor.DocumentBatchVisitor;
import org.apache.lucene.monitor.MonitorFields;
import org.apache.lucene.monitor.Presearcher;
import org.apache.lucene.monitor.QueryDecomposer;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.monitor.TermFilteredPresearcher;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.component.DebugComponent;
import org.apache.solr.handler.component.QueryComponent;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.loader.JsonLoader;
import org.apache.solr.savedsearch.AliasingPresearcher;
import org.apache.solr.savedsearch.SavedSearchDecoder;
import org.apache.solr.savedsearch.cache.DefaultSavedSearchCache;
import org.apache.solr.savedsearch.cache.SavedSearchCache;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;

public class ReverseSearchComponent extends QueryComponent implements SolrCoreAware {

  public static final String COMPONENT_NAME = "reverseSearch";

  private static final String SAVED_SEARCH_CACHE_NAME_KEY = "savedSearchCacheName";
  private static final String SAVED_SEARCH_CACHE_NAME_DEFAULT = "savedSearchCache";
  private static final String REVERSE_SEARCH_DOCUMENTS_KEY = "reverseSearchDocuments";
  private String savedSearchCacheName = SAVED_SEARCH_CACHE_NAME_DEFAULT;

  private QueryDecomposer queryDecomposer;
  private Presearcher presearcher;
  private PresearcherFactory.PresearcherParameters presearcherParameters;

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    Object savedSearchCacheName = args.remove(SAVED_SEARCH_CACHE_NAME_KEY);
    if (savedSearchCacheName != null) {
      this.savedSearchCacheName = (String) savedSearchCacheName;
    }
    presearcherParameters = new PresearcherFactory.PresearcherParameters();
    SolrPluginUtils.invokeSetters(presearcherParameters, args);
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    super.prepare(rb);
    try (final DocumentBatchVisitor documentBatchVisitor =
        documentBatchVisitor(rb.req.getJSON(), rb.req.getSchema())) {
      final LeafReader documentBatch = documentBatchVisitor.get();

      final SavedSearchCache savedSearchCache =
          (DefaultSavedSearchCache) rb.req.getSearcher().getCache(this.savedSearchCacheName);

      final BiPredicate<String, BytesRef> termAcceptor;
      if (savedSearchCache == null) {
        termAcceptor = (__, ___) -> true;
      } else {
        termAcceptor = savedSearchCache::acceptTerm;
      }
      final Query preFilterQuery = presearcher.buildQuery(documentBatch, termAcceptor);
      rb.setQuery(reverseSearchQuery(preFilterQuery, rb, documentBatch, savedSearchCache));
    }
  }

  private static ReverseSearchQuery reverseSearchQuery(
      Query preFilterQuery,
      ResponseBuilder rb,
      LeafReader documentBatch,
      SavedSearchCache savedSearchCache) {
    final SavedSearchDecoder savedSearchDecoder = new SavedSearchDecoder(rb.req.getCore());

    final SolrMatcherSink solrMatcherSink =
        new DefaultSolrMatcherSink<>(
            QueryMatch.SIMPLE_MATCHER::createMatcher, new IndexSearcher(documentBatch));
    rb.req.getContext().put(SolrMatcherSink.class.getSimpleName(), solrMatcherSink);

    final ReverseSearchQuery.ReverseSearchContext context =
        new ReverseSearchQuery.ReverseSearchContext(
            savedSearchCache, savedSearchDecoder, solrMatcherSink);

    return new ReverseSearchQuery(context, preFilterQuery);
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    super.process(rb);
    SolrMatcherSink solrMatcherSink =
        (SolrMatcherSink) rb.req.getContext().get(SolrMatcherSink.class.getSimpleName());
    if (rb.isDebug() && solrMatcherSink != null) {
      ReverseSearchQuery.Metadata metadata = solrMatcherSink.getMetadata();
      DebugComponent.CustomDebugInfoSources debugInfoSources =
          (DebugComponent.CustomDebugInfoSources)
              rb.req
                  .getContext()
                  .computeIfAbsent(
                      DebugComponent.CustomDebugInfoSources.KEY,
                      key -> new DebugComponent.CustomDebugInfoSources());
      SimpleOrderedMap<Object> info = new SimpleOrderedMap<>();
      info.add("queriesRun", metadata.getQueriesRun());
      debugInfoSources.add(new DebugComponent.CustomDebugInfoSource("reverse-search-debug", info));
    }
  }

  @SuppressWarnings({"unchecked"})
  private static DocumentBatchVisitor documentBatchVisitor(
      Map<String, Object> json, IndexSchema indexSchema) {
    Object jsonParams = json.get("params");
    if (!(jsonParams instanceof Map)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "need params");
    }
    Map<?, ?> paramMap = (Map<?, ?>) jsonParams;
    Object documents = paramMap.get(REVERSE_SEARCH_DOCUMENTS_KEY);
    if (!(documents instanceof List)) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "need documents list");
    }
    List<Document> luceneDocs = new ArrayList<>();
    for (Object document : (List<?>) documents) {
      if (!(document instanceof Map)
          || !((Map<?, ?>) document).keySet().stream().allMatch(key -> key instanceof String)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "document needs to be a string-keyed map");
      }
      Map<Object, Object> docAsMap = (Map<Object, Object>) document;
      docAsMap.putIfAbsent(indexSchema.getUniqueKeyField().getName(), UUID.randomUUID().toString());
      var solrInputDoc = JsonLoader.buildDoc((Map<String, Object>) document);
      var luceneDoc = DocumentBuilder.toDocument(solrInputDoc, indexSchema);
      luceneDocs.add(luceneDoc);
    }
    return DocumentBatchVisitor.of(indexSchema.getIndexAnalyzer(), luceneDocs);
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

  static class DefaultSolrMatcherSink<T extends QueryMatch> implements SolrMatcherSink {

    private final Function<IndexSearcher, CandidateMatcher<T>> matcherFactory;
    private final IndexSearcher docBatchSearcher;
    private final ConcurrentHashMap.KeySetView<ReverseSearchQuery.Metadata, Boolean> metadataSet =
        ConcurrentHashMap.newKeySet();

    DefaultSolrMatcherSink(
        Function<IndexSearcher, CandidateMatcher<T>> matcherFactory,
        IndexSearcher docBatchSearcher) {
      this.matcherFactory = matcherFactory;
      this.docBatchSearcher = docBatchSearcher;
    }

    @Override
    public boolean matchQuery(String queryId, Query matchQuery, Map<String, String> metadata)
        throws IOException {
      var matcher = matcherFactory.apply(docBatchSearcher);
      matcher.matchQuery(queryId, matchQuery, metadata);
      var matches = matcher.finish(Long.MIN_VALUE, 1);
      for (int doc = 0; doc < matches.getBatchSize(); doc++) {
        var match = matches.getMatches(doc);
        if (!match.isEmpty()) {
          return true;
        }
      }
      return false;
    }

    @Override
    public void captureMetadata(ReverseSearchQuery.Metadata metadata) {
      metadataSet.add(metadata);
    }

    @Override
    public ReverseSearchQuery.Metadata getMetadata() {
      return metadataSet.stream()
          .reduce(ReverseSearchQuery.Metadata.IDENTITY, ReverseSearchQuery.Metadata::add);
    }
  }
}
