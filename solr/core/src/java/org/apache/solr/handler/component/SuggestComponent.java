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
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.CloseHook;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.QueryLimits;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.spelling.suggest.SolrSuggester;
import org.apache.solr.spelling.suggest.SuggesterOptions;
import org.apache.solr.spelling.suggest.SuggesterParams;
import org.apache.solr.spelling.suggest.SuggesterResult;
import org.apache.solr.util.SolrResponseUtil;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SuggestComponent: interacts with multiple {@link SolrSuggester} to serve up suggestions
 * Responsible for routing commands and queries to the appropriate {@link SolrSuggester} and for
 * initializing them as specified by SolrConfig
 */
public class SuggestComponent extends SearchComponent
    implements SolrCoreAware, SuggesterParams, Accountable, SolrMetricProducer {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /** Name used to identify whether the user query concerns this component */
  public static final String COMPONENT_NAME = "suggest";

  /** Name assigned to an unnamed suggester (at most one suggester) can be unnamed */
  private static final String DEFAULT_DICT_NAME = SolrSuggester.DEFAULT_DICT_NAME;

  /** SolrConfig label to identify Config time settings */
  private static final String CONFIG_PARAM_LABEL = "suggester";

  /** SolrConfig label to identify boolean value to build suggesters on commit */
  private static final String BUILD_ON_COMMIT_LABEL = "buildOnCommit";

  /** SolrConfig label to identify boolean value to build suggesters on optimize */
  private static final String BUILD_ON_OPTIMIZE_LABEL = "buildOnOptimize";

  /** SolrConfig label to identify boolean value to build suggesters on startup */
  private static final String BUILD_ON_STARTUP_LABEL = "buildOnStartup";

  /**
   * SolrConfig label to identify boolean value to build suggesters on commit in the background
   * instead of blocking the commit. Defaults to false: buildOnCommit is synchronous unless a
   * suggester opts into the async behavior explicitly, to preserve buildOnCommit's existing
   * "suggestions are guaranteed fresh immediately after commit" contract for anyone not asking
   * for the new behavior.
   */
  private static final String BUILD_ON_COMMIT_ASYNC_LABEL = "buildOnCommitAsync";

  protected NamedList<?> initParams;

  /**
   * Key is the dictionary name used in SolrConfig, value is the corresponding {@link SolrSuggester}
   */
  protected Map<String, SolrSuggester> suggesters = new ConcurrentHashMap<>();

  /** Key is the dictionary name used in SolrConfig, value is that suggester's event listener */
  protected Map<String, SuggesterListener> suggesterListeners = new ConcurrentHashMap<>();

  /** Container for various labels used in the responses generated by this component */
  private static class SuggesterResultLabels {
    static final String SUGGEST = "suggest";
    static final String SUGGESTIONS = "suggestions";
    static final String SUGGESTION_NUM_FOUND = "numFound";
    static final String SUGGESTION_TERM = "term";
    static final String SUGGESTION_WEIGHT = "weight";
    static final String SUGGESTION_PAYLOAD = "payload";
    static final String SUGGESTER_INDEX_VERSIONS = "suggesterIndexVersions";
    static final String BUILT_FROM_INDEX_VERSION = "builtFromIndexVersion";
    static final String CURRENT_INDEX_VERSION = "currentIndexVersion";
  }

  @Override
  public void init(NamedList<?> args) {
    super.init(args);
    this.initParams = args;
  }

  @Override
  public void inform(SolrCore core) {
    if (initParams != null) {
      log.info("Initializing SuggestComponent");
      boolean hasDefault = false;
      for (int i = 0; i < initParams.size(); i++) {
        if (initParams.getName(i).equals(CONFIG_PARAM_LABEL)) {
          NamedList<?> suggesterParams = (NamedList<?>) initParams.getVal(i);
          SolrSuggester suggester = new SolrSuggester();
          String dictionary = suggester.init(suggesterParams, core);
          if (dictionary != null) {
            boolean isDefault = dictionary.equals(DEFAULT_DICT_NAME);
            if (isDefault && !hasDefault) {
              hasDefault = true;
            } else if (isDefault) {
              throw new RuntimeException("More than one dictionary is missing name.");
            }
            suggesters.put(dictionary, suggester);
          } else {
            if (!hasDefault) {
              suggesters.put(DEFAULT_DICT_NAME, suggester);
              hasDefault = true;
            } else {
              throw new RuntimeException("More than one dictionary is missing name.");
            }
          }
          boolean buildOnStartup;
          Object buildOnStartupObj = suggesterParams.get(BUILD_ON_STARTUP_LABEL);
          if (buildOnStartupObj == null) {
            Path storeFile = suggester.getStoreFile();
            buildOnStartup = storeFile == null || !Files.exists(storeFile);
          } else {
            buildOnStartup = Boolean.parseBoolean((String) buildOnStartupObj);
          }
          boolean buildOnCommit =
              Boolean.parseBoolean((String) suggesterParams.get(BUILD_ON_COMMIT_LABEL));
          boolean buildOnOptimize =
              Boolean.parseBoolean((String) suggesterParams.get(BUILD_ON_OPTIMIZE_LABEL));
          boolean buildOnCommitAsync =
              Boolean.parseBoolean((String) suggesterParams.get(BUILD_ON_COMMIT_ASYNC_LABEL));

          if (buildOnCommit || buildOnOptimize || buildOnStartup) {
            SuggesterListener listener =
                new SuggesterListener(
                    core,
                    suggester,
                    buildOnCommit,
                    buildOnOptimize,
                    buildOnStartup,
                    core.isReloaded(),
                    buildOnCommitAsync);
            if (log.isInfoEnabled()) {
              log.info(
                  "Registering searcher listener for suggester: {} = {}",
                  suggester.getName(),
                  listener);
            }
            core.registerFirstSearcherListener(listener);
            core.registerNewSearcherListener(listener);
            suggesterListeners.put(dictionary != null ? dictionary : DEFAULT_DICT_NAME, listener);
            if (buildOnCommit) {
              core.addCloseHook(
                  new CloseHook() {
                    @Override
                    public void preClose(SolrCore core) {
                      listener.shutdownBuildOnCommitExecutor();
                    }
                  });
            }
          }
        }
      }
    }
  }

  /** Responsible for issuing build and rebuild command to the specified {@link SolrSuggester} */
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    log.info("SuggestComponent prepare with : {}", params);
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }

    boolean buildAll = params.getBool(SUGGEST_BUILD_ALL, false);
    boolean reloadAll = params.getBool(SUGGEST_RELOAD_ALL, false);

    final Collection<SolrSuggester> querysuggesters;
    if (buildAll || reloadAll) {
      querysuggesters = suggesters.values();
    } else {
      querysuggesters = getSuggesters(params);
    }
    QueryLimits queryLimits = QueryLimits.getCurrentLimits();
    if (params.getBool(SUGGEST_BUILD, false) || buildAll) {
      rb.rsp.add("command", (!buildAll) ? "build" : "buildAll");
      for (SolrSuggester suggester : querysuggesters) {
        suggester.build(rb.req.getCore(), rb.req.getSearcher());
        if (queryLimits.maybeExitWithPartialResults("Suggester build " + suggester.getName())) {
          return;
        }
      }
    } else if (params.getBool(SUGGEST_RELOAD, false) || reloadAll) {
      rb.rsp.add("command", (!reloadAll) ? "reload" : "reloadAll");
      for (SolrSuggester suggester : querysuggesters) {
        suggester.reload();
        if (queryLimits.maybeExitWithPartialResults("Suggester reload " + suggester.getName())) {
          return;
        }
      }
    }
  }

  /** Dispatch shard request in <code>STAGE_EXECUTE_QUERY</code> stage */
  @Override
  public int distributedProcess(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    log.info("SuggestComponent distributedProcess with : {}", params);
    if (rb.getStage() < ResponseBuilder.STAGE_EXECUTE_QUERY)
      return ResponseBuilder.STAGE_EXECUTE_QUERY;
    if (rb.getStage() == ResponseBuilder.STAGE_EXECUTE_QUERY) {
      ShardRequest sreq = new ShardRequest();
      sreq.purpose = ShardRequest.PURPOSE_GET_TOP_IDS;
      sreq.params = new ModifiableSolrParams(rb.req.getParams());
      sreq.params.remove(ShardParams.SHARDS);
      rb.addRequest(this, sreq);
      return ResponseBuilder.STAGE_GET_FIELDS;
    }

    return ResponseBuilder.STAGE_DONE;
  }

  /**
   * Responsible for using the specified suggester to get the suggestions for the query and write
   * the results
   */
  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    log.debug("SuggestComponent process with : {}", params);
    if (!params.getBool(COMPONENT_NAME, false) || suggesters.isEmpty()) {
      return;
    }

    boolean buildAll = params.getBool(SUGGEST_BUILD_ALL, false);
    boolean reloadAll = params.getBool(SUGGEST_RELOAD_ALL, false);
    List<SolrSuggester> querySuggesters;
    try {
      querySuggesters = getSuggesters(params);
    } catch (SolrException ex) {
      if (!buildAll && !reloadAll) {
        throw ex;
      } else {
        querySuggesters = new ArrayList<>();
      }
    }

    String query = params.get(SUGGEST_Q);
    if (query == null) {
      query = rb.getQueryString();
      if (query == null) {
        query = params.get(CommonParams.Q);
      }
    }

    if (query != null) {
      int count = params.getInt(SUGGEST_COUNT, 1);
      boolean highlight = params.getBool(SUGGEST_HIGHLIGHT, false);
      boolean allTermsRequired = params.getBool(SUGGEST_ALL_TERMS_REQUIRED, true);
      String contextFilter = params.get(SUGGEST_CONTEXT_FILTER_QUERY);
      if (contextFilter != null) {
        contextFilter = contextFilter.trim();
        if (contextFilter.length() == 0) {
          contextFilter = null;
        }
      }

      SuggesterOptions options =
          new SuggesterOptions(
              new CharsRef(query), count, contextFilter, allTermsRequired, highlight);
      SimpleOrderedMap<SimpleOrderedMap<NamedList<Object>>> namedListResults =
          new SimpleOrderedMap<>();
      rb.rsp.add(SuggesterResultLabels.SUGGEST, namedListResults);
      // Lets a caller tell whether these suggestions came from the current index or a previous
      // one - relevant when buildOnCommitAsync=true means a rebuild can still be catching up.
      long currentIndexVersion = rb.req.getSearcher().getIndexReader().getVersion();
      SimpleOrderedMap<Object> indexVersions = new SimpleOrderedMap<>();
      rb.rsp.add(SuggesterResultLabels.SUGGESTER_INDEX_VERSIONS, indexVersions);
      QueryLimits queryLimits = QueryLimits.getCurrentLimits();
      for (SolrSuggester suggester : querySuggesters) {
        SuggesterResult suggesterResult = suggester.getSuggestions(options);
        toNamedList(suggesterResult, namedListResults);
        SimpleOrderedMap<Object> versions = new SimpleOrderedMap<>();
        versions.add(
            SuggesterResultLabels.BUILT_FROM_INDEX_VERSION, suggester.getBuiltFromIndexVersion());
        versions.add(SuggesterResultLabels.CURRENT_INDEX_VERSION, currentIndexVersion);
        indexVersions.add(suggester.getName(), versions);
        if (queryLimits.maybeExitWithPartialResults("Suggester process " + suggester.getName())) {
          return;
        }
      }
    }
  }

  /** Used in Distributed Search, merges the suggestion results from every shard */
  @Override
  public void finishStage(ResponseBuilder rb) {
    SolrParams params = rb.req.getParams();
    log.info("SuggestComponent finishStage with : {}", params);
    if (!params.getBool(COMPONENT_NAME, false) || rb.getStage() != ResponseBuilder.STAGE_GET_FIELDS)
      return;
    int count = params.getInt(SUGGEST_COUNT, 1);

    List<SuggesterResult> suggesterResults = new ArrayList<>();
    QueryLimits queryLimits = QueryLimits.getCurrentLimits();

    // Collect Shard responses
    for (ShardRequest sreq : rb.finished) {
      for (ShardResponse srsp : sreq.responses) {
        @SuppressWarnings("unchecked")
        SimpleOrderedMap<SimpleOrderedMap<NamedList<Object>>> namedList =
            (SimpleOrderedMap<SimpleOrderedMap<NamedList<Object>>>)
                SolrResponseUtil.getSubsectionFromShardResponse(
                    rb, srsp, SuggesterResultLabels.SUGGEST, false);
        if (namedList == null) {
          continue;
        }
        if (log.isInfoEnabled()) {
          log.info("{} : {}", srsp.getShard(), namedList);
        }
        suggesterResults.add(toSuggesterResult(namedList));
        // may have tripped the mem limits
        if (queryLimits.maybeExitWithPartialResults("Suggester finish")) {
          break;
        }
      }
    }
    // Merge Shard responses
    SuggesterResult suggesterResult = merge(suggesterResults, count);
    SimpleOrderedMap<SimpleOrderedMap<NamedList<Object>>> namedListResults =
        new SimpleOrderedMap<>();
    toNamedList(suggesterResult, namedListResults);

    rb.rsp.add(SuggesterResultLabels.SUGGEST, namedListResults);

    // either throw or mark
    queryLimits.maybeExitWithPartialResults("Suggester finish");
  }

  /**
   * Given a list of {@link SuggesterResult} and <code>count</code> returns a {@link
   * SuggesterResult} containing <code>count</code> number of {@link LookupResult}, sorted by their
   * associated weights
   */
  private static SuggesterResult merge(List<SuggesterResult> suggesterResults, int count) {
    SuggesterResult result = new SuggesterResult();
    Set<String> allTokens = new HashSet<>();
    SortedSet<String> suggesterNames = new TreeSet<>();

    // collect all tokens
    for (SuggesterResult shardResult : suggesterResults) {
      for (String suggesterName : shardResult.getSuggesterNames()) {
        allTokens.addAll(shardResult.getTokens(suggesterName));
        suggesterNames.add(suggesterName);
      }
    }

    // Get Top N for every token in every shard (using weights)
    for (String suggesterName : suggesterNames) {
      for (String token : allTokens) {
        Lookup.LookupPriorityQueue resultQueue = new Lookup.LookupPriorityQueue(count);
        for (SuggesterResult shardResult : suggesterResults) {
          List<LookupResult> suggests = shardResult.getLookupResult(suggesterName, token);
          if (suggests == null) {
            continue;
          }
          for (LookupResult res : suggests) {
            resultQueue.insertWithOverflow(res);
          }
        }
        List<LookupResult> sortedSuggests = new ArrayList<>();
        Collections.addAll(sortedSuggests, resultQueue.getResults());
        result.add(suggesterName, token, sortedSuggests);
      }
    }
    return result;
  }

  @Override
  public String getDescription() {
    return "Suggester component";
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    super.initializeMetrics(parentContext, scope);

    this.solrMetricsContext.gauge(
        () -> ramBytesUsed(), true, "totalSizeInBytes", getCategory().toString());
    MetricsMap suggestersMap =
        new MetricsMap(
            map -> {
              for (Map.Entry<String, SolrSuggester> entry : suggesters.entrySet()) {
                SolrSuggester suggester = entry.getValue();
                map.putNoEx(entry.getKey(), suggester.toString());
              }
            });
    this.solrMetricsContext.gauge(
        suggestersMap, true, "suggesters", getCategory().toString(), scope);

    // Lets an operator tell, from outside the JVM, whether an async buildOnCommit rebuild is
    // currently running and how long the last one took - e.g. via /admin/metrics - since with
    // buildOnCommitAsync=true a commit no longer blocks until the rebuild finishes.
    MetricsMap buildOnCommitMap =
        new MetricsMap(
            map -> {
              for (Map.Entry<String, SuggesterListener> entry : suggesterListeners.entrySet()) {
                SuggesterListener listener = entry.getValue();
                if (!listener.buildOnCommit) {
                  continue;
                }
                SimpleOrderedMap<Object> status = new SimpleOrderedMap<>();
                status.add("async", listener.buildOnCommitAsync);
                if (listener.buildOnCommitAsync) {
                  status.add("buildInProgress", listener.isBuildOnCommitInProgress());
                  status.add("lastBuildDurationMs", listener.getLastBuildOnCommitDurationMs());
                }
                map.putNoEx(entry.getKey(), status);
              }
            });
    this.solrMetricsContext.gauge(
        buildOnCommitMap, true, "buildOnCommit", getCategory().toString(), scope);
  }

  @Override
  public long ramBytesUsed() {
    long sizeInBytes = 0;
    for (SolrSuggester suggester : suggesters.values()) {
      sizeInBytes += suggester.ramBytesUsed();
    }
    return sizeInBytes;
  }

  @Override
  public Collection<Accountable> getChildResources() {
    return Accountables.namedAccountables("field", suggesters);
  }

  private List<SolrSuggester> getSuggesters(SolrParams params) {
    List<SolrSuggester> solrSuggesters = new ArrayList<>();
    for (String suggesterName : getSuggesterNames(params)) {
      SolrSuggester curSuggester = suggesters.get(suggesterName);
      if (curSuggester != null) {
        solrSuggesters.add(curSuggester);
      } else {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "No suggester named " + suggesterName + " was configured");
      }
    }
    if (solrSuggesters.size() == 0) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'" + SUGGEST_DICT + "' parameter not specified and no default suggester configured");
    }
    return solrSuggesters;
  }

  private SortedSet<String> getSuggesterNames(SolrParams params) {
    SortedSet<String> suggesterNames = new TreeSet<>();
    String[] suggesterNamesFromParams = params.getParams(SUGGEST_DICT);
    if (suggesterNamesFromParams == null) {
      suggesterNames.add(DEFAULT_DICT_NAME);
    } else {
      for (String name : suggesterNamesFromParams) {
        suggesterNames.add(name);
      }
    }
    return suggesterNames;
  }

  /** Convert {@link SuggesterResult} to NamedList for constructing responses */
  private void toNamedList(
      SuggesterResult suggesterResult,
      SimpleOrderedMap<SimpleOrderedMap<NamedList<Object>>> resultObj) {
    final SortedSet<String> sortedSuggesterNames =
        new TreeSet<>(suggesterResult.getSuggesterNames());
    for (String suggesterName : sortedSuggesterNames) {
      SimpleOrderedMap<NamedList<Object>> results = new SimpleOrderedMap<>();
      for (String token : suggesterResult.getTokens(suggesterName)) {
        SimpleOrderedMap<Object> suggestionBody = new SimpleOrderedMap<>();
        List<LookupResult> lookupResults = suggesterResult.getLookupResult(suggesterName, token);
        suggestionBody.add(SuggesterResultLabels.SUGGESTION_NUM_FOUND, lookupResults.size());
        List<SimpleOrderedMap<Object>> suggestEntriesNamedList = new ArrayList<>();
        for (LookupResult lookupResult : lookupResults) {
          String suggestionString = lookupResult.key.toString();
          long weight = lookupResult.value;
          String payload =
              (lookupResult.payload != null) ? lookupResult.payload.utf8ToString() : "";

          SimpleOrderedMap<Object> suggestEntryNamedList = new SimpleOrderedMap<>();
          suggestEntryNamedList.add(SuggesterResultLabels.SUGGESTION_TERM, suggestionString);
          suggestEntryNamedList.add(SuggesterResultLabels.SUGGESTION_WEIGHT, weight);
          suggestEntryNamedList.add(SuggesterResultLabels.SUGGESTION_PAYLOAD, payload);
          suggestEntriesNamedList.add(suggestEntryNamedList);
        }
        suggestionBody.add(SuggesterResultLabels.SUGGESTIONS, suggestEntriesNamedList);
        results.add(token, suggestionBody);
      }
      resultObj.add(suggesterName, results);
    }
  }

  /** Convert NamedList (suggester response) to {@link SuggesterResult} */
  private SuggesterResult toSuggesterResult(
      SimpleOrderedMap<SimpleOrderedMap<NamedList<Object>>> suggestionsMap) {
    SuggesterResult result = new SuggesterResult();
    if (suggestionsMap == null) {
      return result;
    }
    // for each token
    for (Map.Entry<String, SimpleOrderedMap<NamedList<Object>>> entry : suggestionsMap) {
      String suggesterName = entry.getKey();
      for (Iterator<Map.Entry<String, NamedList<Object>>> suggestionsIter =
              entry.getValue().iterator();
          suggestionsIter.hasNext(); ) {
        Map.Entry<String, NamedList<Object>> suggestions = suggestionsIter.next();
        String tokenString = suggestions.getKey();
        List<LookupResult> lookupResults = new ArrayList<>();
        NamedList<Object> suggestion = suggestions.getValue();
        // for each suggestion
        for (int j = 0; j < suggestion.size(); j++) {
          String property = suggestion.getName(j);
          if (property.equals(SuggesterResultLabels.SUGGESTIONS)) {
            @SuppressWarnings("unchecked")
            List<NamedList<Object>> suggestionEntries =
                (List<NamedList<Object>>) suggestion.getVal(j);
            for (NamedList<Object> suggestionEntry : suggestionEntries) {
              String term = (String) suggestionEntry.get(SuggesterResultLabels.SUGGESTION_TERM);
              Long weight = (Long) suggestionEntry.get(SuggesterResultLabels.SUGGESTION_WEIGHT);
              String payload =
                  (String) suggestionEntry.get(SuggesterResultLabels.SUGGESTION_PAYLOAD);
              LookupResult res =
                  new LookupResult(new CharsRef(term), weight, new BytesRef(payload));
              lookupResults.add(res);
            }
          }
          result.add(suggesterName, tokenString, lookupResults);
        }
      }
    }
    return result;
  }

  /** Listener to build or reload the maintained {@link SolrSuggester} by this component */
  private static class SuggesterListener implements SolrEventListener {
    private final SolrCore core;
    private final SolrSuggester suggester;
    private final boolean buildOnCommit;
    private final boolean buildOnOptimize;
    private final boolean buildOnStartup;

    // On core reload, immediately after the core is created a new searcher is opened, causing the
    // suggester to trigger a "buildOnCommit". The only event that we want to trigger in that
    // situation is "buildOnStartup" so if buildOnCommit is true and this is a core being reloaded,
    // we will skip the first time this listener is called.
    private final AtomicLong callCount = new AtomicLong(0);
    private final boolean isCoreReload;

    // A slow (e.g. I/O-bound) buildOnCommit rebuild run inline would block SolrCore's shared,
    // single-threaded searcherExecutor: DirectUpdateHandler2.commit() blocks the committing
    // thread on that executor's future with no timeout, so an inline build there blocks commits
    // indefinitely. When buildOnCommitAsync is enabled, buildOnCommit rebuilds instead run on
    // this dedicated executor, and buildOnCommitInProgress coalesces overlapping commits so a
    // burst of commits doesn't queue up an unbounded backlog of increasingly stale builds - one
    // lags behind by at most the duration of the previous build, rather than commits blocking or
    // builds piling up. Defaults to false (synchronous, like before) so existing users of
    // buildOnCommit keep the "suggestions are fresh immediately after commit" guarantee unless
    // they explicitly opt into the async, eventually-consistent behavior.
    private final boolean buildOnCommitAsync;
    private final ExecutorService buildOnCommitExecutor;
    private final AtomicBoolean buildOnCommitInProgress = new AtomicBoolean(false);
    private volatile long lastBuildOnCommitDurationMs = -1L;

    public SuggesterListener(
        SolrCore core,
        SolrSuggester checker,
        boolean buildOnCommit,
        boolean buildOnOptimize,
        boolean buildOnStartup,
        boolean isCoreReload,
        boolean buildOnCommitAsync) {
      this.core = core;
      this.suggester = checker;
      this.buildOnCommit = buildOnCommit;
      this.buildOnOptimize = buildOnOptimize;
      this.buildOnStartup = buildOnStartup;
      this.isCoreReload = isCoreReload;
      this.buildOnCommitAsync = buildOnCommitAsync;
      this.buildOnCommitExecutor =
          (buildOnCommit && buildOnCommitAsync)
              ? ExecutorUtil.newMDCAwareSingleThreadExecutor(
                  new SolrNamedThreadFactory("suggesterBuildOnCommit-" + checker.getName()))
              : null;
    }

    @Override
    public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
      long thisCallCount = callCount.incrementAndGet();
      if (isCoreReload && thisCallCount == 1) {
        log.info("Skipping first newSearcher call for suggester {} in core reload", suggester);
        return;
      } else if (thisCallCount == 1 || (isCoreReload && thisCallCount == 2)) {
        if (buildOnStartup) {
          if (log.isInfoEnabled()) {
            log.info("buildOnStartup: {}", suggester.getName());
          }
          buildSuggesterIndex(newSearcher);
        }
      } else {
        if (buildOnCommit) {
          if (log.isInfoEnabled()) {
            log.info("buildOnCommit: {}", suggester.getName());
          }
          if (buildOnCommitAsync) {
            buildSuggesterIndexAsync(newSearcher);
          } else {
            buildSuggesterIndex(newSearcher);
          }
        } else if (buildOnOptimize) {
          if (newSearcher.getIndexReader().leaves().size() == 1) {
            if (log.isInfoEnabled()) {
              log.info("buildOnOptimize: {}", suggester.getName());
            }
            buildSuggesterIndex(newSearcher);
          }
        }
      }
    }

    private void buildSuggesterIndex(SolrIndexSearcher newSearcher) {
      try {
        suggester.build(core, newSearcher);
      } catch (Exception e) {
        log.error("Exception in building suggester index for {}: ", suggester.getName(), e);
      }
    }

    /**
     * Like {@link #buildSuggesterIndex}, but runs off {@code buildOnCommitExecutor} instead of
     * inline, so a slow build doesn't block SolrCore's searcherExecutor (and therefore doesn't
     * block a commit waiting on it). Skips this rebuild entirely if one is already running, so a
     * burst of commits can't queue up an unbounded backlog of stale builds.
     */
    private void buildSuggesterIndexAsync(SolrIndexSearcher newSearcher) {
      if (!buildOnCommitInProgress.compareAndSet(false, true)) {
        log.info(
            "Skipping buildOnCommit for {}: a rebuild is already in progress",
            suggester.getName());
        return;
      }
      final IndexReader reader = newSearcher.getIndexReader();
      reader.incRef(); // pin the reader open until the async build is done with it
      final long startNanos = System.nanoTime();
      log.info("Starting async buildOnCommit for {}", suggester.getName());
      try {
        buildOnCommitExecutor.execute(
            () -> {
              try {
                buildSuggesterIndex(newSearcher);
              } finally {
                lastBuildOnCommitDurationMs =
                    TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                buildOnCommitInProgress.set(false);
                releaseReader(reader);
                log.info(
                    "Finished async buildOnCommit for {} in {}ms",
                    suggester.getName(),
                    lastBuildOnCommitDurationMs);
              }
            });
      } catch (RejectedExecutionException e) {
        // core is closing concurrently with this commit; safe to drop the rebuild.
        buildOnCommitInProgress.set(false);
        releaseReader(reader);
      }
    }

    private void releaseReader(IndexReader reader) {
      try {
        reader.decRef();
      } catch (IOException e) {
        log.error("Error releasing searcher reference for {}: ", suggester.getName(), e);
      }
    }

    boolean isBuildOnCommitInProgress() {
      return buildOnCommitInProgress.get();
    }

    /** Duration of the last async buildOnCommit rebuild, in ms; -1 if none has finished yet. */
    long getLastBuildOnCommitDurationMs() {
      return lastBuildOnCommitDurationMs;
    }

    void shutdownBuildOnCommitExecutor() {
      if (buildOnCommitExecutor != null) {
        ExecutorUtil.shutdownAndAwaitTermination(buildOnCommitExecutor);
      }
    }

    @Override
    public void postCommit() {}

    @Override
    public void postSoftCommit() {}

    @Override
    public String toString() {
      return "SuggesterListener [core="
          + core
          + ", suggester="
          + suggester
          + ", buildOnCommit="
          + buildOnCommit
          + ", buildOnCommitAsync="
          + buildOnCommitAsync
          + ", buildOnOptimize="
          + buildOnOptimize
          + ", buildOnStartup="
          + buildOnStartup
          + ", isCoreReload="
          + isCoreReload
          + "]";
    }
  }
}
