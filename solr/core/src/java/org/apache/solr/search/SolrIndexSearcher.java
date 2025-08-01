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
package org.apache.solr.search;

import static org.apache.solr.search.CpuAllowedLimit.TIMING_CONTEXT;

import com.codahale.metrics.Gauge;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.ExitableDirectoryReader;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiPostingsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.QueryTimeout;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TimeLimitingCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.ExecutorUtil.MDCAwareThreadPoolExecutor;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.facet.UnInvertedField;
import org.apache.solr.search.stats.StatsCache;
import org.apache.solr.search.stats.StatsSource;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.update.IndexFingerprint;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.solr.util.IOFunction;
import org.apache.solr.util.ThreadCpuTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SolrIndexSearcher adds schema awareness and caching functionality over {@link IndexSearcher}.
 *
 * @since solr 0.9
 */
public class SolrIndexSearcher extends IndexSearcher implements Closeable, SolrInfoBean {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String STATS_SOURCE = "org.apache.solr.stats_source";
  public static final String STATISTICS_KEY = "searcher";

  public static final String EXITABLE_READER_PROPERTY = "solr.useExitableDirectoryReader";

  // These should *only* be used for debugging or monitoring purposes
  public static final AtomicLong numOpens = new AtomicLong();
  public static final AtomicLong numCloses = new AtomicLong();
  private static final Map<String, SolrCache<?, ?>> NO_GENERIC_CACHES = Collections.emptyMap();
  private static final SolrCache<?, ?>[] NO_CACHES = new SolrCache<?, ?>[0];

  public static final int EXECUTOR_MAX_CPU_THREADS = Runtime.getRuntime().availableProcessors();

  private final SolrCore core;
  private final IndexSchema schema;
  private final SolrDocumentFetcher docFetcher;

  private final String name;
  private final Date openTime = new Date();
  private final long openNanoTime = System.nanoTime();
  private Date registerTime;
  private long warmupTime = 0;
  private final DirectoryReader reader;
  private final boolean closeReader;

  private final int queryResultWindowSize;
  private final int queryResultMaxDocsCached;
  private final boolean useFilterForSortedQuery;

  private final boolean cachingEnabled;
  private final SolrCache<Query, DocSet> filterCache;
  private final SolrCache<QueryResultKey, DocList> queryResultCache;
  private final SolrCache<String, UnInvertedField> fieldValueCache;
  private final LongAdder fullSortCount = new LongAdder();
  private final LongAdder skipSortCount = new LongAdder();
  private final LongAdder liveDocsNaiveCacheHitCount = new LongAdder();
  private final LongAdder liveDocsInsertsCount = new LongAdder();
  private final LongAdder liveDocsHitCount = new LongAdder();

  // map of generic caches - not synchronized since it's read-only after the constructor.
  private final Map<String, SolrCache<?, ?>> cacheMap;

  // list of all caches associated with this searcher.
  @SuppressWarnings({"rawtypes"})
  private final SolrCache[] cacheList;

  private final DirectoryFactory directoryFactory;

  private final LeafReader leafReader;
  // only for addIndexes etc (no fieldcache)
  private final DirectoryReader rawReader;

  private final String path;
  private boolean releaseDirectory;

  private final StatsCache statsCache;

  private SolrMetricsContext solrMetricsContext;

  private static DirectoryReader getReader(
      SolrCore core, SolrIndexConfig config, DirectoryFactory directoryFactory, String path)
      throws IOException {
    final Directory dir = directoryFactory.get(path, DirContext.DEFAULT, config.lockType);
    try {
      return core.getIndexReaderFactory().newReader(dir, core);
    } catch (Exception e) {
      directoryFactory.release(dir);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error opening Reader", e);
    }
  }

  private static final class QueryLimitsTimeout implements QueryTimeout {
    private static final QueryLimitsTimeout INSTANCE = new QueryLimitsTimeout();

    private QueryLimitsTimeout() {}

    @Override
    public boolean shouldExit() {
      return QueryLimits.getCurrentLimits().shouldExit();
    }

    @Override
    public String toString() {
      return QueryLimits.getCurrentLimits().limitStatusMessage();
    }
  }

  // TODO: wrap elsewhere and return a "map" from the schema that overrides get() ?
  // this reader supports reopen
  private static DirectoryReader wrapReader(SolrCore core, DirectoryReader reader)
      throws IOException {
    assert reader != null;
    reader = UninvertingReader.wrap(reader, core.getLatestSchema().getUninversionMapper());
    // see SOLR-16693 and SOLR-17831 for more details
    if (EnvUtils.getPropertyAsBool(EXITABLE_READER_PROPERTY, Boolean.FALSE)) {
      reader = ExitableDirectoryReader.wrap(reader, QueryLimitsTimeout.INSTANCE);
    }
    return reader;
  }

  /**
   * Create an {@link ExecutorService} to be used by the Lucene {@link IndexSearcher#getExecutor()}.
   * Shared across the whole node because it's a machine CPU resource.
   */
  public static ExecutorService initCollectorExecutor(NodeConfig cfg) {
    int indexSearcherExecutorThreads = cfg.getIndexSearcherExecutorThreads();
    if (indexSearcherExecutorThreads == 0) {
      return null;
    } else if (indexSearcherExecutorThreads < 0) {
      // Treat a negative value as "unlimited" and set it to the value number of available CPU
      // threads
      indexSearcherExecutorThreads = EXECUTOR_MAX_CPU_THREADS;
    }

    // note that Lucene will catch a RejectedExecutionException to just run the task.
    //  Therefore, we shouldn't worry too much about the queue size.
    return new MDCAwareThreadPoolExecutor(
        indexSearcherExecutorThreads,
        indexSearcherExecutorThreads,
        0L,
        TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<>(
            EnvUtils.getPropertyAsInteger("solr.search.multiThreaded.queueSize", 1000)),
        new SolrNamedThreadFactory("searcherCollector")) {

      @Override
      protected void beforeExecute(Thread t, Runnable r) {
        ThreadCpuTimer.reset(TIMING_CONTEXT);
      }
    };
  }

  /**
   * Builds the necessary collector chain (via delegate wrapping) and executes the query against it.
   * This method takes into consideration both the explicitly provided collector and postFilter as
   * well as any needed collector wrappers for dealing with options specified in the QueryCommand.
   *
   * @return The collector used for search
   */
  private Collector buildAndRunCollectorChain(
      QueryResult qr,
      Query query,
      Collector collector,
      QueryCommand cmd,
      DelegatingCollector postFilter)
      throws IOException {

    EarlyTerminatingSortingCollector earlyTerminatingSortingCollector = null;
    if (cmd.getSegmentTerminateEarly()) {
      final Sort cmdSort = cmd.getSort();
      final int cmdLen = cmd.getLen();
      final Sort mergeSort = core.getSolrCoreState().getMergePolicySort();

      if (cmdSort == null
          || cmdLen <= 0
          || mergeSort == null
          || !EarlyTerminatingSortingCollector.canEarlyTerminate(cmdSort, mergeSort)) {
        log.warn(
            "unsupported combination: segmentTerminateEarly=true cmdSort={} cmdLen={} mergeSort={}",
            cmdSort,
            cmdLen,
            mergeSort);
      } else {
        collector =
            earlyTerminatingSortingCollector =
                new EarlyTerminatingSortingCollector(collector, cmdSort, cmd.getLen());
      }
    }

    if (cmd.shouldEarlyTerminateSearch()) {
      collector = new EarlyTerminatingCollector(collector, cmd.getMaxHitsAllowed());
    }

    final long timeAllowed = cmd.getTimeAllowed();
    if (timeAllowed > 0) {
      collector =
          new TimeLimitingCollector(
              collector, TimeLimitingCollector.getGlobalCounter(), timeAllowed);
    }

    if (postFilter != null) {
      postFilter.setLastDelegate(collector);
      collector = postFilter;
    }

    if (cmd.isQueryCancellable()) {
      collector = new CancellableCollector(collector);

      // Add this to the local active queries map
      core.getCancellableQueryTracker()
          .addShardLevelActiveQuery(cmd.getQueryID(), (CancellableCollector) collector);
    }

    try {
      try {
        super.search(query, collector);
      } finally {
        // The complete() method can use the collectors, so this needs to be surrounded by the same
        // catch logic that limit collecting
        if (collector instanceof DelegatingCollector) {
          ((DelegatingCollector) collector).complete();
        }
      }
    } catch (TimeLimitingCollector.TimeExceededException
        | ExitableDirectoryReader.ExitingReaderException
        | CancellableCollector.QueryCancelledException x) {
      log.warn("Query: [{}]; ", query, x);
      qr.setPartialResults(true);
    } catch (EarlyTerminatingCollectorException etce) {
      qr.setPartialResults(true);
      qr.setMaxHitsTerminatedEarly(true);
      qr.setPartialResultsDetails(etce.getMessage());
      qr.setApproximateTotalHits(etce.getApproximateTotalHits(reader.maxDoc()));
    } finally {
      if (earlyTerminatingSortingCollector != null) {
        qr.setSegmentTerminatedEarly(earlyTerminatingSortingCollector.terminatedEarly());
      }
      if (cmd.isQueryCancellable()) {
        core.getCancellableQueryTracker().removeCancellableQuery(cmd.getQueryID());
      }
    }

    return collector;
  }

  public SolrIndexSearcher(
      SolrCore core,
      String path,
      IndexSchema schema,
      SolrIndexConfig config,
      String name,
      boolean enableCache,
      DirectoryFactory directoryFactory)
      throws IOException {
    // We don't need to reserve the directory because we get it from the factory
    this(
        core,
        path,
        schema,
        name,
        getReader(core, config, directoryFactory, path),
        true,
        enableCache,
        false,
        directoryFactory);
    // Release the directory at close.
    this.releaseDirectory = true;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public SolrIndexSearcher(
      SolrCore core,
      String path,
      IndexSchema schema,
      String name,
      DirectoryReader r,
      boolean closeReader,
      boolean enableCache,
      boolean reserveDirectory,
      DirectoryFactory directoryFactory)
      throws IOException {
    super(wrapReader(core, r), core.getCoreContainer().getIndexSearcherExecutor());

    this.path = path;
    this.directoryFactory = directoryFactory;
    this.reader = (DirectoryReader) super.readerContext.reader();
    this.rawReader = r;
    this.leafReader = SlowCompositeReaderWrapper.wrap(this.reader);
    this.core = core;
    this.statsCache = core.createStatsCache();
    this.schema = schema;
    this.name =
        "Searcher@"
            + Integer.toHexString(hashCode())
            + "["
            + core.getName()
            + "]"
            + (name != null ? " " + name : "");
    log.debug("Opening [{}]", this.name);

    if (directoryFactory.searchersReserveCommitPoints()) {
      // reserve commit point for life of searcher
      // TODO: This may not be safe w/softCommit, see SOLR-13908
      core.getDeletionPolicy().saveCommitPoint(reader.getIndexCommit().getGeneration());
    }

    if (reserveDirectory) {
      // Keep the directory from being released while we use it.
      directoryFactory.incRef(getIndexReader().directory());
      // Make sure to release it when closing.
      this.releaseDirectory = true;
    }

    this.closeReader = closeReader;
    setSimilarity(schema.getSimilarity());

    final SolrConfig solrConfig = core.getSolrConfig();
    this.queryResultWindowSize = solrConfig.queryResultWindowSize;
    this.queryResultMaxDocsCached = solrConfig.queryResultMaxDocsCached;
    this.useFilterForSortedQuery = solrConfig.useFilterForSortedQuery;

    this.docFetcher = new SolrDocumentFetcher(this, solrConfig, enableCache);

    this.cachingEnabled = enableCache;
    if (cachingEnabled) {
      final ArrayList<SolrCache> clist = new ArrayList<>();
      fieldValueCache =
          solrConfig.fieldValueCacheConfig == null
              ? null
              : solrConfig.fieldValueCacheConfig.newInstance();
      if (fieldValueCache != null) clist.add(fieldValueCache);
      filterCache =
          solrConfig.filterCacheConfig == null ? null : solrConfig.filterCacheConfig.newInstance();
      if (filterCache != null) clist.add(filterCache);
      queryResultCache =
          solrConfig.queryResultCacheConfig == null
              ? null
              : solrConfig.queryResultCacheConfig.newInstance();
      if (queryResultCache != null) clist.add(queryResultCache);
      SolrCache<Integer, Document> documentCache = docFetcher.getDocumentCache();
      if (documentCache != null) clist.add(documentCache);

      if (solrConfig.userCacheConfigs.isEmpty()) {
        cacheMap = NO_GENERIC_CACHES;
      } else {
        cacheMap = CollectionUtil.newHashMap(solrConfig.userCacheConfigs.size());
        for (Map.Entry<String, CacheConfig> e : solrConfig.userCacheConfigs.entrySet()) {
          SolrCache<?, ?> cache = e.getValue().newInstance();
          if (cache != null) {
            cacheMap.put(cache.name(), cache);
            clist.add(cache);
          }
        }
      }

      cacheList = clist.toArray(new SolrCache[0]);
    } else {
      this.filterCache = null;
      this.queryResultCache = null;
      this.fieldValueCache = null;
      this.cacheMap = NO_GENERIC_CACHES;
      this.cacheList = NO_CACHES;
    }

    // We already have our own filter cache
    setQueryCache(null);

    // do this at the end since an exception in the constructor means we won't close
    numOpens.incrementAndGet();
    assert ObjectReleaseTracker.track(this);
  }

  public SolrDocumentFetcher getDocFetcher() {
    return docFetcher.clone();
  }

  /**
   * Allows interrogation of {@link #docFetcher} template (checking field names, etc.) without
   * forcing it to be cloned (as it would be if an instance were retrieved via {@link
   * #getDocFetcher()}).
   */
  public <T> T interrogateDocFetcher(Function<SolrDocumentFetcher, T> func) {
    return func.apply(docFetcher);
  }

  public StatsCache getStatsCache() {
    return statsCache;
  }

  public FieldInfos getFieldInfos() {
    return leafReader.getFieldInfos();
  }

  /*
   * Override these two methods to provide a way to use global collection stats.
   */
  @Override
  public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq)
      throws IOException {
    final SolrRequestInfo reqInfo = SolrRequestInfo.getRequestInfo();
    if (reqInfo != null) {
      final StatsSource statsSrc = (StatsSource) reqInfo.getReq().getContext().get(STATS_SOURCE);
      if (statsSrc != null) {
        return statsSrc.termStatistics(this, term, docFreq, totalTermFreq);
      }
    }
    return localTermStatistics(term, docFreq, totalTermFreq);
  }

  @Override
  public CollectionStatistics collectionStatistics(String field) throws IOException {
    final SolrRequestInfo reqInfo = SolrRequestInfo.getRequestInfo();
    if (reqInfo != null) {
      final StatsSource statsSrc = (StatsSource) reqInfo.getReq().getContext().get(STATS_SOURCE);
      if (statsSrc != null) {
        return statsSrc.collectionStatistics(this, field);
      }
    }
    return localCollectionStatistics(field);
  }

  public TermStatistics localTermStatistics(Term term, int docFreq, long totalTermFreq)
      throws IOException {
    return super.termStatistics(term, docFreq, totalTermFreq);
  }

  public CollectionStatistics localCollectionStatistics(String field) throws IOException {
    return super.collectionStatistics(field);
  }

  public boolean isCachingEnabled() {
    return cachingEnabled;
  }

  public String getPath() {
    return path;
  }

  @Override
  public String toString() {
    return name + "{" + reader + "}";
  }

  public SolrCore getCore() {
    return core;
  }

  public final int maxDoc() {
    return reader.maxDoc();
  }

  public final int numDocs() {
    return reader.numDocs();
  }

  public final int docFreq(Term term) throws IOException {
    return reader.docFreq(term);
  }

  /**
   * Not recommended to call this method unless there is some particular reason due to internally
   * calling {@link SlowCompositeReaderWrapper}. Use {@link IndexSearcher#leafContexts} to get the
   * sub readers instead of using this method.
   */
  public final LeafReader getSlowAtomicReader() {
    return leafReader;
  }

  /** Raw reader (no fieldcaches etc). Useful for operations like addIndexes */
  public final DirectoryReader getRawReader() {
    return rawReader;
  }

  @Override
  public final DirectoryReader getIndexReader() {
    assert Objects.equals(reader, super.getIndexReader());
    return reader;
  }

  /** Register sub-objects such as caches and our own metrics */
  public void register() {
    final Map<String, SolrInfoBean> infoRegistry = core.getInfoRegistry();
    // register self
    infoRegistry.put(STATISTICS_KEY, this);
    infoRegistry.put(name, this);
    for (SolrCache<?, ?> cache : cacheList) {
      cache.setState(SolrCache.State.LIVE);
      infoRegistry.put(cache.name(), cache);
    }
    this.solrMetricsContext = core.getSolrMetricsContext().getChildContext(this);
    for (SolrCache<?, ?> cache : cacheList) {
      cache.initializeMetrics(
          solrMetricsContext, SolrMetricManager.mkName(cache.name(), STATISTICS_KEY));
    }
    initializeMetrics(solrMetricsContext, STATISTICS_KEY);
    registerTime = new Date();
  }

  /**
   * Free's resources associated with this searcher.
   *
   * <p>In particular, the underlying reader and any cache's in use are closed.
   */
  @Override
  public void close() throws IOException {
    if (log.isDebugEnabled()) {
      if (cachingEnabled) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Closing ").append(name);
        for (SolrCache<?, ?> cache : cacheList) {
          sb.append("\n\t");
          sb.append(cache);
        }
        log.debug("{}", sb);
      } else {
        log.debug("Closing [{}]", name);
      }
    }

    core.getInfoRegistry().remove(name);

    // super.close();
    // can't use super.close() since it just calls reader.close() and that may only be called once
    // per reader (even if incRef() was previously called).

    long cpg = reader.getIndexCommit().getGeneration();
    try {
      if (closeReader) rawReader.decRef();
    } catch (Exception e) {
      log.error("Problem dec ref'ing reader", e);
    }

    if (directoryFactory.searchersReserveCommitPoints()) {
      core.getDeletionPolicy().releaseCommitPoint(cpg);
    }

    for (SolrCache<?, ?> cache : cacheList) {
      try {
        cache.close();
      } catch (Exception e) {
        log.error("Exception closing cache {}", cache.name(), e);
      }
    }

    if (releaseDirectory) {
      directoryFactory.release(getIndexReader().directory());
    }

    try {
      SolrInfoBean.super.close();
    } catch (Exception e) {
      log.warn("Exception closing", e);
    }

    // do this at the end so it only gets done if there are no exceptions
    numCloses.incrementAndGet();
    assert ObjectReleaseTracker.release(this);
  }

  /** Direct access to the IndexSchema for use with this searcher */
  public IndexSchema getSchema() {
    return schema;
  }

  /** Returns a collection of all field names the index reader knows about. */
  public Iterable<String> getFieldNames() {
    return StreamSupport.stream(getFieldInfos().spliterator(), false)
        .map(fieldInfo -> fieldInfo.name)
        .collect(Collectors.toUnmodifiableList());
  }

  public SolrCache<Query, DocSet> getFilterCache() {
    return filterCache;
  }

  //
  // Set default regenerators on filter and query caches if they don't have any
  //
  public static void initRegenerators(SolrConfig solrConfig) {
    if (solrConfig.fieldValueCacheConfig != null
        && solrConfig.fieldValueCacheConfig.getRegenerator() == null) {
      solrConfig.fieldValueCacheConfig.setRegenerator(
          new CacheRegenerator() {
            @Override
            public <K, V> boolean regenerateItem(
                SolrIndexSearcher newSearcher,
                SolrCache<K, V> newCache,
                SolrCache<K, V> oldCache,
                K oldKey,
                V oldVal)
                throws IOException {
              if (oldVal instanceof UnInvertedField) {
                UnInvertedField.getUnInvertedField((String) oldKey, newSearcher);
              }
              return true;
            }
          });
    }

    if (solrConfig.filterCacheConfig != null
        && solrConfig.filterCacheConfig.getRegenerator() == null) {
      solrConfig.filterCacheConfig.setRegenerator(
          new CacheRegenerator() {
            @Override
            public <K, V> boolean regenerateItem(
                SolrIndexSearcher newSearcher,
                SolrCache<K, V> newCache,
                SolrCache<K, V> oldCache,
                K oldKey,
                V oldVal)
                throws IOException {
              newSearcher.cacheDocSet((Query) oldKey, null, false);
              return true;
            }
          });
    }

    if (solrConfig.queryResultCacheConfig != null
        && solrConfig.queryResultCacheConfig.getRegenerator() == null) {
      final int queryResultWindowSize = solrConfig.queryResultWindowSize;
      solrConfig.queryResultCacheConfig.setRegenerator(
          new CacheRegenerator() {
            @Override
            public <K, V> boolean regenerateItem(
                SolrIndexSearcher newSearcher,
                SolrCache<K, V> newCache,
                SolrCache<K, V> oldCache,
                K oldKey,
                V oldVal)
                throws IOException {
              QueryResultKey key = (QueryResultKey) oldKey;
              int nDocs = 1;
              // request 1 doc and let caching round up to the next window size...
              // unless the window size is <=1, in which case we will pick
              // the minimum of the number of documents requested last time and
              // a reasonable number such as 40.
              // TODO: make more configurable later...

              if (queryResultWindowSize <= 1) {
                DocList oldList = (DocList) oldVal;
                int oldnDocs = oldList.offset() + oldList.size();
                // 40 has factors of 2,4,5,10,20
                nDocs = Math.min(oldnDocs, 40);
              }

              int flags = NO_CHECK_QCACHE | key.nc_flags;
              QueryCommand qc = new QueryCommand();
              qc.setQuery(key.query)
                  .setFilterList(key.filters)
                  .setSort(key.sort)
                  .setLen(nDocs)
                  .setSupersetMaxDoc(nDocs)
                  .setFlags(flags);
              QueryResult qr = new QueryResult();
              newSearcher.getDocListC(qr, qc);
              return true;
            }
          });
    }
  }

  /** Primary entrypoint for searching, using a {@link QueryCommand}. */
  public QueryResult search(QueryCommand cmd) throws IOException {
    return search(new QueryResult(), cmd);
  }

  @Deprecated
  public QueryResult search(QueryResult qr, QueryCommand cmd) throws IOException {
    getDocListC(qr, cmd);
    return qr;
  }

  @Override
  protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector)
      throws IOException {
    QueryLimits queryLimits = QueryLimits.getCurrentLimits();
    if (EnvUtils.getPropertyAsBool(EXITABLE_READER_PROPERTY, Boolean.FALSE)
        || !queryLimits.isLimitsEnabled()) {
      // no timeout.  Pass through to super class
      super.search(leaves, weight, collector);
    } else {
      // Timeout enabled!  This impl is maybe a hack.  Use Lucene's IndexSearcher timeout.
      // But only some queries have it so don't use on "this" (SolrIndexSearcher), not to mention
      //   that timedOut() might race with concurrent queries (dubious design).
      // So we need to make a new IndexSearcher instead of using "this".
      new IndexSearcher(reader) { // cheap, actually!
        void searchWithTimeout() throws IOException {
          setTimeout(queryLimits); // Lucene's method name is less than ideal here...
          super.search(leaves, weight, collector); // FYI protected access
          if (timedOut()) {
            throw new QueryLimitsExceededException(
                "Limits exceeded! (search): " + queryLimits.limitStatusMessage());
          }
        }
      }.searchWithTimeout();
    }
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   *
   * @see SolrDocumentFetcher
   */
  @Override
  @Deprecated
  public Document doc(int docId) throws IOException {
    return doc(docId, (Set<String>) null);
  }

  /**
   * Visit a document's fields using a {@link StoredFieldVisitor}. This method does not currently
   * add to the Solr document cache.
   *
   * @see IndexReader#document(int, StoredFieldVisitor)
   * @see SolrDocumentFetcher
   */
  @Override
  @Deprecated
  public final void doc(int docId, StoredFieldVisitor visitor) throws IOException {
    getDocFetcher().doc(docId, visitor);
  }

  /**
   * Retrieve the {@link Document} instance corresponding to the document id.
   *
   * <p><b>NOTE</b>: the document will have all fields accessible, but if a field filter is
   * provided, only the provided fields will be loaded (the remainder will be available lazily).
   *
   * @see SolrDocumentFetcher
   */
  @Override
  @Deprecated
  public final Document doc(int i, Set<String> fields) throws IOException {
    return getDocFetcher().doc(i, fields);
  }

  /** expert: internal API, subject to change */
  public SolrCache<String, UnInvertedField> getFieldValueCache() {
    return fieldValueCache;
  }

  /** Returns a weighted sort according to this searcher */
  public Sort weightSort(Sort sort) throws IOException {
    return (sort != null) ? sort.rewrite(this) : null;
  }

  /** Returns a weighted sort spec according to this searcher */
  public SortSpec weightSortSpec(SortSpec originalSortSpec, Sort nullEquivalent)
      throws IOException {
    return implWeightSortSpec(
        originalSortSpec.getSort(),
        originalSortSpec.getCount(),
        originalSortSpec.getOffset(),
        nullEquivalent);
  }

  /** Returns a weighted sort spec according to this searcher */
  private SortSpec implWeightSortSpec(Sort originalSort, int num, int offset, Sort nullEquivalent)
      throws IOException {
    Sort rewrittenSort = weightSort(originalSort);
    if (rewrittenSort == null) {
      rewrittenSort = nullEquivalent;
    }

    final SortField[] rewrittenSortFields = rewrittenSort.getSort();
    final SchemaField[] rewrittenSchemaFields = new SchemaField[rewrittenSortFields.length];
    for (int ii = 0; ii < rewrittenSortFields.length; ++ii) {
      final String fieldName = rewrittenSortFields[ii].getField();
      rewrittenSchemaFields[ii] = (fieldName == null ? null : schema.getFieldOrNull(fieldName));
    }

    return new SortSpec(rewrittenSort, rewrittenSchemaFields, num, offset);
  }

  /**
   * Returns the first document number containing the term <code>t</code> Returns -1 if no document
   * was found. This method is primarily intended for clients that want to fetch documents using a
   * unique identifier."
   *
   * @return the first document number containing the term
   */
  public int getFirstMatch(Term t) throws IOException {
    long pair = lookupId(t.field(), t.bytes());
    if (pair == -1) {
      return -1;
    } else {
      final int segIndex = (int) (pair >> 32);
      final int segDocId = (int) pair;
      return leafContexts.get(segIndex).docBase + segDocId;
    }
  }

  /**
   * lookup the docid by the unique key field, and return the id *within* the leaf reader in the low
   * 32 bits, and the index of the leaf reader in the high 32 bits. -1 is returned if not found.
   *
   * @lucene.internal
   */
  public long lookupId(BytesRef idBytes) throws IOException {
    return lookupId(schema.getUniqueKeyField().getName(), idBytes);
  }

  private long lookupId(String field, BytesRef idBytes) throws IOException {
    for (int i = 0, c = leafContexts.size(); i < c; i++) {
      final LeafReaderContext leaf = leafContexts.get(i);
      final LeafReader reader = leaf.reader();

      final Terms terms = reader.terms(field);
      if (terms == null) continue;

      TermsEnum te = terms.iterator();
      if (te.seekExact(idBytes)) {
        PostingsEnum docs = te.postings(null, PostingsEnum.NONE);
        docs = BitsFilteredPostingsEnum.wrap(docs, reader.getLiveDocs());
        int id = docs.nextDoc();
        if (id == DocIdSetIterator.NO_MORE_DOCS) continue;
        assert docs.nextDoc() == DocIdSetIterator.NO_MORE_DOCS;

        return (((long) i) << 32) | id;
      }
    }

    return -1;
  }

  /**
   * Compute and cache the DocSet that matches a query. The normal usage is expected to be
   * cacheDocSet(myQuery, null,false) meaning that Solr will determine if the Query warrants
   * caching, and if so, will compute the DocSet that matches the Query and cache it. If the answer
   * to the query is already cached, nothing further will be done.
   *
   * <p>If the optionalAnswer DocSet is provided, it should *not* be modified after this call.
   *
   * @param query the lucene query that will act as the key
   * @param optionalAnswer the DocSet to be cached - if null, it will be computed.
   * @param mustCache if true, a best effort will be made to cache this entry. if false, heuristics
   *     may be used to determine if it should be cached.
   */
  public void cacheDocSet(Query query, DocSet optionalAnswer, boolean mustCache)
      throws IOException {
    // Even if the cache is null, still compute the DocSet as it may serve to warm the Lucene
    // or OS disk cache.
    if (optionalAnswer != null) {
      if (filterCache != null) {
        filterCache.put(query, optionalAnswer);
      }
      return;
    }

    // Throw away the result, relying on the fact that getDocSet
    // will currently always cache what it found. If getDocSet() starts
    // using heuristics about what to cache, and mustCache==true, (or if we
    // want this method to start using heuristics too) then
    // this needs to change.
    getDocSet(query);
  }

  private BitDocSet makeBitDocSet(DocSet answer) {
    // TODO: this should be implemented in DocSet, most likely with a getBits method that takes a
    // maxDoc argument or make DocSet instances remember maxDoc
    if (answer instanceof BitDocSet) {
      return (BitDocSet) answer;
    }
    FixedBitSet bs = new FixedBitSet(maxDoc());
    DocIterator iter = answer.iterator();
    while (iter.hasNext()) {
      bs.set(iter.nextDoc());
    }

    return new BitDocSet(bs, answer.size());
  }

  public BitDocSet getDocSetBits(Query q) throws IOException {
    DocSet answer = getDocSet(q);
    BitDocSet answerBits = makeBitDocSet(answer);
    if (answerBits != answer && filterCache != null) {
      filterCache.put(q, answerBits);
    }
    return answerBits;
  }

  // only handle positive (non negative) queries
  DocSet getPositiveDocSet(Query query) throws IOException {
    // TODO duplicated code with getDocSet?
    boolean doCache = filterCache != null;
    if (query instanceof ExtendedQuery) {
      if (!((ExtendedQuery) query).getCache()) {
        doCache = false;
      }
      if (query instanceof WrappedQuery) {
        query = ((WrappedQuery) query).getWrappedQuery();
      }
    }

    if (doCache) {
      return getAndCacheDocSet(query);
    }

    return getDocSetNC(query, null);
  }

  /**
   * Attempt to read the query from the filter cache, if not will compute the result and insert back
   * into the cache
   *
   * <p>Callers must ensure that:
   *
   * <ul>
   *   <li>The query is unwrapped
   *   <li>The query has caching enabled
   *   <li>The filter cache exists
   *
   * @param query the query to compute.
   * @return the DocSet answer
   */
  private DocSet getAndCacheDocSet(Query query) throws IOException {
    assert !(query instanceof WrappedQuery) : "should have unwrapped";
    assert filterCache != null : "must check for caching before calling this method";

    if (query instanceof MatchAllDocsQuery) {
      // bypass the filterCache for MatchAllDocsQuery
      return getLiveDocSet();
    }

    DocSet answer;
    QueryLimits queryLimits = QueryLimits.getCurrentLimits();
    if (queryLimits.isLimitsEnabled()) {
      // If there is a possibility of timeout for this query, then don't reserve a computation slot.
      // Further, we can't naively wait for an in progress computation to finish, because if we time
      // out before it does then we won't even have partial results to provide. We could possibly
      // wait for the query to finish in parallel with our own results and if they complete first
      // use that instead, but we'll leave that to implement later.
      answer = filterCache.get(query);

      // Not found in the cache so compute and put in the cache
      if (answer == null) {
        answer = getDocSetNC(query, null);
        filterCache.put(query, answer);
      }
    } else {
      answer = filterCache.computeIfAbsent(query, q -> getDocSetNC(q, null));
    }

    assert !(answer instanceof MutableBitDocSet) : "should not be mutable";
    return answer;
  }

  private static final MatchAllDocsQuery MATCH_ALL_DOCS_QUERY = new MatchAllDocsQuery();

  /** Used as a synchronization point to handle the lazy-init of {@link #liveDocs}. */
  private final Object liveDocsCacheLock = new Object();

  private BitDocSet liveDocs;

  private static final BitDocSet EMPTY = new BitDocSet(new FixedBitSet(0), 0);

  private BitDocSet computeLiveDocs() {
    switch (leafContexts.size()) {
      case 0:
        assert numDocs() == 0;
        return EMPTY;
      case 1:
        final Bits onlySegLiveDocs = leafContexts.get(0).reader().getLiveDocs();
        final FixedBitSet fbs;
        if (onlySegLiveDocs == null) {
          // `LeafReader.getLiveDocs()` returns null if no deleted docs -- accordingly, set all bits
          final int onlySegMaxDoc = maxDoc();
          fbs = new FixedBitSet(onlySegMaxDoc);
          fbs.set(0, onlySegMaxDoc);
        } else {
          fbs = FixedBitSet.copyOf(onlySegLiveDocs);
        }
        assert fbs.cardinality() == numDocs();
        return new BitDocSet(fbs, numDocs());
      default:
        final FixedBitSet bs = new FixedBitSet(maxDoc());
        for (LeafReaderContext ctx : leafContexts) {
          final LeafReader r = ctx.reader();
          final Bits segLiveDocs = r.getLiveDocs();
          final int segDocBase = ctx.docBase;
          if (segLiveDocs == null) {
            // `LeafReader.getLiveDocs()` returns null if no deleted docs -- accordingly, set all
            // bits in seg range
            bs.set(segDocBase, segDocBase + r.maxDoc());
          } else {
            DocSetUtil.copyTo(segLiveDocs, 0, r.maxDoc(), bs, segDocBase);
          }
        }
        assert bs.cardinality() == numDocs();
        return new BitDocSet(bs, numDocs());
    }
  }

  private BitDocSet populateLiveDocs(Supplier<BitDocSet> liveDocsSupplier) {
    synchronized (liveDocsCacheLock) {
      if (liveDocs != null) {
        liveDocsHitCount.increment();
      } else {
        liveDocs = liveDocsSupplier.get();
        liveDocsInsertsCount.increment();
      }
    }
    return liveDocs;
  }

  /**
   * Returns an efficient random-access {@link DocSet} of the live docs. It's cached. Never null.
   *
   * @lucene.internal the type of DocSet returned may change in the future
   */
  public BitDocSet getLiveDocSet() throws IOException {
    BitDocSet docs = liveDocs;
    if (docs != null) {
      liveDocsNaiveCacheHitCount.increment();
    } else {
      docs = populateLiveDocs(this::computeLiveDocs);
      // assert DocSetUtil.equals(docs, DocSetUtil.createDocSetGeneric(this, MATCH_ALL_DOCS_QUERY));
    }
    assert docs.size() == numDocs();
    return docs;
  }

  /**
   * Returns an efficient random-access {@link Bits} of the live docs. It's cached. Null means all
   * docs are live. Use this like {@link LeafReader#getLiveDocs()}.
   *
   * @lucene.internal
   */
  // TODO rename to getLiveDocs in 8.0
  public Bits getLiveDocsBits() throws IOException {
    return getIndexReader().hasDeletions() ? getLiveDocSet().getBits() : null;
  }

  /**
   * If some process external to {@link SolrIndexSearcher} has produced a DocSet whose cardinality
   * matches that of `liveDocs`, this method provides such caller the ability to offer its own
   * DocSet to be cached in the searcher. The caller should then use the returned value (which may
   * or may not be derived from the DocSet instance supplied), allowing more efficient memory use.
   *
   * @lucene.internal
   */
  public BitDocSet offerLiveDocs(Supplier<DocSet> docSetSupplier, int suppliedSize) {
    assert suppliedSize == numDocs();
    final BitDocSet ret = liveDocs;
    if (ret != null) {
      liveDocsNaiveCacheHitCount.increment();
      return ret;
    }
    // a few places currently expect BitDocSet
    return populateLiveDocs(() -> makeBitDocSet(docSetSupplier.get()));
  }

  private static Comparator<ExtendedQuery> sortByCost =
      Comparator.comparingInt(ExtendedQuery::getCost);

  /**
   * Returns the set of document ids matching all queries. This method is cache-aware and attempts
   * to retrieve the answer from the cache if possible. If the answer was not cached, it may have
   * been inserted into the cache as a result of this call. This method can handle negative queries.
   * A null/empty list results in {@link #getLiveDocSet()}.
   *
   * <p>The DocSet returned should <b>not</b> be modified.
   */
  public DocSet getDocSet(List<Query> queries) throws IOException {

    ProcessedFilter pf = getProcessedFilter(queries);

    if (pf.postFilter == null) {
      if (pf.answer != null) {
        return pf.answer;
      } else if (pf.filter == null) {
        return getLiveDocSet(); // note: this is what happens when queries is an empty list
      }
    }

    DocSetCollector setCollector = new DocSetCollector(maxDoc());
    Collector collector = setCollector;
    if (pf.postFilter != null) {
      pf.postFilter.setLastDelegate(collector);
      collector = pf.postFilter;
    }

    Query query = pf.filter != null ? pf.filter : MATCH_ALL_DOCS_QUERY;

    search(query, collector);

    if (collector instanceof DelegatingCollector) {
      ((DelegatingCollector) collector).complete();
    }

    return DocSetUtil.getDocSet(setCollector, this);
  }

  /**
   * INTERNAL: The response object from {@link #getProcessedFilter(List)}. Holds a filter and
   * postFilter pair that together match a set of documents. Either of them may be null, in which
   * case the semantics are to match everything.
   *
   * @see #getProcessedFilter(List)
   */
  public static class ProcessedFilter {
    // maybe null. Sometimes we have a docSet answer that represents the complete answer or result.
    public DocSet answer;
    public Query filter; // maybe null.  Scoring is irrelevant / unspecified.
    public DelegatingCollector postFilter; // maybe null
  }

  /**
   * INTERNAL: Processes conjunction (AND) of the queries into a {@link ProcessedFilter} result.
   * Queries may be null/empty thus doesn't restrict the matching docs. Queries typically are
   * resolved against the filter cache, and populate it.
   */
  public ProcessedFilter getProcessedFilter(List<Query> queries) throws IOException {
    ProcessedFilter pf = new ProcessedFilter();
    if (queries == null || queries.size() == 0) {
      return pf;
    }

    // We combine all the filter queries that come from the filter cache into "answer".
    // This might become pf.answer but not if there are any non-cached filters
    DocSet answer = null;

    boolean[] neg = new boolean[queries.size()];
    DocSet[] sets = new DocSet[queries.size()];
    List<ExtendedQuery> notCached = null;
    List<PostFilter> postFilters = null;

    int end = 0; // size of "sets" and "neg"; parallel arrays

    for (Query q : queries) {
      if (q instanceof ExtendedQuery eq) {
        if (!eq.getCache()) {
          if (eq.getCost() >= 100 && eq instanceof PostFilter) {
            if (postFilters == null) postFilters = new ArrayList<>(sets.length - end);
            postFilters.add((PostFilter) q);
          } else {
            if (notCached == null) notCached = new ArrayList<>(sets.length - end);
            notCached.add((ExtendedQuery) q);
          }
          continue;
        }
      }

      if (filterCache == null) {
        // there is no cache: don't pull bitsets
        if (notCached == null) notCached = new ArrayList<>(sets.length - end);
        WrappedQuery uncached = new WrappedQuery(q);
        uncached.setCache(false);
        notCached.add(uncached);
        continue;
      }

      Query posQuery = QueryUtils.getAbs(q);
      DocSet docSet = getPositiveDocSet(posQuery);
      // Negative query if absolute value different from original
      if (Objects.equals(q, posQuery)) {
        // keep track of the smallest positive set; use "answer" for this.
        if (answer == null) {
          answer = docSet;
          continue;
        }
        // note: assume that size() is cached.  It generally comes from the cache, so should be.
        if (docSet.size() < answer.size()) {
          // swap answer & docSet so that answer is smallest
          DocSet tmp = answer;
          answer = docSet;
          docSet = tmp;
        }
        neg[end] = false;
      } else {
        neg[end] = true;
      }
      sets[end++] = docSet;
    } // end of queries

    if (end > 0) {
      // Are all of our normal cached filters negative?
      if (answer == null) {
        answer = getLiveDocSet();
      }

      // This optimizes for the case where we have more than 2 filters and instead
      // of copying the bitsets we make one mutable bitset. We should only do this
      // for BitDocSet since it clones the backing bitset for andNot and intersection.
      if (end > 1 && answer instanceof BitDocSet) {
        answer = MutableBitDocSet.fromBitDocSet((BitDocSet) answer);
      }

      // do negative queries first to shrink set size
      for (int i = 0; i < end; i++) {
        if (neg[i]) answer = answer.andNot(sets[i]);
      }

      for (int i = 0; i < end; i++) {
        if (!neg[i]) answer = answer.intersection(sets[i]);
      }

      // Make sure to keep answer as an immutable DocSet if we made it mutable
      answer = MutableBitDocSet.unwrapIfMutable(answer);
    }

    // ignore "answer" if it simply matches all docs
    if (answer != null && answer.size() == numDocs()) {
      answer = null;
    }

    // answer is done.

    // If no notCached nor postFilters, we can return now.
    if (notCached == null && postFilters == null) {
      // "answer" is the only part of the filter, so set it.
      if (answer != null) {
        pf.answer = answer;
        pf.filter = answer.makeQuery();
      }
      return pf;
    }
    // pf.answer will remain null ...  (our local "answer" var is not the complete answer)

    // Set pf.filter based on combining "answer" and "notCached"
    if (notCached == null) {
      if (answer != null) {
        pf.filter = answer.makeQuery();
      }
    } else {
      notCached.sort(sortByCost); // pointless?
      final BooleanQuery.Builder builder = new BooleanQuery.Builder();
      if (answer != null) {
        builder.add(answer.makeQuery(), Occur.FILTER);
      }
      for (ExtendedQuery eq : notCached) {
        Query q = eq.getCostAppliedQuery();
        builder.add(q, Occur.FILTER);
      }
      pf.filter = builder.build();
    }

    // Set pf.postFilter
    if (postFilters != null) {
      postFilters.sort(sortByCost);
      for (int i = postFilters.size() - 1; i >= 0; i--) {
        DelegatingCollector prev = pf.postFilter;
        pf.postFilter = postFilters.get(i).getFilterCollector(this);
        if (prev != null) pf.postFilter.setDelegate(prev);
      }
    }

    return pf;
  }

  /**
   * @lucene.internal
   */
  public DocSet getDocSet(DocsEnumState deState) throws IOException {
    int largestPossible = deState.termsEnum.docFreq();
    boolean useCache = filterCache != null && largestPossible >= deState.minSetSizeCached;

    if (useCache) {
      TermQuery key = new TermQuery(new Term(deState.fieldName, deState.termsEnum.term()));
      return filterCache.computeIfAbsent(
          key,
          (IOFunction<? super Query, ? extends DocSet>) k -> getResult(deState, largestPossible));
    }

    return getResult(deState, largestPossible);
  }

  private DocSet getResult(DocsEnumState deState, int largestPossible) throws IOException {
    int smallSetSize = DocSetUtil.smallSetSize(maxDoc());
    int scratchSize = Math.min(smallSetSize, largestPossible);
    if (deState.scratch == null || deState.scratch.length < scratchSize)
      deState.scratch = new int[scratchSize];

    final int[] docs = deState.scratch;
    int upto = 0;
    int bitsSet = 0;
    FixedBitSet fbs = null;

    PostingsEnum postingsEnum = deState.termsEnum.postings(deState.postingsEnum, PostingsEnum.NONE);
    postingsEnum = BitsFilteredPostingsEnum.wrap(postingsEnum, deState.liveDocs);
    if (deState.postingsEnum == null) {
      deState.postingsEnum = postingsEnum;
    }

    if (postingsEnum instanceof MultiPostingsEnum) {
      MultiPostingsEnum.EnumWithSlice[] subs = ((MultiPostingsEnum) postingsEnum).getSubs();
      int numSubs = ((MultiPostingsEnum) postingsEnum).getNumSubs();
      for (int subindex = 0; subindex < numSubs; subindex++) {
        MultiPostingsEnum.EnumWithSlice sub = subs[subindex];
        if (sub.postingsEnum == null) continue;
        int base = sub.slice.start;
        int docid;

        if (largestPossible > docs.length) {
          if (fbs == null) fbs = new FixedBitSet(maxDoc());
          while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            fbs.set(docid + base);
            bitsSet++;
          }
        } else {
          while ((docid = sub.postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            docs[upto++] = docid + base;
          }
        }
      }
    } else {
      int docid;
      if (largestPossible > docs.length) {
        fbs = new FixedBitSet(maxDoc());
        while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          fbs.set(docid);
          bitsSet++;
        }
      } else {
        while ((docid = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          docs[upto++] = docid;
        }
      }
    }

    DocSet result;
    if (fbs != null) {
      for (int i = 0; i < upto; i++) {
        fbs.set(docs[i]);
      }
      bitsSet += upto;
      result = new BitDocSet(fbs, bitsSet);
    } else {
      result = upto == 0 ? DocSet.empty() : new SortedIntDocSet(Arrays.copyOf(docs, upto));
    }
    return result;
  }

  // query must be positive
  protected DocSet getDocSetNC(Query query, DocSet filter) throws IOException {
    return DocSetUtil.createDocSet(this, query, filter);
  }

  /**
   * Returns the set of document ids matching both the query. This method is cache-aware and
   * attempts to retrieve a DocSet of the query from the cache if possible. If the answer was not
   * cached, it may have been inserted into the cache as a result of this call.
   *
   * @return Non-null DocSet meeting the specified criteria. Should <b>not</b> be modified by the
   *     caller.
   * @see #getDocSet(Query,DocSet)
   */
  public DocSet getDocSet(Query query) throws IOException {
    return getDocSet(query, null);
  }

  /**
   * Returns the set of document ids matching both the query and the filter. This method is
   * cache-aware and attempts to retrieve a DocSet of the query from the cache if possible. If the
   * answer was not cached, it may have been inserted into the cache as a result of this call.
   *
   * @param filter may be null if none
   * @return Non-null DocSet meeting the specified criteria. Should <b>not</b> be modified by the
   *     caller.
   */
  public DocSet getDocSet(Query query, DocSet filter) throws IOException {
    boolean doCache = filterCache != null;
    if (query instanceof ExtendedQuery) {
      if (!((ExtendedQuery) query).getCache()) {
        doCache = false;
      }
      if (query instanceof WrappedQuery) {
        query = ((WrappedQuery) query).getWrappedQuery();
      }
    }

    if (!doCache) {
      query = QueryUtils.makeQueryable(query);
      return getDocSetNC(query, filter);
    }

    // Get the absolute value (positive version) of this query. If we
    // get back the same reference, we know it's positive.
    Query absQ = QueryUtils.getAbs(query);
    boolean positive = Objects.equals(absQ, query);

    DocSet absAnswer = getAndCacheDocSet(absQ);

    if (filter == null) {
      return positive ? absAnswer : getLiveDocSet().andNot(absAnswer);
    } else {
      return positive ? absAnswer.intersection(filter) : filter.andNot(absAnswer);
    }
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code> and sorted by <code>
   * sort</code>.
   *
   * <p>This method is cache aware and may retrieve <code>filter</code> from the cache or make an
   * insertion into the cache as a result of this call.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param filter may be null
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocList getDocList(Query query, Query filter, Sort lsort, int offset, int len)
      throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setFilterList(filter)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .search(this)
        .getDocList();
  }

  /**
   * Returns documents matching both <code>query</code> and the intersection of the <code>filterList
   * </code>, sorted by <code>sort</code>.
   *
   * <p>This method is cache aware and may retrieve <code>filter</code> from the cache or make an
   * insertion into the cache as a result of this call.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param filterList may be null
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocList getDocList(
      Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags)
      throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setFilterList(filterList)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .setFlags(flags)
        .search(this)
        .getDocList();
  }

  public static final int NO_CHECK_QCACHE = 0x80000000;
  public static final int GET_DOCSET = 0x40000000;
  static final int NO_CHECK_FILTERCACHE = 0x20000000;
  static final int NO_SET_QCACHE = 0x10000000;
  static final int SEGMENT_TERMINATE_EARLY = 0x08;
  public static final int TERMINATE_EARLY = 0x04;
  public static final int GET_DOCLIST = 0x02; // get the documents actually returned in a response
  public static final int GET_SCORES = 0x01;

  private static boolean sortIncludesOtherThanScore(final Sort sort) {
    if (sort == null) {
      return false;
    }
    final SortField[] sortFields = sort.getSort();
    return sortFields.length > 1 || sortFields[0].getType() != Type.SCORE;
  }

  private boolean useFilterCacheForDynamicScoreQuery(boolean needSort, QueryCommand cmd) {
    if (!useFilterForSortedQuery) {
      // under no circumstance use filterCache
      return false;
    } else if (!needSort) {
      // if don't need to sort at all, doesn't matter whether score would be needed
      return true;
    } else {
      // we _do_ need to sort; only use filterCache if `score` is not a factor for sort
      final Sort sort = cmd.getSort();
      if (sort == null) {
        // defaults to sort-by-score, so can't use filterCache
        return false;
      } else {
        return Arrays.stream(sort.getSort())
            .noneMatch((sf) -> sf.getType() == SortField.Type.SCORE);
      }
    }
  }

  /**
   * getDocList version that uses+populates query and filter caches. In the event of a timeout, the
   * cache is not populated.
   */
  private QueryResult getDocListC(QueryResult qr, QueryCommand cmd) throws IOException {
    // TODO don't take QueryResult as arg; create one here
    if (cmd.getSegmentTerminateEarly()) {
      qr.setSegmentTerminatedEarly(Boolean.FALSE);
    }
    DocListAndSet out = new DocListAndSet();
    qr.setDocListAndSet(out);
    QueryResultKey key = null;
    int maxDocRequested = cmd.getOffset() + cmd.getLen();
    // check for overflow, and check for # docs in index
    if (maxDocRequested < 0 || maxDocRequested > maxDoc()) maxDocRequested = maxDoc();
    int supersetMaxDoc = maxDocRequested;
    DocList superset = null;

    int flags = cmd.getFlags();
    Query q = cmd.getQuery();
    if (q instanceof ExtendedQuery eq) {
      if (!eq.getCache()) {
        flags |= (NO_CHECK_QCACHE | NO_SET_QCACHE | NO_CHECK_FILTERCACHE);
      }
    }

    // we can try and look up the complete query in the cache.
    if (queryResultCache != null
        && (flags & (NO_CHECK_QCACHE | NO_SET_QCACHE)) != ((NO_CHECK_QCACHE | NO_SET_QCACHE))) {
      // all the current flags can be reused during warming,
      // so set all of them on the cache key.
      key =
          new QueryResultKey(
              q,
              cmd.getFilterList(),
              cmd.getSort(),
              flags,
              cmd.getMinExactCount(),
              cmd.isDistribStatsDisabled());
      if ((flags & NO_CHECK_QCACHE) == 0) {
        superset = queryResultCache.get(key);

        if (superset != null) {
          // check that the cache entry has scores recorded if we need them
          if ((flags & GET_SCORES) == 0 || superset.hasScores()) {
            // NOTE: subset() returns null if the DocList has fewer docs than
            // requested
            out.docList = superset.subset(cmd.getOffset(), cmd.getLen());
          }
        }
        if (out.docList != null) {
          // found the docList in the cache... now check if we need the docset too.
          // OPT: possible future optimization - if the doclist contains all the matches,
          // use it to make the docset instead of rerunning the query.
          if (out.docSet == null && ((flags & GET_DOCSET) != 0)) {
            if (cmd.getFilterList() == null) {
              out.docSet = getDocSet(cmd.getQuery());
            } else {
              List<Query> newList = new ArrayList<>(cmd.getFilterList().size() + 1);
              newList.add(cmd.getQuery());
              newList.addAll(cmd.getFilterList());
              out.docSet = getDocSet(newList);
            }
          }
          return qr;
        }
      }

      // If we are going to generate the result, bump up to the
      // next resultWindowSize for better caching.

      if ((flags & NO_SET_QCACHE) == 0) {
        // handle 0 special case as well as avoid idiv in the common case.
        if (maxDocRequested < queryResultWindowSize) {
          supersetMaxDoc = queryResultWindowSize;
        } else {
          supersetMaxDoc =
              ((maxDocRequested - 1) / queryResultWindowSize + 1) * queryResultWindowSize;
          if (supersetMaxDoc < 0) supersetMaxDoc = maxDocRequested;
        }
      } else {
        key = null; // we won't be caching the result
      }
    }
    cmd.setSupersetMaxDoc(supersetMaxDoc);

    // OK, so now we need to generate an answer.
    // One way to do that would be to check if we have an unordered list
    // of results for the base query. If so, we can apply the filters and then
    // sort by the resulting set. This can only be used if:
    // - the sort doesn't contain score
    // - we don't want score returned.

    // check if we should try and use the filter cache
    final boolean needSort;
    final boolean useFilterCache;
    if ((flags & (GET_SCORES | NO_CHECK_FILTERCACHE)) != 0 || filterCache == null) {
      needSort = true; // this value should be irrelevant when `useFilterCache=false`
      useFilterCache = false;
    } else if (q instanceof MatchAllDocsQuery
        || (useFilterForSortedQuery && QueryUtils.isConstantScoreQuery(q))) {
      // special-case MatchAllDocsQuery: implicit default useFilterForSortedQuery=true;
      // otherwise, default behavior should not risk filterCache thrashing, so require
      // `useFilterForSortedQuery==true`

      // We only need to sort if we're returning results AND sorting by something other than SCORE
      // (sort by "score" alone is pointless for these constant score queries)
      final Sort sort = cmd.getSort();
      needSort = cmd.getLen() > 0 && sortIncludesOtherThanScore(sort);
      if (!needSort) {
        useFilterCache = true;
      } else {
        /*
        NOTE: if `sort:score` is specified, it will have no effect, so we really _could_ in
        principle always use filterCache; but this would be a user request misconfiguration,
        and supporting it would require us to mess with user sort, or ignore the fact that sort
        expects `score` to be present ... so just make the optimization contingent on the absence
        of `score` in the requested sort.
         */
        useFilterCache =
            Arrays.stream(sort.getSort()).noneMatch((sf) -> sf.getType() == SortField.Type.SCORE);
      }
    } else {
      // for non-constant-score queries, must sort unless no docs requested
      needSort = cmd.getLen() > 0;
      useFilterCache = useFilterCacheForDynamicScoreQuery(needSort, cmd);
    }

    if (useFilterCache) {
      // now actually use the filter cache.
      // for large filters that match few documents, this may be
      // slower than simply re-executing the query.
      if (out.docSet == null) {
        out.docSet = getDocSet(cmd.getQuery());
        List<Query> filterList = cmd.getFilterList();
        if (filterList != null && !filterList.isEmpty()) {
          out.docSet = DocSetUtil.getDocSet(out.docSet.intersection(getDocSet(filterList)), this);
        }
      }
      // todo: there could be a sortDocSet that could take a list of
      // the filters instead of anding them first...
      // perhaps there should be a multi-docset-iterator
      if (needSort) {
        fullSortCount.increment();
        sortDocSet(qr, cmd);
      } else {
        skipSortCount.increment();
        // put unsorted list in place
        out.docList = constantScoreDocList(cmd.getOffset(), cmd.getLen(), out.docSet);
        if (0 == cmd.getSupersetMaxDoc()) {
          // this is the only case where `cursorMark && !needSort`
          qr.setNextCursorMark(cmd.getCursorMark());
        } else {
          // cursorMark should always add a `uniqueKey` sort field tie-breaker, which
          // should prevent `needSort` from ever being false in conjunction with
          // cursorMark, _except_ in the event of `rows=0` (accounted for in the clause
          // above)
          assert cmd.getCursorMark() == null;
        }
      }
    } else {
      fullSortCount.increment();
      // do it the normal way...
      if ((flags & GET_DOCSET) != 0) {
        // this currently conflates returning the docset for the base query vs
        // the base query and all filters.
        DocSet qDocSet = getDocListAndSetNC(qr, cmd);
        // cache the docSet matching the query w/o filtering
        if (qDocSet != null && filterCache != null && !qr.isPartialResults())
          filterCache.put(cmd.getQuery(), qDocSet);
      } else {
        getDocListNC(qr, cmd);
      }
      assert null != out.docList : "docList is null";
    }

    if (null == cmd.getCursorMark()) {
      // Kludge...
      // we can't use DocSlice.subset, even though it should be an identity op
      // because it gets confused by situations where there are lots of matches, but
      // less docs in the slice then were requested, (due to the cursor)
      // so we have to short circuit the call.
      // None of which is really a problem since we can't use caching with
      // cursors anyway, but it still looks weird to have to special case this
      // behavior based on this condition - hence the long explanation.
      superset = out.docList;
      out.docList = superset.subset(cmd.getOffset(), cmd.getLen());
    } else {
      // sanity check our cursor assumptions
      assert null == superset : "cursor: superset isn't null";
      assert 0 == cmd.getOffset() : "cursor: command offset mismatch";
      assert 0 == out.docList.offset() : "cursor: docList offset mismatch";
      assert cmd.getLen() >= supersetMaxDoc
          : "cursor: superset len mismatch: " + cmd.getLen() + " vs " + supersetMaxDoc;
    }

    // lastly, put the superset in the cache if the size is less than or equal
    // to queryResultMaxDocsCached
    if (key != null && superset.size() <= queryResultMaxDocsCached && !qr.isPartialResults()) {
      queryResultCache.put(key, superset);
    }
    return qr;
  }

  private Relation populateScoresIfNeeded(
      QueryCommand cmd, boolean needScores, TopDocs topDocs, Query query, ScoreMode scoreModeUsed)
      throws IOException {
    if (cmd.getSort() != null && !(cmd.getQuery() instanceof RankQuery) && needScores) {
      TopFieldCollector.populateScores(topDocs.scoreDocs, this, query);
    }
    if (scoreModeUsed == ScoreMode.COMPLETE || scoreModeUsed == ScoreMode.COMPLETE_NO_SCORES) {
      return TotalHits.Relation.EQUAL_TO;
    } else {
      return topDocs.totalHits.relation;
    }
  }

  /**
   * Helper method for extracting the {@link FieldDoc} sort values from a {@link TopFieldDocs} when
   * available and making the appropriate call to {@link QueryResult#setNextCursorMark} when
   * applicable.
   *
   * @param qr <code>QueryResult</code> to modify
   * @param qc <code>QueryCommand</code> for context of method
   * @param topDocs May or may not be a <code>TopFieldDocs</code>
   */
  private void populateNextCursorMarkFromTopDocs(QueryResult qr, QueryCommand qc, TopDocs topDocs) {
    // TODO: would be nice to rename & generalize this method for non-cursor cases...
    // ...would be handy to reuse the ScoreDoc/FieldDoc sort vals directly in distrib sort
    // ...but that has non-trivial queryResultCache implications
    // See: SOLR-5595

    if (null == qc.getCursorMark()) {
      // nothing to do, short circuit out
      return;
    }

    final CursorMark lastCursorMark = qc.getCursorMark();

    // if we have a cursor, then we have a sort that at minimum involves uniqueKey..
    // so we must have a TopFieldDocs containing FieldDoc[]
    assert topDocs instanceof TopFieldDocs : "TopFieldDocs cursor constraint violated";
    final TopFieldDocs topFieldDocs = (TopFieldDocs) topDocs;
    final ScoreDoc[] scoreDocs = topFieldDocs.scoreDocs;

    if (0 == scoreDocs.length) {
      // no docs on this page, re-use existing cursor mark
      qr.setNextCursorMark(lastCursorMark);
    } else {
      ScoreDoc lastDoc = scoreDocs[scoreDocs.length - 1];
      assert lastDoc instanceof FieldDoc : "FieldDoc cursor constraint violated";

      List<Object> lastFields = Arrays.<Object>asList(((FieldDoc) lastDoc).fields);
      CursorMark nextCursorMark = lastCursorMark.createNext(lastFields);
      assert null != nextCursorMark : "null nextCursorMark";
      qr.setNextCursorMark(nextCursorMark);
    }
  }

  /**
   * Helper method for inspecting QueryCommand and creating the appropriate {@link TopDocsCollector}
   *
   * @param len the number of docs to return
   * @param cmd The Command whose properties should determine the type of TopDocsCollector to use.
   */
  TopDocsCollector<? extends ScoreDoc> buildTopDocsCollector(int len, QueryCommand cmd)
      throws IOException {
    int minNumFound = cmd.getMinExactCount();
    Query q = cmd.getQuery();
    if (q instanceof RankQuery rq) {
      return rq.getTopDocsCollector(len, cmd, this);
    }

    if (null == cmd.getSort()) {
      assert null == cmd.getCursorMark() : "have cursor but no sort";
      return TopScoreDocCollector.create(len, minNumFound);
    } else {
      // we have a sort
      final Sort weightedSort = weightSort(cmd.getSort());
      final CursorMark cursor = cmd.getCursorMark();

      final FieldDoc searchAfter = (null != cursor ? cursor.getSearchAfterFieldDoc() : null);
      return TopFieldCollector.create(weightedSort, len, searchAfter, minNumFound);
    }
  }

  private void getDocListNC(QueryResult qr, QueryCommand cmd) throws IOException {
    final int len = cmd.getSupersetMaxDoc();
    int last = len;
    if (last < 0 || last > maxDoc()) last = maxDoc();
    final int lastDocRequested = last;
    int totalHits;
    final float maxScore;
    final DocList docList;

    final boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;

    final ProcessedFilter pf = getProcessedFilter(cmd.getFilterList());
    final Query query =
        QueryUtils.combineQueryAndFilter(QueryUtils.makeQueryable(cmd.getQuery()), pf.filter);
    final Relation hitsRelation;

    // handle zero case...
    if (lastDocRequested <= 0) {
      final float[] topscore = new float[] {Float.NEGATIVE_INFINITY};
      final int[] numHits = new int[1];

      final Collector collector;

      if (!needScores) {
        collector = new TotalHitCountCollector();
      } else {
        collector =
            new SimpleCollector() {
              Scorable scorer;

              @Override
              public void setScorer(Scorable scorer) {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                numHits[0]++;
                float score = scorer.score();
                if (score > topscore[0]) topscore[0] = score;
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE;
              }
            };
      }

      buildAndRunCollectorChain(qr, query, collector, cmd, pf.postFilter);

      totalHits = numHits[0];
      if (collector instanceof TotalHitCountCollector) {
        totalHits = ((TotalHitCountCollector) collector).getTotalHits();
      } else {
        totalHits = numHits[0];
      }
      maxScore = totalHits > 0 ? topscore[0] : 0.0f;
      docList =
          new DocSlice(
              0, 0, new int[0], new float[0], totalHits, maxScore, TotalHits.Relation.EQUAL_TO);
      // no docs on this page, so cursor doesn't change
      qr.setNextCursorMark(cmd.getCursorMark());
    } else {
      if (log.isDebugEnabled()) {
        log.debug("calling from 2, query: {}", query.getClass());
      }
      final TopDocs topDocs;
      final ScoreMode scoreModeUsed;
      if (!MultiThreadedSearcher.allowMT(pf.postFilter, cmd)) {
        log.trace("SINGLE THREADED search, skipping collector manager in getDocListNC");
        final TopDocsCollector<?> topCollector = buildTopDocsCollector(len, cmd);
        MaxScoreCollector maxScoreCollector = null;
        Collector collector = topCollector;
        if (needScores) {
          maxScoreCollector = new MaxScoreCollector();
          collector = MultiCollector.wrap(topCollector, maxScoreCollector);
        }
        scoreModeUsed =
            buildAndRunCollectorChain(qr, query, collector, cmd, pf.postFilter).scoreMode();

        totalHits = topCollector.getTotalHits();
        topDocs = topCollector.topDocs(0, len);

        maxScore =
            totalHits > 0
                ? (maxScoreCollector == null ? Float.NaN : maxScoreCollector.getMaxScore())
                : 0.0f;
      } else {
        log.trace("MULTI-THREADED search, using CollectorManager int getDocListNC");
        final MultiThreadedSearcher.SearchResult searchResult =
            new MultiThreadedSearcher(this)
                .searchCollectorManagers(len, cmd, query, true, needScores, false, qr);
        scoreModeUsed = searchResult.scoreMode;

        MultiThreadedSearcher.TopDocsResult topDocsResult = searchResult.getTopDocsResult();
        totalHits = topDocsResult.totalHits;
        topDocs = topDocsResult.topDocs;
        maxScore = searchResult.getMaxScore(totalHits);
      }

      hitsRelation = populateScoresIfNeeded(cmd, needScores, topDocs, query, scoreModeUsed);
      populateNextCursorMarkFromTopDocs(qr, cmd, topDocs);

      int nDocsReturned = topDocs.scoreDocs.length;
      int sliceLen = Math.min(lastDocRequested, nDocsReturned);
      docList =
          new TopDocsSlice(0, sliceLen, topDocs, totalHits, needScores, maxScore, hitsRelation);
    }
    qr.setDocList(docList);
  }

  // any DocSet returned is for the query only, without any filtering... that way it may
  // be cached if desired.
  private DocSet getDocListAndSetNC(QueryResult qr, QueryCommand cmd) throws IOException {
    final int len = cmd.getSupersetMaxDoc();
    int last = len;
    if (last < 0 || last > maxDoc()) last = maxDoc();
    final int lastDocRequested = last;
    final int nDocsReturned;
    final int totalHits;
    final float maxScore;
    final DocSet set;
    final DocList docList;

    final boolean needScores = (cmd.getFlags() & GET_SCORES) != 0;
    final int maxDoc = maxDoc();
    cmd.setMinExactCount(Integer.MAX_VALUE); // We need the full DocSet

    final ProcessedFilter pf = getProcessedFilter(cmd.getFilterList());
    final Query query =
        QueryUtils.combineQueryAndFilter(QueryUtils.makeQueryable(cmd.getQuery()), pf.filter);

    // handle zero case...
    if (lastDocRequested <= 0) {
      final float[] topscore = new float[] {Float.NEGATIVE_INFINITY};

      final Collector collector;
      final DocSetCollector setCollector = new DocSetCollector(maxDoc);

      if (!needScores) {
        collector = setCollector;
      } else {
        final Collector topScoreCollector =
            new SimpleCollector() {

              Scorable scorer;

              @Override
              public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
              }

              @Override
              public void collect(int doc) throws IOException {
                float score = scorer.score();
                if (score > topscore[0]) topscore[0] = score;
              }

              @Override
              public ScoreMode scoreMode() {
                return ScoreMode.TOP_SCORES;
              }
            };

        collector = MultiCollector.wrap(setCollector, topScoreCollector);
      }

      buildAndRunCollectorChain(qr, query, collector, cmd, pf.postFilter);

      set = DocSetUtil.getDocSet(setCollector, this);

      maxScore = set.size() > 0 ? topscore[0] : 0.0f;
      docList =
          new DocSlice(
              0, 0, new int[0], new float[0], set.size(), maxScore, TotalHits.Relation.EQUAL_TO);
      // no docs on this page, so cursor doesn't change
      qr.setNextCursorMark(cmd.getCursorMark());
    } else {
      final TopDocs topDocs;
      if (!MultiThreadedSearcher.allowMT(pf.postFilter, cmd)) {
        log.trace("SINGLE THREADED search, skipping collector manager in getDocListAndSetNC");

        @SuppressWarnings({"rawtypes"})
        final TopDocsCollector<? extends ScoreDoc> topCollector = buildTopDocsCollector(len, cmd);
        final DocSetCollector setCollector = new DocSetCollector(maxDoc);
        MaxScoreCollector maxScoreCollector = null;
        List<Collector> collectors = new ArrayList<>(Arrays.asList(topCollector, setCollector));

        if (needScores) {
          maxScoreCollector = new MaxScoreCollector();
          collectors.add(maxScoreCollector);
        }

        Collector collector = MultiCollector.wrap(collectors);

        buildAndRunCollectorChain(qr, query, collector, cmd, pf.postFilter);

        set = DocSetUtil.getDocSet(setCollector, this);

        totalHits = topCollector.getTotalHits();
        assert (totalHits == set.size()) || qr.isPartialResults();

        topDocs = topCollector.topDocs(0, len);
        maxScore =
            totalHits > 0
                ? (maxScoreCollector == null ? Float.NaN : maxScoreCollector.getMaxScore())
                : 0.0f;
      } else {
        log.trace("MULTI-THREADED search, using CollectorManager in getDocListAndSetNC");

        MultiThreadedSearcher.SearchResult searchResult =
            new MultiThreadedSearcher(this)
                .searchCollectorManagers(len, cmd, query, true, needScores, true, qr);
        MultiThreadedSearcher.TopDocsResult topDocsResult = searchResult.getTopDocsResult();
        totalHits = topDocsResult.totalHits;
        topDocs = topDocsResult.topDocs;
        maxScore = searchResult.getMaxScore(totalHits);
        set = new BitDocSet(searchResult.getFixedBitSet());
        // TODO: Think about using ScoreMode from searchResult down below
      }
      final Relation relation =
          populateScoresIfNeeded(cmd, needScores, topDocs, query, ScoreMode.COMPLETE);
      populateNextCursorMarkFromTopDocs(qr, cmd, topDocs);
      nDocsReturned = topDocs.scoreDocs.length;
      int sliceLen = Math.min(lastDocRequested, nDocsReturned);
      docList = new TopDocsSlice(0, sliceLen, topDocs, totalHits, needScores, maxScore, relation);
    }

    qr.setDocList(docList);
    // TODO: if we collect results before the filter, we just need to intersect with
    // that filter to generate the DocSet for qr.setDocSet()
    qr.setDocSet(set);

    // TODO: currently we don't generate the DocSet for the base query,
    // but the QueryDocSet == CompleteDocSet if filter==null.
    return pf.filter == null && pf.postFilter == null ? qr.getDocSet() : null;
  }

  /**
   * Returns documents matching <code>query</code>, sorted by <code>sort</code>.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @return DocList meeting the specified criteria, should <b>not</b> be modified by the caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocList getDocList(Query query, Sort lsort, int offset, int len) throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .search(this)
        .getDocList();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code> and sorted by <code>
   * sort</code>. Also returns the complete set of documents matching <code>query</code> and <code>
   * filter</code> (regardless of <code>offset</code> and <code>len</code>).
   *
   * <p>This method is cache aware and may retrieve <code>filter</code> from the cache or make an
   * insertion into the cache as a result of this call.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * <p>The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param filter may be null
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the
   *     caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocListAndSet getDocListAndSet(Query query, Query filter, Sort lsort, int offset, int len)
      throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setFilterList(filter)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .setNeedDocSet(true)
        .search(this)
        .getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and <code>filter</code> and sorted by <code>
   * sort</code>. Also returns the compete set of documents matching <code>query</code> and <code>
   * filter</code> (regardless of <code>offset</code> and <code>len</code>).
   *
   * <p>This method is cache aware and may retrieve <code>filter</code> from the cache or make an
   * insertion into the cache as a result of this call.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * <p>The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param filter may be null
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @param flags user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the
   *     caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocListAndSet getDocListAndSet(
      Query query, Query filter, Sort lsort, int offset, int len, int flags) throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setFilterList(filter)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .setFlags(flags)
        .setNeedDocSet(true)
        .search(this)
        .getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and the intersection of <code>filterList
   * </code>, sorted by <code>sort</code>. Also returns the compete set of documents matching <code>
   * query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len</code>).
   *
   * <p>This method is cache aware and may retrieve <code>filter</code> from the cache or make an
   * insertion into the cache as a result of this call.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * <p>The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param filterList may be null
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the
   *     caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocListAndSet getDocListAndSet(
      Query query, List<Query> filterList, Sort lsort, int offset, int len) throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setFilterList(filterList)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .setNeedDocSet(true)
        .search(this)
        .getDocListAndSet();
  }

  /**
   * Returns documents matching both <code>query</code> and the intersection of <code>filterList
   * </code>, sorted by <code>sort</code>. Also returns the complete set of documents matching
   * <code>query</code> and <code>filter</code> (regardless of <code>offset</code> and <code>len
   * </code>).
   *
   * <p>This method is cache aware and may retrieve filters from the cache or make an insertion into
   * the cache as a result of this call.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * <p>The DocList and DocSet returned should <b>not</b> be modified.
   *
   * @param filterList may be null
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @param flags user supplied flags for the result set
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the
   *     caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocListAndSet getDocListAndSet(
      Query query, List<Query> filterList, Sort lsort, int offset, int len, int flags)
      throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setFilterList(filterList)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .setFlags(flags)
        .setNeedDocSet(true)
        .search(this)
        .getDocListAndSet();
  }

  /**
   * Returns the top documents matching the <code>query</code> and sorted by <code>
   * sort</code>, limited by <code>offset</code> and <code>len</code>. Also returns compete set of
   * matching documents as a {@link DocSet}.
   *
   * <p>FUTURE: The returned DocList may be retrieved from a cache.
   *
   * @param lsort criteria by which to sort (if null, query relevance is used)
   * @param offset offset into the list of documents to return
   * @param len maximum number of documents to return
   * @return DocListAndSet meeting the specified criteria, should <b>not</b> be modified by the
   *     caller.
   * @throws IOException If there is a low-level I/O error.
   */
  @Deprecated
  public DocListAndSet getDocListAndSet(Query query, Sort lsort, int offset, int len)
      throws IOException {
    return new QueryCommand()
        .setQuery(query)
        .setSort(lsort)
        .setOffset(offset)
        .setLen(len)
        .setNeedDocSet(true)
        .search(this)
        .getDocListAndSet();
  }

  private DocList constantScoreDocList(int offset, int length, DocSet docs) {
    final int size = docs.size();

    // NOTE: it would be possible to special-case `length == 0 || size <= offset` here
    // (returning a DocList backed by an empty array) -- but the cases that would practically
    // benefit from doing so would be extremely unusual, and likely pathological:
    //   1. length==0 in conjunction with offset>0 (why?)
    //   2. specifying offset>size (paging beyond end of results)
    // This would require special consideration in dealing with cache handling (and generation
    // of the final DocList via `DocSlice.subset(int, int)`), and it's just not worth it.

    final int returnSize = Math.min(offset + length, size);
    final int[] docIds = new int[returnSize];

    final DocIterator iter = docs.iterator();
    for (int i = 0; i < returnSize; i++) {
      docIds[i] = iter.nextDoc();
    }
    return new DocSlice(0, returnSize, docIds, null, size, 0f, TotalHits.Relation.EQUAL_TO);
  }

  protected void sortDocSet(QueryResult qr, QueryCommand cmd) throws IOException {
    DocSet set = qr.getDocListAndSet().docSet;
    int nDocs = cmd.getSupersetMaxDoc();
    if (nDocs == 0) {
      // SOLR-2923
      qr.getDocListAndSet().docList =
          new DocSlice(0, 0, new int[0], null, set.size(), 0f, TotalHits.Relation.EQUAL_TO);
      qr.setNextCursorMark(cmd.getCursorMark());
      return;
    }

    TopDocsCollector<? extends ScoreDoc> topCollector = buildTopDocsCollector(nDocs, cmd);

    DocIterator iter = set.iterator();
    int base = 0;
    int end = 0;
    int readerIndex = 0;

    LeafCollector leafCollector = null;
    while (iter.hasNext()) {
      int doc = iter.nextDoc();
      while (doc >= end) {
        LeafReaderContext leaf = leafContexts.get(readerIndex++);
        base = leaf.docBase;
        end = base + leaf.reader().maxDoc();
        leafCollector = topCollector.getLeafCollector(leaf);
        // we should never need to set the scorer given the settings for the collector
      }
      leafCollector.collect(doc - base);
    }

    TopDocs topDocs = topCollector.topDocs(0, nDocs);

    int nDocsReturned = topDocs.scoreDocs.length;
    int[] ids = new int[nDocsReturned];

    for (int i = 0; i < nDocsReturned; i++) {
      ScoreDoc scoreDoc = topDocs.scoreDocs[i];
      ids[i] = scoreDoc.doc;
    }

    assert topDocs.totalHits.relation == TotalHits.Relation.EQUAL_TO;
    qr.getDocListAndSet().docList =
        new DocSlice(
            0, nDocsReturned, ids, null, topDocs.totalHits.value, 0.0f, topDocs.totalHits.relation);
    populateNextCursorMarkFromTopDocs(qr, cmd, topDocs);
  }

  /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   *
   * <p>This method is cache-aware and may check as well as modify the cache.
   *
   * @return the number of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException If there is a low-level I/O error.
   */
  public int numDocs(Query a, DocSet b) throws IOException {
    if (b.size() == 0) {
      return 0;
    }
    if (filterCache != null) {
      // Negative query if absolute value different from original
      Query absQ = QueryUtils.getAbs(a);
      DocSet positiveA = getPositiveDocSet(absQ);
      return Objects.equals(a, absQ) ? b.intersectionSize(positiveA) : b.andNotSize(positiveA);
    } else {
      // If there isn't a cache, then do a single filtered query
      TotalHitCountCollector collector = new TotalHitCountCollector();
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(QueryUtils.makeQueryable(a), Occur.MUST);
      bq.add(b.makeQuery(), Occur.MUST);
      super.search(bq.build(), collector);
      return collector.getTotalHits();
    }
  }

  /**
   * @lucene.internal
   */
  public int numDocs(DocSet a, DocsEnumState deState) throws IOException {
    // Negative query if absolute value different from original
    return a.intersectionSize(getDocSet(deState));
  }

  public static class DocsEnumState {
    public String fieldName; // currently interned for as long as lucene requires it
    public TermsEnum termsEnum;
    public Bits liveDocs;
    public PostingsEnum postingsEnum;

    public int minSetSizeCached;

    public int[] scratch;
  }

  /**
   * Returns the number of documents that match both <code>a</code> and <code>b</code>.
   *
   * <p>This method is cache-aware and may check as well as modify the cache.
   *
   * @return the number of documents in the intersection between <code>a</code> and <code>b</code>.
   * @throws IOException If there is a low-level I/O error.
   */
  public int numDocs(Query a, Query b) throws IOException {
    Query absA = QueryUtils.getAbs(a);
    Query absB = QueryUtils.getAbs(b);
    DocSet positiveA = getPositiveDocSet(absA);
    DocSet positiveB = getPositiveDocSet(absB);

    // Negative query if absolute value different from original
    if (Objects.equals(a, absA)) {
      if (Objects.equals(b, absB)) return positiveA.intersectionSize(positiveB);
      return positiveA.andNotSize(positiveB);
    }
    if (Objects.equals(b, absB)) return positiveB.andNotSize(positiveA);

    // if both negative, we need to create a temp DocSet since we
    // don't have a counting method that takes three.
    DocSet all = getLiveDocSet();

    // -a -b == *:*.andNot(a).andNotSize(b) == *.*.andNotSize(a.union(b))
    // we use the last form since the intermediate DocSet should normally be smaller.
    return all.andNotSize(positiveA.union(positiveB));
  }

  /**
   * @lucene.internal
   */
  public boolean intersects(DocSet a, DocsEnumState deState) throws IOException {
    return a.intersects(getDocSet(deState));
  }

  /**
   * Called on the initial searcher for each core, immediately before <code>firstSearcherListeners
   * </code> are called for the searcher. This provides the opportunity to perform initialization on
   * the first registered searcher before the searcher begins to see any <code>firstSearcher</code>
   * -triggered events.
   */
  public void bootstrapFirstSearcher() {
    for (SolrCache<?, ?> solrCache : cacheList) {
      solrCache.initialSearcher(this);
    }
  }

  /** Warm this searcher based on an old one (primarily for auto-cache warming). */
  @SuppressWarnings({"unchecked"})
  public void warm(SolrIndexSearcher old) {
    // Make sure this is first! filters can help queryResults execute!
    long warmingStartTime = System.nanoTime();
    // warm the caches in order...
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.add("warming", "true");
    for (int i = 0; i < cacheList.length; i++) {
      if (log.isDebugEnabled()) {
        log.debug("autowarming [{}] from [{}]\n\t{}", this, old, old.cacheList[i]);
      }

      final SolrQueryRequest req = SolrQueryRequest.wrapSearcher(SolrIndexSearcher.this, params);
      final SolrQueryResponse rsp = new SolrQueryResponse();
      SolrRequestInfo.setRequestInfo(new SolrRequestInfo(req, rsp));
      try {
        cacheList[i].warm(this, old.cacheList[i]);
      } finally {
        try {
          req.close();
        } finally {
          SolrRequestInfo.clearRequestInfo();
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("autowarming result for [{}]\n\t{}", this, cacheList[i]);
      }
    }
    warmupTime =
        TimeUnit.MILLISECONDS.convert(System.nanoTime() - warmingStartTime, TimeUnit.NANOSECONDS);
  }

  /** return the named generic cache */
  @SuppressWarnings({"rawtypes"})
  public SolrCache getCache(String cacheName) {
    return cacheMap.get(cacheName);
  }

  /** lookup an entry in a generic cache */
  @SuppressWarnings({"unchecked"})
  public Object cacheLookup(String cacheName, Object key) {
    @SuppressWarnings({"rawtypes"})
    SolrCache cache = cacheMap.get(cacheName);
    return cache == null ? null : cache.get(key);
  }

  /** insert an entry in a generic cache */
  @SuppressWarnings({"unchecked"})
  public Object cacheInsert(String cacheName, Object key, Object val) {
    @SuppressWarnings({"rawtypes"})
    SolrCache cache = cacheMap.get(cacheName);
    return cache == null ? null : cache.put(key, val);
  }

  public Date getOpenTimeStamp() {
    return openTime;
  }

  // public but primarily for test case usage
  public long getOpenNanoTime() {
    return openNanoTime;
  }

  @Override
  public Explanation explain(Query query, int doc) throws IOException {
    return super.explain(QueryUtils.makeQueryable(query), doc);
  }

  /**
   * @lucene.internal gets a cached version of the IndexFingerprint for this searcher
   */
  public IndexFingerprint getIndexFingerprint(long maxVersion) throws IOException {
    final SolrIndexSearcher searcher = this;
    List<Callable<IndexFingerprint>> tasks =
        searcher.getTopReaderContext().leaves().stream()
            .map(
                ctx ->
                    (Callable<IndexFingerprint>)
                        () -> searcher.getCore().getIndexFingerprint(searcher, ctx, maxVersion))
            .collect(Collectors.toList());
    return ExecutorUtil.submitAllAndAwaitAggregatingExceptions(
            core.getCoreContainer().getIndexFingerprintExecutor(), tasks)
        .stream()
        .reduce(new IndexFingerprint(maxVersion), IndexFingerprint::reduce);
  }

  /////////////////////////////////////////////////////////////////////
  // SolrInfoBean stuff: Statistics and Module Info
  /////////////////////////////////////////////////////////////////////

  @Override
  public String getName() {
    return SolrIndexSearcher.class.getName();
  }

  @Override
  public String getDescription() {
    return "index searcher";
  }

  @Override
  public Category getCategory() {
    return Category.CORE;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    parentContext.gauge(() -> name, true, "searcherName", Category.SEARCHER.toString(), scope);
    parentContext.gauge(() -> cachingEnabled, true, "caching", Category.SEARCHER.toString(), scope);
    parentContext.gauge(() -> openTime, true, "openedAt", Category.SEARCHER.toString(), scope);
    parentContext.gauge(() -> warmupTime, true, "warmupTime", Category.SEARCHER.toString(), scope);
    parentContext.gauge(
        () -> registerTime, true, "registeredAt", Category.SEARCHER.toString(), scope);
    parentContext.gauge(
        fullSortCount::sum, true, "fullSortCount", Category.SEARCHER.toString(), scope);
    parentContext.gauge(
        skipSortCount::sum, true, "skipSortCount", Category.SEARCHER.toString(), scope);
    final MetricsMap liveDocsCacheMetrics =
        new MetricsMap(
            (map) -> {
              map.put("inserts", liveDocsInsertsCount.sum());
              map.put("hits", liveDocsHitCount.sum());
              map.put("naiveHits", liveDocsNaiveCacheHitCount.sum());
            });
    parentContext.gauge(
        liveDocsCacheMetrics, true, "liveDocsCache", Category.SEARCHER.toString(), scope);
    // reader stats
    parentContext.gauge(
        rgauge(parentContext.nullNumber(), () -> reader.numDocs()),
        true,
        "numDocs",
        Category.SEARCHER.toString(),
        scope);
    parentContext.gauge(
        rgauge(parentContext.nullNumber(), () -> reader.maxDoc()),
        true,
        "maxDoc",
        Category.SEARCHER.toString(),
        scope);
    parentContext.gauge(
        rgauge(parentContext.nullNumber(), () -> reader.maxDoc() - reader.numDocs()),
        true,
        "deletedDocs",
        Category.SEARCHER.toString(),
        scope);
    parentContext.gauge(
        rgauge(parentContext.nullString(), () -> reader.toString()),
        true,
        "reader",
        Category.SEARCHER.toString(),
        scope);
    parentContext.gauge(
        rgauge(parentContext.nullString(), () -> reader.directory().toString()),
        true,
        "readerDir",
        Category.SEARCHER.toString(),
        scope);
    parentContext.gauge(
        rgauge(parentContext.nullNumber(), () -> reader.getVersion()),
        true,
        "indexVersion",
        Category.SEARCHER.toString(),
        scope);
    // size of the currently opened commit
    parentContext.gauge(
        () -> {
          try {
            Collection<String> files = reader.getIndexCommit().getFileNames();
            long total = 0;
            for (String file : files) {
              total += DirectoryFactory.sizeOf(reader.directory(), file);
            }
            return total;
          } catch (Exception e) {
            return parentContext.nullNumber();
          }
        },
        true,
        "indexCommitSize",
        Category.SEARCHER.toString(),
        scope);
    // statsCache metrics
    parentContext.gauge(
        new MetricsMap(
            map -> {
              statsCache.getCacheMetrics().getSnapshot(map::putNoEx);
              map.put("statsCacheImpl", statsCache.getClass().getSimpleName());
            }),
        true,
        "statsCache",
        Category.CACHE.toString(),
        scope);
  }

  /**
   * wraps a gauge (related to an IndexReader) and swallows any {@link AlreadyClosedException} that
   * might be thrown, returning the specified default in it's place.
   */
  private <T> Gauge<T> rgauge(T closedDefault, Gauge<T> g) {
    return () -> {
      try {
        return g.getValue();
      } catch (AlreadyClosedException ignore) {
        return closedDefault;
      }
    };
  }

  public long getWarmupTime() {
    return warmupTime;
  }
}
