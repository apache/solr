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

package org.apache.solr.monitor.cache;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import org.apache.lucene.monitor.QCEVisitor;
import org.apache.lucene.monitor.QueryTermFilterVisitor;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.monitor.MonitorDataValues;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrCacheBase;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.IOFunction;

public class SharedMonitorCache extends SolrCacheBase
    implements SolrCache<String, VersionedQueryCacheEntry>,
        MonitorQueryCache,
        RemovalListener<String, VersionedQueryCacheEntry> {

  private static final long START_HIGH_WATER_MARK = -1;

  private final AtomicReference<CurrentStats> currentStats =
      new AtomicReference<>(CurrentStats.init());

  private Cache<String, VersionedQueryCacheEntry> mqCache;
  BiPredicate<String, BytesRef> termAcceptor = (__, ___) -> true;

  private SolrMetricsContext solrMetricsContext;
  private long generationTimeMs;
  private long priorLookups;
  private long priorHits;
  private long cumulativeGenerationTimeMs;
  private long cumulativeDocVisits;

  long docVisits;
  // only needs to be an approximation
  long versionHighWaterMark = START_HIGH_WATER_MARK;
  private int initialSize;
  private int maxSize;
  private int maxRamMB;

  @Override
  public VersionedQueryCacheEntry computeIfStale(
      MonitorDataValues dataValues, SolrMonitorQueryDecoder decoder) throws IOException {
    return mqCacheMap()
        .compute(
            dataValues.getCacheId(),
            (cacheId, prevEntry) -> compute(cacheId, prevEntry, dataValues, decoder));
  }

  @Override
  public boolean acceptTerm(String field, BytesRef value) {
    return termAcceptor.test(field, value);
  }

  private Map<String, VersionedQueryCacheEntry> mqCacheMap() {
    return mqCache.asMap();
  }

  @Override
  public Object init(Map<String, String> args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = args.get(MAX_SIZE_PARAM);
    maxSize = (str == null) ? 100_000 : Integer.parseInt(str);
    str = args.get(MAX_RAM_MB_PARAM);
    maxRamMB = (str == null) ? -1 : Integer.parseInt(str);
    str = args.get(INITIAL_SIZE_PARAM);
    initialSize = Math.min((str == null) ? 10_000 : Integer.parseInt(str), maxSize);
    str = args.get(MAX_IDLE_TIME_PARAM);
    int maxIdleTimeSec = -1;
    if (str != null) {
      maxIdleTimeSec = Integer.parseInt(str);
    }
    mqCache = buildCache(maxIdleTimeSec);
    return persistence;
  }

  private Cache<String, VersionedQueryCacheEntry> buildCache(int maxIdleTimeSec) {
    Caffeine<String, VersionedQueryCacheEntry> builder =
        Caffeine.newBuilder().initialCapacity(initialSize).removalListener(this).recordStats();
    if (maxIdleTimeSec > 0) {
      builder.expireAfterAccess(Duration.ofSeconds(maxIdleTimeSec));
    }

    if (maxRamMB >= 0) {
      builder.maximumWeight(maxRamMB * 1024L * 1024L);
      builder.weigher(
          (k, v) ->
              (int)
                  (RamUsageEstimator.sizeOf(k)
                      + RamUsageEstimator.sizeOf(v.entry.getMatchQuery())));
    } else {
      builder.maximumSize(maxSize);
    }
    return builder.build();
  }

  @Override
  public int size() {
    return mqCacheMap().size();
  }

  @Override
  public VersionedQueryCacheEntry put(String key, VersionedQueryCacheEntry value) {
    return mqCacheMap().put(key, value);
  }

  @Override
  public VersionedQueryCacheEntry get(String key) {
    return mqCacheMap().get(key);
  }

  @Override
  public VersionedQueryCacheEntry remove(String key) {
    return mqCacheMap().remove(key);
  }

  @Override
  public VersionedQueryCacheEntry computeIfAbsent(
      String key, IOFunction<? super String, ? extends VersionedQueryCacheEntry> mappingFunction)
      throws IOException {
    return mqCacheMap()
        .computeIfAbsent(
            key,
            _key -> {
              try {
                return mappingFunction.apply(_key);
              } catch (IOException e) {
                throw new SolrException(ErrorCode.INVALID_STATE, "Could not update cache", e);
              }
            });
  }

  @Override
  public void clear() {
    mqCacheMap().clear();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<String, VersionedQueryCacheEntry> old) {
    try {
      SharedMonitorCache oldSharedMonitorCache =
          old instanceof SharedMonitorCache ? (SharedMonitorCache) old : null;
      if (oldSharedMonitorCache != null) {
        mqCache = oldSharedMonitorCache.mqCache;
        versionHighWaterMark = oldSharedMonitorCache.versionHighWaterMark;
      }
      if (regenerator != null) {
        long nanoStart = System.nanoTime();
        regenerator.regenerateItem(searcher, this, old, null, null);
        generationTimeMs = NANOSECONDS.toMillis(System.nanoTime() - nanoStart);
      }
      termAcceptor = new QueryTermFilterVisitor(searcher.getIndexReader());
      if (oldSharedMonitorCache != null) {
        var oldStats = oldSharedMonitorCache.currentStats.get();
        priorHits = oldSharedMonitorCache.priorHits + oldStats.hits;
        priorLookups = oldSharedMonitorCache.priorLookups + oldStats.lookups;
        cumulativeGenerationTimeMs =
            generationTimeMs + oldSharedMonitorCache.cumulativeGenerationTimeMs;
        cumulativeDocVisits = oldSharedMonitorCache.cumulativeDocVisits + docVisits;
      }
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.INVALID_STATE, "could not boostrap cache", e);
    }
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  @Override
  public void setMaxSize(int maxSize) {
    throw new UnsupportedOperationException("maxSize is unsupported");
  }

  @Override
  public int getMaxRamMB() {
    return maxRamMB;
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    throw new UnsupportedOperationException("cannot set max RAM Mb");
  }

  @Override
  public String getName() {
    return name();
  }

  @Override
  public String getDescription() {
    return "Solr monitor query cache";
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    var cacheMap =
        new MetricsMap(
            map -> {
              var stats = currentStats.get();
              map.put(LOOKUPS_PARAM, stats.lookups);
              map.put(HITS_PARAM, stats.hits);
              map.put(HIT_RATIO_PARAM, rate(stats.hits, stats.lookups));
              map.put(SIZE_PARAM, size());
              map.put(MAX_RAM_MB_PARAM, getMaxRamMB());
              map.put(MAX_SIZE_PARAM, getMaxSize());
              map.put("generation_time", generationTimeMs);
              map.put("doc_visits", docVisits);
              long cumulativeLookups = priorLookups + stats.lookups;
              long cumulativeHits = priorHits + stats.hits;
              map.put("cumulative_lookups", cumulativeLookups);
              map.put("cumulative_hits", cumulativeHits);
              map.put("cumulative_generation_time", cumulativeGenerationTimeMs);
              map.put("cumulative_hitratio", rate(cumulativeHits, cumulativeLookups));
              map.put(
                  "cumulative_generation_overhead",
                  rate(cumulativeGenerationTimeMs, cumulativeHits));
              map.put("version_high_water_mark", String.valueOf(versionHighWaterMark));
              map.put("cumulative_doc_visits", cumulativeDocVisits);
            });
    solrMetricsContext.gauge(cacheMap, true, scope, getCategory().toString());
  }

  public int getInitialSize() {
    return initialSize;
  }

  private static double rate(long num, long den) {
    return den == 0 ? 1.0 : (double) num / den;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  private VersionedQueryCacheEntry compute(
      String cacheId,
      VersionedQueryCacheEntry prevEntry,
      MonitorDataValues dataValues,
      SolrMonitorQueryDecoder decoder) {
    try {
      var version = dataValues.getVersion();
      if (prevEntry == null || version > prevEntry.version) {
        var monitorQuery = decoder.decode(dataValues);
        QCEVisitor component = decoder.getComponent(monitorQuery, cacheId);
        currentStats.updateAndGet(CurrentStats::miss);
        return new VersionedQueryCacheEntry(component, version);
      }
      if (version == prevEntry.version) {
        currentStats.updateAndGet(CurrentStats::hit);
      } else {
        currentStats.updateAndGet(CurrentStats::miss);
      }
      return prevEntry;
    } catch (Exception e) {
      throw new SolrException(ErrorCode.INVALID_STATE, "Failed to update MonitorQueryCache", e);
    }
  }

  @Override
  public void onRemoval(String key, VersionedQueryCacheEntry value, RemovalCause cause) {
    /* TODO revisit */
  }

  private static final class CurrentStats {

    private final long hits;
    private final long lookups;

    private CurrentStats(long hits, long lookups) {
      this.hits = hits;
      this.lookups = lookups;
    }

    private static CurrentStats init() {
      return new CurrentStats(0, 0);
    }

    private static CurrentStats hit(CurrentStats old) {
      return new CurrentStats(old.hits + 1, old.lookups + 1);
    }

    private static CurrentStats miss(CurrentStats old) {
      return new CurrentStats(old.hits, old.lookups + 1);
    }
  }
}
