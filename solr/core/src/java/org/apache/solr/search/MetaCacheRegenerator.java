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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.common.MapWriter;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.SolrCache.MetaEntry;
import org.apache.solr.search.SolrCache.SidecarMetricProducer;

/**
 * Base class for {@link CacheRegenerator} implementations that may be used to internally wrap cache
 * values to track extra metadata like access timestamps or hit counts. Wrapping/unwrapping of the
 * associated cache is implemented in {@link CacheRegenerator#wrap(SolrCache)} and {@link
 * CacheRegenerator#unwrap(SolrCache)}, and invoked via {@link SolrCache#toExternal()} and {@link
 * SolrCache#toInternal()}.
 *
 * <p>Two common use cases for this approach are:
 *
 * <ol>
 *   <li>Using metadata to inform autowarming decisions (such as in {@link OrdMapRegenerator})
 *   <li>Using metadata to report nuanced cache metrics (such as in {@link FilterHistogram} or
 *       {@link FilterDump})
 * </ol>
 *
 * <p>The second of these use cases might superficially seem like a poor fit for implementing via a
 * {@link CacheRegenerator} (as opposed to via a special {@link SolrCache} implementation. But in
 * practice this works out quite cleanly: {@link SolrCache} implementations (and their consumers)
 * remain unaware of the under-the-hood changes to support metadata, and {@link CacheRegenerator} is
 * in any case the only component that actually needs to be directly aware of the type of values in
 * the internal cache.
 *
 * @param <K> Key type
 * @param <V> Raw value type. This is the type that is exposed in most interactions with the cache.
 * @param <M> {@link MetaEntry} value type. This is the type of the underlying "internal" cache that
 *     is used for autowarming and lifecycle operations.
 */
public class MetaCacheRegenerator<K, V, M extends MetaEntry<V, M>>
    implements CacheRegenerator, SidecarMetricProducer<K, M> {

  private static final int DEFAULT_BUCKETS = 10;
  private static final SearcherIOBiFunction<Query, DocSet> FILTER_REGEN_FUNC =
      (s, q) -> s.getDocSetNC(q, null);

  private static <K, V> SearcherIOBiFunction<K, V> nullWarningRegenFunc() {
    return (s, q) -> {
      throw new UnsupportedOperationException(
          MetaCacheRegenerator.class.getSimpleName()
              + " must either supply regenFunc or override `regenerateItem()`");
    };
  }

  public static final class FilterHistogram
      extends MetaCacheRegenerator<Query, DocSet, HitsMetaEntry<DocSet>> {
    public FilterHistogram() {
      super(
          FILTER_REGEN_FUNC,
          HitsMetaEntry.WRAP_FUNC,
          ".histogram",
          (cache) ->
              map -> {
                Histogram histogram = new Histogram(new UniformReservoir());
                cache.forEach((k, v) -> histogram.update(v.priorHits + v.hits.sum()));
                Snapshot snapshot = histogram.getSnapshot();
                for (int i = 0; i < DEFAULT_BUCKETS; i++) {
                  double val = snapshot.getValue((double) i / DEFAULT_BUCKETS);
                  map.put(Integer.toString(i), Double.toString(val));
                }
              });
    }
  }

  public static final class FilterDump
      extends MetaCacheRegenerator<Query, DocSet, HitsMetaEntry<DocSet>> {
    public FilterDump() {
      super(
          FILTER_REGEN_FUNC,
          HitsMetaEntry.WRAP_FUNC,
          ".dump",
          (cache) ->
              map -> {
                List<DumpEntry<Query>> lst = new ArrayList<>(ArrayUtil.oversize(cache.size(), 1));
                cache.forEach((k, v) -> lst.add(new DumpEntry<>(k, v.priorHits + v.hits.sum())));
                lst.sort((a, b) -> Long.compare(b.hits, a.hits)); // most hits first
                for (DumpEntry<Query> e : lst) {
                  map.put(Long.toString(e.hits), e.key.toString());
                }
              });
    }
  }

  private static final class DumpEntry<K> {
    private final K key;
    private final long hits;

    private DumpEntry(K key, long hits) {
      this.key = key;
      this.hits = hits;
    }
  }

  public interface SearcherIOBiFunction<K, V> {
    V apply(SolrIndexSearcher s, K k) throws IOException;
  }

  private final SearcherIOBiFunction<K, V> regenFunction;
  private final Function<V, M> wrapFunction;
  private final String metaType;
  private final Function<SolrCache<K, M>, MapWriter> mapWriterFunction;
  private SolrMetricsContext metricsContext;

  /**
   * This ctor should be used by subclasses that will make regen decisions based on metadata. Such
   * implementations must override {@link MetaCacheRegenerator#regenerateItem(SolrIndexSearcher,
   * SolrCache, SolrCache, Object, Object)}.
   *
   * @param wrapFunction function to wrap raw values in a {@link MetaEntry} wrapper.
   */
  public MetaCacheRegenerator(Function<V, M> wrapFunction) {
    this(null, wrapFunction, null, null);
  }

  /**
   * This ctor should be used by subclasses that are strictly interested in cache entry metadata for
   * the purpose of reporting nuanced cache metrics.
   *
   * @param regenFunction Function to regenerate raw value for the provided searcher and key.
   * @param wrapFunction function to wrap raw values in a {@link MetaEntry} wrapper.
   * @param metaType suffix added to the associated cache's metric name to define extra
   *     meta-metrics.
   * @param mapWriterFunction defines the mapWriter for supplying meta-metrics
   */
  public MetaCacheRegenerator(
      SearcherIOBiFunction<K, V> regenFunction,
      Function<V, M> wrapFunction,
      String metaType,
      Function<SolrCache<K, M>, MapWriter> mapWriterFunction) {
    this.regenFunction = regenFunction == null ? nullWarningRegenFunc() : regenFunction;
    this.wrapFunction = wrapFunction;
    if (metaType == null ^ mapWriterFunction == null) {
      throw new IllegalArgumentException(
          "metaType and mapWriterFunction must both or neither be specified");
    }
    this.metaType = metaType;
    this.mapWriterFunction = mapWriterFunction;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K1, M1> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K1, M1> newCache,
      SolrCache<K1, M1> oldCache,
      K1 oldKey,
      M1 oldVal)
      throws IOException {
    M oldMetaVal = (M) oldVal;
    SolrCache<K, M> c = (SolrCache<K, M>) newCache;
    c.computeIfAbsent(
        (K) oldKey,
        (k) -> {
          V val = regenFunction.apply(newSearcher, k);
          return oldMetaVal.metaClone(val);
        });
    return true;
  }

  @Override
  public <K1> SolrCache<K1, ?> wrap(SolrCache<K1, ?> internal) {
    @SuppressWarnings("unchecked")
    CaffeineCache<K, M> backing = (CaffeineCache<K, M>) internal;
    @SuppressWarnings("unchecked")
    SolrCache<K1, ?> ret = (SolrCache<K1, ?>) new MetaSolrCache<>(backing, wrapFunction);
    return ret;
  }

  @Override
  public <K1> SolrCache<K1, ?> unwrap(SolrCache<K1, ?> external) {
    @SuppressWarnings("unchecked")
    SolrCache<K1, ?> ret = (SolrCache<K1, ?>) external.toInternal();
    return ret;
  }

  @Override
  public void initializeMetrics(
      SolrMetricsContext parentContext, String scope, SolrCache<K, M> cache) {
    this.metricsContext = parentContext;
    if (metaType != null) {
      MetricsMap metricsMap = new MetricsMap(mapWriterFunction.apply(cache));
      metricsContext.gauge(
          metricsMap, true, scope.concat(metaType), cache.getCategory().toString());
    }
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return metricsContext;
  }

  private static class HitsMetaEntry<V> implements MetaEntry<V, HitsMetaEntry<V>> {
    @SuppressWarnings("UnnecessaryLambda")
    private static final Function<DocSet, HitsMetaEntry<DocSet>> WRAP_FUNC =
        (v) -> new HitsMetaEntry<>(v, 0);

    private static final long BASE_RAM_BYTES =
        RamUsageEstimator.shallowSizeOfInstance(MetaEntry.class);

    private final V val;
    private final long priorHits;
    private final LongAdder hits = new LongAdder();

    private HitsMetaEntry(V val, long priorHits) {
      this.val = val;
      this.priorHits = priorHits;
    }

    @Override
    public V get() {
      hits.increment();
      return val;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES + RamUsageEstimator.sizeOfObject(val);
    }

    @Override
    public HitsMetaEntry<V> metaClone(V val) {
      return new HitsMetaEntry<>(val, priorHits + hits.sum());
    }
  }
}
