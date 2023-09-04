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

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.lucene.util.Accountable;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DISCLAIMER: This class is initially included in the test codebase as a proof-of-concept for
 * demonstrating/validating node-level cache configuration. Although the implementation is
 * relatively naive, it should be usable as-is as a plugin, or as a template for developing more
 * robust implementations.
 *
 * <p>A "thin" cache that does not hold strong direct references to the values that it stores and
 * supplies. Strong references to values are held by a backing {@link NodeLevelCache}. Local
 * references to keys (and weak references to values) are held by this ThinCache only as an
 * approximation of the contents of the cache.
 *
 * <p>There are no strong guarantees regarding the consistency of local bookkeeping in the ThinCache
 * (wrt the "source of truth" backing cache). Such guarantees are not necessary, because the local
 * bookkeeping only exists to support functionality (such as auto-warming and metrics reporting)
 * where strict correctness is not essential.
 *
 * <p>There <i>is</i> however a guarantee that any inconsistency will only be in a safe direction --
 * i.e., that although there may be entries in the backing cache that are not represented locally in
 * the ThinCache bookkeeping, the reverse is not true (to protect against memory leak resulting from
 * the accumulation of stale local references with no corresponding entry in the backing cache).
 *
 * <p>NOTE REGARDING AUTOWARMING: because both the warming cache and the cache associated with the
 * active searcher are backed by the same underlying node-level cache, some extra consideration must
 * be taken in configuring autowarming. Crosstalk between thin caches is an unavoidable consequence
 * of the node-level cache approach. Indeed, in the sense that "dynamic resource allocation" is a
 * type of crosstalk, crosstalk could be considered to be the distinguishing <i>feature</i> of the
 * node-level cache approach! But in order to prevent competition between active searchers and
 * corresponding warming searchers, it is advisable to autowarm by percentage -- generally &lt;= 50%
 * -- or set a relatively low autowarm count (wrt the anticipated overall size of the backing
 * cache).
 */
public class ThinCache<S, K, V> extends SolrCacheBase
    implements SolrCache<K, V>, Accountable, RemovalListener<K, V> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final class ScopedKey<S, K> {
    public final S scope;
    public final K key;

    private ScopedKey(S scope, K key) {
      this.scope = scope;
      this.key = key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ScopedKey<?, ?> scopedKey = (ScopedKey<?, ?>) o;
      return scope.equals(scopedKey.scope) && key.equals(scopedKey.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scope, key);
    }
  }

  private interface RemovalListenerRegistry<S, K, V> extends RemovalListener<ScopedKey<S, K>, V> {

    void register(S scope, RemovalListener<K, V> listener);

    void unregister(S scope);
  }

  private static class RemovalListenerRegistryImpl<S, K, V>
      implements RemovalListenerRegistry<S, K, V> {

    private final Map<S, RemovalListener<K, V>> listeners = new ConcurrentHashMap<>();

    @Override
    public void register(S scope, RemovalListener<K, V> listener) {
      if (listeners.put(scope, listener) != null) {
        throw new IllegalStateException("removal listener already added for scope " + scope);
      }
    }

    @Override
    public void unregister(S scope) {
      if (listeners.remove(scope) == null) {
        log.warn("no removal listener found for scope {}", scope);
      }
    }

    @Override
    public void onRemoval(ScopedKey<S, K> key, V value, RemovalCause cause) {
      RemovalListener<K, V> listener;
      if (key != null && (listener = listeners.get(key.scope)) != null) {
        listener.onRemoval(key.key, value, cause);
      }
    }
  }

  public static final class NodeLevelCache<S, K, V> extends CaffeineCache<ScopedKey<S, K>, V>
      implements RemovalListenerRegistry<S, K, V> {

    private final RemovalListenerRegistry<S, K, V> removalListenerRegistry =
        new RemovalListenerRegistryImpl<>();

    @Override
    public void onRemoval(ScopedKey<S, K> key, V value, RemovalCause cause) {
      super.onRemoval(key, value, cause);
      removalListenerRegistry.onRemoval(key, value, cause);
    }

    @Override
    public void register(S scope, RemovalListener<K, V> listener) {
      removalListenerRegistry.register(scope, listener);
    }

    @Override
    public void unregister(S scope) {
      removalListenerRegistry.unregister(scope);
    }

    @Override
    public String getName() {
      return NodeLevelCache.class.getName();
    }

    @Override
    public String getDescription() {
      return String.format(Locale.ROOT, "Node Level Cache(impl=%s)", super.getDescription());
    }
  }

  private String description = "Thin Cache";

  private NodeLevelCache<S, K, V> backing;

  private S scope;

  private String parentCacheName;

  private final ConcurrentMap<K, ValEntry<V>> local = new ConcurrentHashMap<>();

  private static final class ValEntry<V> {
    private final LongAdder ct = new LongAdder();
    private final WeakReference<V> ref;

    private ValEntry(V val) {
      this.ref = new WeakReference<>(val);
    }
  }

  private static final class HitCountEntry<K, V> {
    private final long ct;
    private final K key;
    private final V val;

    private HitCountEntry(long ct, K key, V val) {
      this.ct = ct;
      this.key = key;
      this.val = val;
    }
  }

  @VisibleForTesting
  void setBacking(S scope, NodeLevelCache<S, K, V> backing) {
    this.scope = scope;
    this.backing = backing;
  }

  private void initForSearcher(SolrIndexSearcher searcher) {
    if (searcher != null) {
      // `searcher` may be null for tests, in which case we assume that `this.backing` will
      // have been set manually via `setBacking()`. In normal use, we expect `searcher != null`.
      @SuppressWarnings("unchecked")
      S scope = (S) searcher.getTopReaderContext().reader().getReaderCacheHelper().getKey();
      this.scope = scope;
      @SuppressWarnings("unchecked")
      NodeLevelCache<S, K, V> backing =
          (NodeLevelCache<S, K, V>) searcher.getCore().getCoreContainer().getCache(parentCacheName);
      this.backing = backing;
    }
    description = generateDescription();
    backing.register(scope, this);
  }

  @Override
  public void initialSearcher(SolrIndexSearcher initialSearcher) {
    initForSearcher(initialSearcher);
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    initForSearcher(searcher);
    @SuppressWarnings("unchecked")
    ThinCache<S, K, V> other = (ThinCache<S, K, V>) old;
    long warmingStartTimeNanos = System.nanoTime();
    List<HitCountEntry<K, V>> orderedEntries = Collections.emptyList();
    // warm entries
    if (isAutowarmingOn()) {
      orderedEntries = new ArrayList<>(other.local.size() << 1); // oversize
      for (Entry<K, ValEntry<V>> e : other.local.entrySet()) {
        ValEntry<V> valEntry = e.getValue();
        V val = valEntry.ref.get();
        if (val != null) {
          orderedEntries.add(new HitCountEntry<>(valEntry.ct.sum(), e.getKey(), val));
        }
      }
      orderedEntries.sort((a, b) -> Long.compare(b.ct, a.ct));
    }

    int size = autowarm.getWarmCount(orderedEntries.size());
    int ct = 0;
    for (HitCountEntry<K, V> entry : orderedEntries) {
      try {
        boolean continueRegen =
            regenerator.regenerateItem(searcher, this, old, entry.key, entry.val);
        if (!continueRegen || ++ct >= size) {
          break;
        }
      } catch (Exception e) {
        log.error("Error during auto-warming of key: {}", entry.key, e);
      }
    }

    backing.adjustMetrics(hits.sumThenReset(), inserts.sumThenReset(), lookups.sumThenReset());
    evictions.reset();
    priorHits = other.hits.sum() + other.priorHits;
    priorInserts = other.inserts.sum() + other.priorInserts;
    priorLookups = other.lookups.sum() + other.priorLookups;
    priorEvictions = other.evictions.sum() + other.priorEvictions;
    warmupTimeMillis =
        TimeUnit.MILLISECONDS.convert(
            System.nanoTime() - warmingStartTimeNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public Object init(Map<String, String> args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    parentCacheName = args.get("parentCacheName");
    return persistence;
  }

  private MetricsMap cacheMap;
  private SolrMetricsContext solrMetricsContext;

  private final LongAdder hits = new LongAdder();
  private final LongAdder inserts = new LongAdder();
  private final LongAdder lookups = new LongAdder();
  private final LongAdder evictions = new LongAdder();
  private long warmupTimeMillis;

  private long priorHits;
  private long priorInserts;
  private long priorLookups;
  private long priorEvictions;

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    solrMetricsContext = parentContext.getChildContext(this);
    cacheMap =
        new MetricsMap(
            map -> {
              long hitCount = hits.sum();
              long insertCount = inserts.sum();
              long lookupCount = lookups.sum();
              long evictionCount = evictions.sum();

              map.put(LOOKUPS_PARAM, lookupCount);
              map.put(HITS_PARAM, hitCount);
              map.put(HIT_RATIO_PARAM, hitRate(hitCount, lookupCount));
              map.put(INSERTS_PARAM, insertCount);
              map.put(EVICTIONS_PARAM, evictionCount);
              map.put(SIZE_PARAM, local.size());
              map.put("warmupTime", warmupTimeMillis);
              map.put(RAM_BYTES_USED_PARAM, ramBytesUsed());
              map.put(MAX_RAM_MB_PARAM, getMaxRamMB());

              long cumLookups = priorLookups + lookupCount;
              long cumHits = priorHits + hitCount;
              map.put("cumulative_lookups", cumLookups);
              map.put("cumulative_hits", cumHits);
              map.put("cumulative_hitratio", hitRate(cumHits, cumLookups));
              map.put("cumulative_inserts", priorInserts + insertCount);
              map.put("cumulative_evictions", priorEvictions + evictionCount);
            });
    solrMetricsContext.gauge(cacheMap, true, scope, getCategory().toString());
  }

  @VisibleForTesting
  MetricsMap getMetricsMap() {
    return cacheMap;
  }

  // TODO: refactor this common method out of here and `CaffeineCache`
  private static double hitRate(long hitCount, long lookupCount) {
    return lookupCount == 0 ? 1.0 : (double) hitCount / lookupCount;
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

  @Override
  public void close() throws IOException {
    backing.unregister(scope);
    SolrCache.super.close();
  }

  @Override
  public void onRemoval(K key, V value, RemovalCause cause) {
    if (cause.wasEvicted()) {
      evictions.increment();
    }
    local.remove(key);
  }

  @Override
  public String toString() {
    return name() + (cacheMap != null ? cacheMap.getValue().toString() : "");
  }

  @Override
  public int size() {
    return local.size();
  }

  @Override
  public V put(K key, V value) {
    inserts.increment();
    ValEntry<V> valEntry = new ValEntry<>(value);
    valEntry.ct.increment();
    local.put(key, valEntry);
    return backing.put(new ScopedKey<>(scope, key), value);
  }

  @Override
  public V get(K key) {
    lookups.increment();
    V ret = backing.get(new ScopedKey<>(scope, key));
    if (ret != null) {
      hits.increment();
      ValEntry<V> valEntry = local.get(key);
      if (valEntry != null) {
        valEntry.ct.increment();
      }
    }
    return ret;
  }

  @Override
  public V remove(K key) {
    // NOTE: rely on `onRemoval()` to remove entry from `local`
    return backing.remove(new ScopedKey<>(scope, key));
  }

  @Override
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction)
      throws IOException {
    lookups.increment();
    boolean[] hit = new boolean[] {true};
    V ret =
        backing.computeIfAbsent(
            new ScopedKey<>(scope, key),
            (k) -> {
              hit[0] = false;
              inserts.increment();
              V innerRet = mappingFunction.apply(k.key);
              ValEntry<V> valEntry = new ValEntry<>(innerRet);
              valEntry.ct.increment();
              local.put(key, valEntry);
              return innerRet;
            });
    if (hit[0]) {
      hits.increment();
    }
    return ret;
  }

  @Override
  public void clear() {
    for (K key : local.keySet()) {
      backing.remove(new ScopedKey<>(scope, key));
    }
    // NOTE: rely on `onRemoval()` to remove entries from `local`
  }

  @Override
  public int getMaxSize() {
    return backing == null ? 0 : backing.getMaxSize();
  }

  @Override
  public void setMaxSize(int maxSize) {
    throw new UnsupportedOperationException(
        "limits cannot be configured directly on " + getClass());
  }

  @Override
  public int getMaxRamMB() {
    return backing == null ? 0 : backing.getMaxRamMB();
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    throw new UnsupportedOperationException(
        "limits cannot be configured directly on " + getClass());
  }

  @Override
  public long ramBytesUsed() {
    // TODO: this has yet to be implemented.
    //  the actual implementation should be straightforward, but there are questions about what's
    //  in and out of scope for calculating ramBytesUsed.
    return 0;
  }

  @Override
  public String getName() {
    return ThinCache.class.getName();
  }

  /** Returns the description of this cache. */
  private String generateDescription() {
    return String.format(
        Locale.ROOT,
        "Thin Cache(backing=%s%s)",
        backing.getDescription(),
        isAutowarmingOn() ? (", " + getAutowarmDescription()) : "");
  }

  @Override
  public String getDescription() {
    return description;
  }
}
