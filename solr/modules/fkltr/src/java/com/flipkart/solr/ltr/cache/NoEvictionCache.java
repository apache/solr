package com.flipkart.solr.ltr.cache;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import com.codahale.metrics.MetricRegistry;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrCacheBase;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;

public class NoEvictionCache<K,V> extends SolrCacheBase implements SolrCache<K,V> {
  private Map<K,V> map;

  private String description="NoEviction Cache";

  private LongAdder hits;
  private LongAdder inserts;
  private LongAdder lookups;
  private final LongAdder ramBytes = new LongAdder();
  private MetricsMap cacheMap;

  private SolrMetricsContext solrMetricsContext;
  private static final long RAM_BYTES_PER_FUTURE =
          RamUsageEstimator.shallowSizeOfInstance(CompletableFuture.class);
  private Set<String> metricNames = ConcurrentHashMap.newKeySet();
  private MetricRegistry registry;

  private int maxSize;

  private long maxRamBytes;

  public NoEvictionCache() { }

  @Override
  public V remove(K key) {
    // ramBytes adjustment happens via #onRemoval
    return null;
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
  }
  @Override
  @SuppressWarnings("unchecked")
  public Object init(Map args, Object persistence, CacheRegenerator regenerator) {
    super.init(args, regenerator);
    String str = (String)args.get("initialSize");
    final int initialSize = str==null ? 1024 : Integer.parseInt(str);
    description = generateDescription(initialSize);

    map = new ConcurrentHashMap<>(initialSize);

    return persistence;
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public V put(K key, V value) {
    return map.put(key, value);
  }

  @Override
  public V get(K key) {
    return map.get(key);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    // NOTE:fkltr warmup not supported.
  }

  @Override
  public void close() {

  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }

//  @Override
  public String getName() {
    return NoEvictionCache.class.getName();
  }

//  @Override
  public String getDescription() {
    return description;
  }

//  @Override
  public Set<String> getMetricNames() {
    return metricNames;
  }

//  @Override
  public void initializeMetrics(SolrMetricManager manager, String registryName, String tag, String scope) {
    this.registry = manager.registry(registryName);
    this.cacheMap = new MetricsMap((detailed, res) -> res.put("size", map.size()));
    SolrMetricsContext solrMetricsContext = new SolrMetricsContext(manager, registryName, tag);
    manager.registerGauge(solrMetricsContext, registryName, cacheMap, tag, true, scope, getCategory().toString());
  }

//  @Override
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  @Override
  public String toString() {
    return name() + (cacheMap != null ? cacheMap.getValue().toString() : "");
  }

  private String generateDescription(int initialSize) {
    String description = "NoEviction Cache(initialSize=" + initialSize;
    if (isAutowarmingOn()) {
      description += ", " + getAutowarmDescription();
    }
    description += ')';
    return description;
  }

  @Override
  public int getMaxSize() {
    return maxSize;
  }

  @Override
  public void setMaxSize(int maxSize) {
    maxSize = Integer.MAX_VALUE;
  }

  @Override
  public int getMaxRamMB() {
    return maxRamBytes != Long.MAX_VALUE ? (int) (maxRamBytes / 1024L / 1024L) : -1;
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    maxRamBytes = Long.MAX_VALUE;
  }

  @Override
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction)
          throws IOException {
    return null;
  }

  private V computeAsync(K key, IOFunction<? super K, ? extends V> mappingFunction)
          throws IOException {
    return null;
  }
}
