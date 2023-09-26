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

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.lucene.util.Accountable;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.util.IOFunction;

/**
 * A shim cache implementation that may be used to provide an "external" view over an associated
 * {@link #backing} cache. Commonly used in conjunction with {@link
 * CacheRegenerator#wrap(SolrCache)}.
 */
public class MetaSolrCache<K, V, M extends Supplier<V> & Accountable> implements SolrCache<K, V> {
  private final SolrCache<K, M> backing;
  private final Function<V, M> mapping;

  /**
   * Creates an external view over the specified backing cache.
   *
   * @param backing the associated backing cache
   * @param mapping a function that wraps "external" values as "internal" values associated with the
   *     backing cache.
   */
  public MetaSolrCache(SolrCache<K, M> backing, Function<V, M> mapping) {
    this.backing = backing;
    this.mapping = mapping;
  }

  /** Returns the associated backing cache. */
  public SolrCache<K, M> unwrap() {
    return backing;
  }

  @Override
  public Object init(Map<String, String> args, Object persistence, CacheRegenerator regenerator) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String name() {
    return backing.name();
  }

  @Override
  public int size() {
    return backing.size();
  }

  @Override
  public V put(K key, V value) {
    M replaced = backing.put(key, mapping.apply(value));
    return replaced == null ? null : replaced.get();
  }

  @Override
  public V get(K key) {
    M ret = backing.get(key);
    return ret == null ? null : ret.get();
  }

  @Override
  public V remove(K key) {
    M removed = backing.remove(key);
    return removed == null ? null : removed.get();
  }

  @Override
  public V computeIfAbsent(K key, IOFunction<? super K, ? extends V> mappingFunction)
      throws IOException {
    return backing.computeIfAbsent(key, (k) -> mapping.apply(mappingFunction.apply(k))).get();
  }

  @Override
  public void clear() {
    backing.clear();
  }

  @Override
  public void setState(State state) {
    backing.setState(state);
  }

  @Override
  public State getState() {
    return backing.getState();
  }

  @Override
  public void warm(SolrIndexSearcher searcher, SolrCache<K, V> old) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMaxSize() {
    return backing.getMaxSize();
  }

  @Override
  public void setMaxSize(int maxSize) {
    backing.setMaxSize(maxSize);
  }

  @Override
  public int getMaxRamMB() {
    return backing.getMaxRamMB();
  }

  @Override
  public void setMaxRamMB(int maxRamMB) {
    backing.setMaxRamMB(maxRamMB);
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    backing.initializeMetrics(parentContext, scope);
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return backing.getSolrMetricsContext();
  }

  @Override
  public String getName() {
    return backing.getName();
  }

  @Override
  public String getDescription() {
    return backing.getDescription();
  }

  @Override
  public Category getCategory() {
    return backing.getCategory();
  }
}
