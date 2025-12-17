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
package org.apache.solr.blockcache;

import io.opentelemetry.api.common.Attributes;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.search.SolrCacheBase;

/**
 * A {@link SolrInfoBean} that provides metrics on block cache operations.
 *
 * @lucene.experimental
 */
public class Metrics extends SolrCacheBase implements SolrInfoBean {

  public AtomicLong blockCacheSize = new AtomicLong(0);
  public AtomicLong blockCacheHit = new AtomicLong(0);
  public AtomicLong blockCacheMiss = new AtomicLong(0);
  public AtomicLong blockCacheEviction = new AtomicLong(0);
  public AtomicLong blockCacheStoreFail = new AtomicLong(0);

  // since the last call
  private AtomicLong blockCacheHit_last = new AtomicLong(0);
  private AtomicLong blockCacheMiss_last = new AtomicLong(0);
  private AtomicLong blockCacheEviction_last = new AtomicLong(0);
  public AtomicLong blockCacheStoreFail_last = new AtomicLong(0);

  // These are used by the BufferStore (just a generic cache of byte[]).
  // TODO: If this (the Store) is a good idea, we should make it more general and use it across more
  // places in Solr.
  public AtomicLong shardBuffercacheAllocate = new AtomicLong(0);
  public AtomicLong shardBuffercacheLost = new AtomicLong(0);

  private SolrMetricsContext solrMetricsContext;
  private long previous = System.nanoTime();

  private AutoCloseable toClose;

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, Attributes attributes) {
    solrMetricsContext = parentContext.getChildContext(this);
    var baseAttributes =
        attributes.toBuilder().put(CATEGORY_ATTR, getCategory().toString()).build();
    var blockcacheStats =
        solrMetricsContext.longGaugeMeasurement("solr_block_cache_stats", "Block cache stats");
    var hitRatio =
        solrMetricsContext.doubleGaugeMeasurement(
            "solr_block_cache_hit_ratio", "Block cache hit ratio");
    var perSecStats =
        solrMetricsContext.doubleGaugeMeasurement(
            "solr_block_cache_stats_per_second", "Block cache per second stats");
    var bufferCacheStats =
        solrMetricsContext.doubleGaugeMeasurement(
            "solr_buffer_cache_stats", "Buffer cache per second stats");

    this.toClose =
        solrMetricsContext.batchCallback(
            () -> {
              long now = System.nanoTime();
              long delta = Math.max(now - previous, 1);
              double seconds = delta / 1000000000.0;

              long hits_total = blockCacheHit.get();
              long hits_delta = hits_total - blockCacheHit_last.get();
              blockCacheHit_last.set(hits_total);

              long miss_total = blockCacheMiss.get();
              long miss_delta = miss_total - blockCacheMiss_last.get();
              blockCacheMiss_last.set(miss_total);

              long evict_total = blockCacheEviction.get();
              long evict_delta = evict_total - blockCacheEviction_last.get();
              blockCacheEviction_last.set(evict_total);

              long storeFail_total = blockCacheStoreFail.get();
              long storeFail_delta = storeFail_total - blockCacheStoreFail_last.get();
              blockCacheStoreFail_last.set(storeFail_total);

              long lookups_delta = hits_delta + miss_delta;
              long lookups_total = hits_total + miss_total;

              blockcacheStats.record(
                  blockCacheSize.get(), baseAttributes.toBuilder().put(TYPE_ATTR, "size").build());
              blockcacheStats.record(
                  lookups_total, baseAttributes.toBuilder().put(TYPE_ATTR, "lookups").build());
              blockcacheStats.record(
                  hits_total, baseAttributes.toBuilder().put(TYPE_ATTR, "hits").build());
              blockcacheStats.record(
                  hits_total, baseAttributes.toBuilder().put(TYPE_ATTR, "evictions").build());
              blockcacheStats.record(
                  storeFail_total,
                  baseAttributes.toBuilder().put(TYPE_ATTR, "store_fails").build());
              perSecStats.record(
                  getPerSecond(lookups_delta, seconds),
                  baseAttributes.toBuilder()
                      .put(TYPE_ATTR, "lookups")
                      .build()); // lookups per second since the last call
              perSecStats.record(
                  getPerSecond(hits_delta, seconds),
                  baseAttributes.toBuilder()
                      .put(TYPE_ATTR, "hits")
                      .build()); // hits per second since the last call
              perSecStats.record(
                  getPerSecond(evict_delta, seconds),
                  baseAttributes.toBuilder()
                      .put(TYPE_ATTR, "evictions")
                      .build()); // evictions per second since the last call
              perSecStats.record(
                  getPerSecond(storeFail_delta, seconds),
                  baseAttributes.toBuilder()
                      .put(TYPE_ATTR, "store_fails")
                      .build()); // evictions per second since the last call
              hitRatio.record(
                  calcHitRatio(lookups_delta, hits_delta),
                  baseAttributes); // hit ratio since the last call
              bufferCacheStats.record(
                  getPerSecond(shardBuffercacheAllocate.getAndSet(0), seconds),
                  baseAttributes.toBuilder().put(TYPE_ATTR, "allocations").build());
              bufferCacheStats.record(
                  getPerSecond(shardBuffercacheLost.getAndSet(0), seconds),
                  baseAttributes.toBuilder().put(TYPE_ATTR, "lost").build());
              previous = now;
            },
            blockcacheStats,
            perSecStats,
            hitRatio,
            bufferCacheStats);
  }

  private float getPerSecond(long value, double seconds) {
    return (float) (value / seconds);
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(toClose);
  }

  // SolrInfoBean methods

  @Override
  public String getName() {
    return "blockCache";
  }

  @Override
  public String getDescription() {
    return "Provides metrics for the BlockCache.";
  }

  @Override
  public SolrMetricsContext getSolrMetricsContext() {
    return solrMetricsContext;
  }
}
