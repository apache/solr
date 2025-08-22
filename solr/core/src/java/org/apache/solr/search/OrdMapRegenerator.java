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

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.search.SolrCacheBase.autowarmOn;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.index.SlowCompositeReaderWrapper;
import org.apache.solr.search.SolrCache.MetaEntry;
import org.apache.solr.util.IOFunction;

/** Cache regenerator that builds OrdinalMap instances against the new searcher. */
public class OrdMapRegenerator<M extends MetaEntry<String, OrdinalMap, M>>
    extends MetaCacheRegenerator<String, OrdinalMap, M> {

  private static final int DEFAULT_REGEN_KEEPALIVE_MINUTES = 2;
  private static final long DEFAULT_REGEN_KEEPALIVE_NANOS =
      TimeUnit.MINUTES.toNanos(DEFAULT_REGEN_KEEPALIVE_MINUTES);
  private static final OrdMapRegenerator<KeepAliveValue<String, OrdinalMap>> DEFAULT_INSTANCE =
      new OrdMapRegenerator<>(DEFAULT_REGEN_KEEPALIVE_NANOS);

  private final long regenKeepAliveNanos;

  public OrdMapRegenerator() {
    this(DEFAULT_REGEN_KEEPALIVE_NANOS);
    // default ctor in case someone specifies this class via standard `"regen"=[className]` syntax
  }

  private OrdMapRegenerator(long regenKeepAliveNanos) {
    super(true, getWrapFunction());
    this.regenKeepAliveNanos = regenKeepAliveNanos;
  }

  public OrdMapRegenerator(SolrConfig solrConfig, CacheConfig cacheConfig) {
    super(autowarmOn(cacheConfig), getWrapFunction());
    this.regenKeepAliveNanos =
        getRegenKeepAliveNanos(
            "regenKeepAlive", solrConfig, cacheConfig.toMap(Collections.emptyMap()), null);
  }

  @SuppressWarnings({"unchecked", "UnnecessaryLambda"})
  private static <M> BiFunction<SegmentMap, OrdinalMap, M> getWrapFunction() {
    return (segMap, v) -> (M) new KeepAliveValue<>(v, System.nanoTime());
  }

  public static class KeepAliveValue<K, T extends Accountable>
      implements MetaEntry<K, T, KeepAliveValue<K, T>> {
    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(KeepAliveValue.class);
    private final T val;
    private long accessTimestampNanos;

    KeepAliveValue(T val, long accessTimestampNanos) {
      this.val = val;
      this.accessTimestampNanos = accessTimestampNanos;
    }

    @Override
    public T get(SegmentMap segMap, K key, IOFunction<? super K, ? extends T> mappingFunction) {
      accessTimestampNanos = System.nanoTime();
      return val;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + val.ramBytesUsed();
    }

    @Override
    public KeepAliveValue<K, T> metaClone(T val) {
      return new KeepAliveValue<>(val, accessTimestampNanos);
    }
  }

  /**
   * Configures and supplies a default `ordMapCache` {@link CacheConfig} of unlimited maximum size
   * and modest initial size, with no autowarming.
   */
  public static CacheConfig getDefaultCacheConfig() {
    // for back-compat, default to an effectively unlimited-sized cache with no regeneration
    Map<String, String> args = new HashMap<>();
    args.put(NAME, "ordMapCache");
    args.put("size", Integer.toString(Integer.MAX_VALUE)); // effectively unlimited
    args.put("initialSize", "10");
    return new CacheConfig(CaffeineCache.class, args, null);
  }

  /**
   * `regenKeepAlive` is respected if explicitly set, implicitly defaulting to 2x the autocommit
   * interval defined by the specified {@link SolrConfig} (or default of {@value
   * #DEFAULT_REGEN_KEEPALIVE_MINUTES} minutes if no autocommit interval can be determined).
   */
  static long getRegenKeepAliveNanos(
      String argName,
      SolrConfig solrConfig,
      Map<String, Object> cacheConfigArgs,
      String defaultConfig) {
    String keepAliveConfig = (String) cacheConfigArgs.getOrDefault(argName, defaultConfig);
    final long regenKeepAliveNanos;
    if (keepAliveConfig == null || keepAliveConfig.isEmpty()) {
      long osiNanos;
      if (solrConfig == null || (osiNanos = getOpenSearcherIntervalNanos(solrConfig)) == -1) {
        regenKeepAliveNanos = DEFAULT_REGEN_KEEPALIVE_NANOS;
      } else {
        regenKeepAliveNanos = osiNanos << 1;
      }
    } else {
      int lastIdx = keepAliveConfig.length() - 1;
      String sub = keepAliveConfig.substring(0, lastIdx);
      switch (keepAliveConfig.charAt(lastIdx)) {
        case 's':
          regenKeepAliveNanos = TimeUnit.SECONDS.toNanos(Long.parseLong(sub));
          break;
        case 'm':
          regenKeepAliveNanos = TimeUnit.MINUTES.toNanos(Long.parseLong(sub));
          break;
        case 'h':
          regenKeepAliveNanos = TimeUnit.HOURS.toNanos(Long.parseLong(sub));
          break;
        case 'd':
          regenKeepAliveNanos = TimeUnit.DAYS.toNanos(Long.parseLong(sub));
          break;
        case '%':
          int keepAlivePct = Integer.parseInt(sub);
          if (keepAlivePct < 0) {
            throw new IllegalArgumentException(
                "regenKeepAlive % must be positive; found " + keepAlivePct);
          }
          long osiNanos;
          if (solrConfig == null || (osiNanos = getOpenSearcherIntervalNanos(solrConfig)) == -1) {
            throw new IllegalArgumentException(
                "regenKeepAlive % must only be configured in conjunction with autoCommit time");
          } else {
            regenKeepAliveNanos = (osiNanos * keepAlivePct) / 100;
          }
          break;
        default:
          regenKeepAliveNanos = TimeUnit.MILLISECONDS.toNanos(Long.parseLong(keepAliveConfig));
          break;
      }
    }
    return regenKeepAliveNanos;
  }

  private static long getOpenSearcherIntervalNanos(SolrConfig solrConfig) {
    SolrConfig.UpdateHandlerInfo uinfo = solrConfig.getUpdateHandlerInfo();
    if (uinfo == null) {
      return -1;
    } else if (uinfo.autoSoftCommmitMaxTime != -1) {
      if (uinfo.openSearcher && uinfo.autoCommmitMaxTime != -1) {
        return TimeUnit.MILLISECONDS.toNanos(
            Math.min(uinfo.autoCommmitMaxTime, uinfo.autoSoftCommmitMaxTime));
      } else {
        return TimeUnit.MILLISECONDS.toNanos(uinfo.autoSoftCommmitMaxTime);
      }
    } else if (uinfo.openSearcher && uinfo.autoCommmitMaxTime != -1) {
      return TimeUnit.MILLISECONDS.toNanos(uinfo.autoCommmitMaxTime);
    } else {
      return -1;
    }
  }

  @Override
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException {
    DirectoryReader in = newSearcher.getIndexReader();
    IndexReader.CacheHelper cacheHelper = in.getReaderCacheHelper();
    if (cacheHelper == null) {
      return false;
    }

    final List<LeafReaderContext> leaves = in.leaves();
    final int size = leaves.size();

    if (size < 2) {
      // we don't need OrdinalMaps for these trivial cases
      return false;
    }

    @SuppressWarnings("unchecked")
    KeepAliveValue<String, OrdinalMap> ordinalMapValue =
        (KeepAliveValue<String, OrdinalMap>) oldVal;
    final long extantTimestamp = ordinalMapValue.accessTimestampNanos;
    if (System.nanoTime() - extantTimestamp > regenKeepAliveNanos) {
      // it has been long enough since this was last accessed that we don't want to carry it forward
      return true;
    }

    final String field = (String) oldKey;
    final IndexReader.CacheKey readerKey = cacheHelper.getKey();
    final IOFunction<? super String, ? extends KeepAliveValue<String, OrdinalMap>> producer;
    DocIdSetIterator[] dvs = SlowCompositeReaderWrapper.getLeafDocValues(leaves, field);
    if (dvs == null) {
      // All empty for this field, but should still warm others
      return true;
    } else if (dvs instanceof SortedDocValues[]) {
      producer =
          (notUsed) ->
              new KeepAliveValue<>(
                  OrdinalMap.build(readerKey, (SortedDocValues[]) dvs, PackedInts.DEFAULT),
                  extantTimestamp);
    } else if (dvs instanceof SortedSetDocValues[]) {
      producer =
          (notUsed) ->
              new KeepAliveValue<>(
                  OrdinalMap.build(readerKey, (SortedSetDocValues[]) dvs, PackedInts.DEFAULT),
                  extantTimestamp);
    } else {
      throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    SolrCache<String, KeepAliveValue<String, OrdinalMap>> c =
        (SolrCache<String, KeepAliveValue<String, OrdinalMap>>) newCache;
    c.computeIfAbsent(field, producer);
    return true;
  }
}
