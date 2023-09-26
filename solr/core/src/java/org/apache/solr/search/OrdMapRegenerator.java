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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
import org.apache.solr.util.IOFunction;

/** Cache regenerator that builds OrdinalMap instances against the new searcher. */
public class OrdMapRegenerator implements CacheRegenerator {

  private static final long DEFAULT_REGEN_KEEPALIVE_NANOS = TimeUnit.MINUTES.toNanos(2);
  private static final OrdMapRegenerator DEFAULT_INSTANCE =
      new OrdMapRegenerator(DEFAULT_REGEN_KEEPALIVE_NANOS);

  private final long regenKeepAliveNanos;

  public OrdMapRegenerator() {
    this(DEFAULT_REGEN_KEEPALIVE_NANOS);
    // default ctor in case someone specifies this class via standard `"regen"=[className]` syntax
  }

  private OrdMapRegenerator(long regenKeepAliveNanos) {
    this.regenKeepAliveNanos = regenKeepAliveNanos;
  }

  private static class OrdinalMapValue implements Supplier<OrdinalMap>, Accountable {
    private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(OrdinalMapValue.class);
    private final OrdinalMap ordinalMap;
    private long accessTimestampNanos;

    private OrdinalMapValue(OrdinalMap ordinalMap, long accessTimestampNanos) {
      this.ordinalMap = ordinalMap;
      this.accessTimestampNanos = accessTimestampNanos;
    }

    @Override
    public OrdinalMap get() {
      accessTimestampNanos = System.nanoTime();
      return ordinalMap;
    }

    @Override
    public long ramBytesUsed() {
      return BASE_RAM_BYTES_USED + ordinalMap.ramBytesUsed();
    }
  }

  private static OrdinalMapValue wrapValue(OrdinalMap ordinalMap) {
    return new OrdinalMapValue(ordinalMap, 0);
  }

  public static CacheConfig getDefaultCacheConfig(SolrConfig solrConfig) {
    // for back-compat, default to an effectively unlimited-sized cache with no regeneration
    Map<String, String> args = new HashMap<>();
    args.put(NAME, "ordMapCache");
    args.put("size", Integer.toString(Integer.MAX_VALUE)); // effectively unlimited
    args.put("initialSize", "10");
    CacheConfig c = new CacheConfig(CaffeineCache.class, args, null);
    configureRegenerator(solrConfig, c);
    return c;
  }

  public static void configureRegenerator(SolrConfig solrConfig, CacheConfig config) {
    if (config.getRegenerator() != null
        || !new SolrCacheBase.AutoWarmCountRef(
                (String) config.toMap(new HashMap<>()).get("autowarmCount"))
            .isAutoWarmingOn()) {
      // If a regenerator is already explicitly configured, we don't want to replace it.
      // Also, if autowarming is not on, we don't configure a regenerator. This is important
      // because the regenerator is also used to wrap/unwrap the cache for the purpose of
      // tracking metadata, etc. If there's no autowarm, the extra overhead is useless.
      return;
    }
    String keepAliveConfig = (String) config.toMap(Collections.emptyMap()).get("regenKeepAlive");
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
    if (regenKeepAliveNanos == DEFAULT_REGEN_KEEPALIVE_NANOS) {
      config.setRegenerator(DEFAULT_INSTANCE);
      return;
    }
    config.setRegenerator(new OrdMapRegenerator(regenKeepAliveNanos));
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

    OrdinalMapValue ordinalMapValue = (OrdinalMapValue) oldVal;
    final long extantTimestamp = ordinalMapValue.accessTimestampNanos;
    if (System.nanoTime() - extantTimestamp > regenKeepAliveNanos) {
      // it has been long enough since this was last accessed that we don't want to carry it forward
      return true;
    }

    final String field = (String) oldKey;
    final IndexReader.CacheKey readerKey = cacheHelper.getKey();
    final IOFunction<? super String, ? extends Supplier<OrdinalMap>> producer;
    DocIdSetIterator[] dvs = SlowCompositeReaderWrapper.getLeafDocValues(leaves, field);
    if (dvs == null) {
      // All empty for this field, but should still warm others
      return true;
    } else if (dvs instanceof SortedDocValues[]) {
      producer =
          (notUsed) ->
              new OrdinalMapValue(
                  OrdinalMap.build(readerKey, (SortedDocValues[]) dvs, PackedInts.DEFAULT),
                  extantTimestamp);
    } else if (dvs instanceof SortedSetDocValues[]) {
      producer =
          (notUsed) ->
              new OrdinalMapValue(
                  OrdinalMap.build(readerKey, (SortedSetDocValues[]) dvs, PackedInts.DEFAULT),
                  extantTimestamp);
    } else {
      throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    SolrCache<String, Supplier<OrdinalMap>> c = (SolrCache<String, Supplier<OrdinalMap>>) newCache;
    c.computeIfAbsent(field, producer);
    return true;
  }

  @Override
  public <K> SolrCache<K, ?> unwrap(SolrCache<K, ?> external) {
    @SuppressWarnings("unchecked")
    SolrCache<K, ?> ret = ((MetaSolrCache<K, ?, ?>) external).unwrap();
    return ret;
  }

  @Override
  public <K> SolrCache<K, ?> wrap(SolrCache<K, ?> internal) {
    @SuppressWarnings("unchecked")
    SolrCache<K, OrdinalMapValue> backing = (SolrCache<K, OrdinalMapValue>) internal;
    return new MetaSolrCache<>(backing, OrdMapRegenerator::wrapValue);
  }
}
