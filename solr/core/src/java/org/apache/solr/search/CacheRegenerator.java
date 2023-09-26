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

/**
 * Implementations of <code>CacheRegenerator</code> are used in autowarming to populate a new cache
 * based on an old cache. <code>regenerateItem</code> is called for each item that should be
 * inserted into the new cache.
 *
 * <p>Implementations should have a noarg constructor and be thread safe (a single instance will be
 * used for all cache autowarmings).
 */
public interface CacheRegenerator {
  /**
   * Regenerate an old cache item and insert it into <code>newCache</code>
   *
   * @param newSearcher the new searcher who's caches are being autowarmed
   * @param newCache where regenerated cache items should be stored. the target of the autowarming
   * @param oldCache the old cache being used as a source for autowarming
   * @param oldKey the key of the old cache item to regenerate in the new cache
   * @param oldVal the old value of the cache item
   * @return true to continue with autowarming, false to stop
   */
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException;

  /**
   * {@link CacheRegenerator} implementations may override this method to return an "external" view
   * of the input cache. The returned value should not be used for autowarming or lifecycle
   * operations, but should otherwise be the main interface via which application code interacts
   * with the associated cache. This is useful, e.g., if the {@link CacheRegenerator} would like to
   * wrap cache entry values with extra metadata (such as access timestamps or per-entry hit
   * counts), but would not like to expose such metadata to the application generally.
   *
   * <p>Implementations that override this method should also override the corresponding {@link
   * #unwrap(SolrCache)} method.
   */
  default <K> SolrCache<K, ?> wrap(SolrCache<K, ?> internal) {
    return internal;
  }

  /**
   * Input should be the output of the {@link #wrap(SolrCache)} method. The returned value should be
   * the raw, "internal" representation of the cache, used for autowarming and lifecycle operations.
   */
  default <K> SolrCache<K, ?> unwrap(SolrCache<K, ?> external) {
    return external;
  }
}
