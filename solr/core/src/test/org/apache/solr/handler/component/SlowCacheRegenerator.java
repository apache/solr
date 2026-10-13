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
package org.apache.solr.handler.component;

import java.io.IOException;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * Test-only {@link CacheRegenerator} that stands in for expensive per-entry regeneration (e.g.
 * re-executing a costly filter query against the new searcher) by sleeping a fixed amount of time
 * before copying each old entry forward. Unlike {@link SlowIOSimulatingDictionaryFactory} and
 * {@link SlowIOSimulatingSpellChecker}, this can't take its delay via a constructor/config argument
 * - {@link CacheRegenerator} requires a no-arg constructor and is instantiated by the cache itself
 * from solrconfig.xml - so the delay is read from a system property instead, set by the test before
 * triggering the commit that does the autowarming.
 */
public class SlowCacheRegenerator implements CacheRegenerator {
  public static final String SLEEP_MS_PROPERTY = "solr.tests.slowCacheRegenerateMs";

  @Override
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException {
    long sleepMs = Long.getLong(SLEEP_MS_PROPERTY, 0);
    if (sleepMs > 0) {
      try {
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      }
    }
    newCache.put(oldKey, oldVal);
    return true;
  }
}
