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
import java.lang.invoke.MethodHandles;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple cache regenerator that computes the new value by executing the Query keys from old cache
 * on the new Searcher
 */
public class SimpleQueryRegenerator implements CacheRegenerator {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @SuppressWarnings("unchecked")
  @Override
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher newSearcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException {
    if (oldKey instanceof Query && oldVal instanceof DocSet) {
      newCache.computeIfAbsent(
          oldKey,
          (k) -> {
            ExtendedQuery noCache = new WrappedQuery((Query) oldKey);
            noCache.setCache(false);

            return (V) newSearcher.getDocSet((Query) noCache);
          });
      return true;
    } else {
      if (log.isWarnEnabled()) {
        log.warn(
            "Not regenerating item as key should be a Query and val a DocSet, but found key of class {} and val of class {}",
            oldKey != null ? oldKey.getClass().getName() : "null",
            oldVal != null ? oldVal.getClass().getName() : "null");
      }
      return false;
    }
  }
}
