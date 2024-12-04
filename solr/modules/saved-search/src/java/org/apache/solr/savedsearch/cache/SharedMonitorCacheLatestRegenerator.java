/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.solr.savedsearch.cache;

import java.io.IOException;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.savedsearch.MonitorDataValues;
import org.apache.solr.savedsearch.SolrMonitorQueryDecoder;
import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

public class SharedMonitorCacheLatestRegenerator implements CacheRegenerator {

  private static final int MAX_BATCH_SIZE = 1 << 10;

  @Override
  public <K, V> boolean regenerateItem(
      SolrIndexSearcher searcher,
      SolrCache<K, V> newCache,
      SolrCache<K, V> oldCache,
      K oldKey,
      V oldVal)
      throws IOException {
    if (!(newCache instanceof SharedMonitorCache)) {
      throw new IllegalArgumentException(
          this.getClass().getSimpleName()
              + " only supports "
              + SharedMonitorCache.class.getSimpleName());
    }
    var cache = (SharedMonitorCache) newCache;
    var reader = searcher.getIndexReader();
    int batchSize = Math.min(MAX_BATCH_SIZE, cache.getInitialSize());
    var topDocs =
        new IndexSearcher(searcher.getTopReaderContext())
            .search(versionRangeQuery(cache), batchSize)
            .scoreDocs;
    int batchesRemaining = cache.getInitialSize() / batchSize - 1;
    SolrMonitorQueryDecoder decoder = new SolrMonitorQueryDecoder(searcher.getCore());
    while (topDocs.length > 0 && batchesRemaining > 0) {
      int docIndex = 0;
      for (LeafReaderContext ctx : reader.leaves()) {
        MonitorDataValues dataValues = new MonitorDataValues(ctx);
        int shiftedMax = ctx.reader().maxDoc() + ctx.docBase;
        while (docIndex < topDocs.length
            && topDocs[docIndex].doc >= ctx.docBase
            && topDocs[docIndex].doc < shiftedMax) {
          int doc = topDocs[docIndex].doc - ctx.docBase;
          docIndex++;
          if (dataValues.advanceTo(doc)) {
            cache.computeIfStale(dataValues, decoder);
          }
          cache.versionHighWaterMark =
              Math.max(cache.versionHighWaterMark, dataValues.getVersion());
          cache.docVisits++;
        }
      }
      var scoreDoc = topDocs[topDocs.length - 1];
      topDocs =
          new IndexSearcher(searcher.getTopReaderContext())
              .searchAfter(scoreDoc, versionRangeQuery(cache), batchSize)
              .scoreDocs;
      batchesRemaining--;
    }
    return false;
  }

  private static Query versionRangeQuery(SharedMonitorCache cache) {
    return LongPoint.newRangeQuery(
        CommonParams.VERSION_FIELD, cache.versionHighWaterMark, Long.MAX_VALUE);
  }
}
