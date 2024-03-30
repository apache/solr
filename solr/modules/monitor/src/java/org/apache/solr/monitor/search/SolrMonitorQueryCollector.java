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

package org.apache.solr.monitor.search;

import java.io.IOException;
import java.util.function.Function;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.monitor.MonitorDataValues;
import org.apache.lucene.monitor.QCEVisitor;
import org.apache.lucene.monitor.SingleMatchConsumer;
import org.apache.lucene.search.ScoreMode;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;
import org.apache.solr.monitor.cache.MonitorQueryCache;
import org.apache.solr.monitor.cache.VersionedQueryCacheEntry;
import org.apache.solr.search.DelegatingCollector;

class SolrMonitorQueryCollector extends DelegatingCollector {

  private final MonitorQueryCache monitorQueryCache;
  private final SolrMonitorQueryDecoder queryDecoder;
  private final SolrMatcherSink matcherSink;
  private final MonitorDataValues dataValues = new MonitorDataValues();
  private final Function<Integer, SingleMatchConsumer> docIdForwarder;

  SolrMonitorQueryCollector(CollectorContext collectorContext) {
    this.monitorQueryCache = collectorContext.queryCache;
    this.queryDecoder = collectorContext.queryDecoder;
    this.matcherSink = collectorContext.solrMatcherSink;
    if (collectorContext.writeToDocList) {
      this.docIdForwarder = MatchingDocForwarder::new;
    } else {
      this.docIdForwarder = __ -> null;
    }
  }

  @Override
  public void collect(int doc) throws IOException {
    dataValues.advanceTo(doc);
    var versionedEntry = monitorQueryCache.computeIfStale(dataValues, queryDecoder);
    var entry = getEntry(versionedEntry, dataValues);
    var queryId = dataValues.getQueryId();
    var forwarder = docIdForwarder.apply(doc);
    matcherSink.matchQuery(queryId, entry.getMatchQuery(), entry.getMetadata(), forwarder);
  }

  private QCEVisitor getEntry(VersionedQueryCacheEntry versionedEntry, MonitorDataValues dataValues)
      throws IOException {
    if (versionedEntry.version != dataValues.getVersion()) {
      // The cache is more up-to-date than the index associated with this collector.
      // For consistency, we parse the query from the earlier index state.
      return QCEVisitor.getComponent(
          queryDecoder.decode(dataValues),
          monitorQueryCache.getDecomposer(),
          dataValues.getCacheId());
    }
    return versionedEntry.entry;
  }

  private void superCollect(int doc) throws IOException {
    super.collect(doc);
  }

  @Override
  public void doSetNextReader(LeafReaderContext context) throws IOException {
    super.doSetNextReader(context);
    dataValues.update(context);
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  @Override
  public void complete() throws IOException {
    super.complete();
    matcherSink.complete();
  }

  static class CollectorContext {

    private final MonitorQueryCache queryCache;
    private final SolrMonitorQueryDecoder queryDecoder;
    private final SolrMatcherSink solrMatcherSink;
    private final boolean writeToDocList;

    CollectorContext(
        MonitorQueryCache queryCache,
        SolrMonitorQueryDecoder queryDecoder,
        SolrMatcherSink solrMatcherSink,
        boolean writeToDocList) {
      this.queryCache = queryCache;
      this.queryDecoder = queryDecoder;
      this.solrMatcherSink = solrMatcherSink;
      this.writeToDocList = writeToDocList;
    }
  }

  private class MatchingDocForwarder implements SingleMatchConsumer {

    private final int doc;
    private boolean visited;

    private MatchingDocForwarder(int doc) {
      this.doc = doc;
    }

    @Override
    public void accept(String __, int ___) throws IOException {
      if (!visited) {
        superCollect(doc);
      }
      visited = true;
    }
  }
}
