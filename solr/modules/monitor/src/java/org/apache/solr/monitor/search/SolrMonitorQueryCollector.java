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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.monitor.QCEVisitor;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.solr.monitor.MonitorDataValues;
import org.apache.solr.monitor.SolrMonitorQueryDecoder;
import org.apache.solr.monitor.cache.MonitorQueryCache;
import org.apache.solr.search.DelegatingCollector;

class SolrMonitorQueryCollector extends DelegatingCollector {

  private final MonitorQueryCache monitorQueryCache;
  private final SolrMonitorQueryDecoder queryDecoder;
  private final SolrMatcherSink matcherSink;
  private final MonitorDataValues dataValues = new MonitorDataValues();
  private final boolean writeToDocList;
  private final QueryMatchType queryMatchType;

  SolrMonitorQueryCollector(CollectorContext collectorContext) {
    this.monitorQueryCache = collectorContext.queryCache;
    this.queryDecoder = collectorContext.queryDecoder;
    this.matcherSink = collectorContext.solrMatcherSink;
    this.writeToDocList = collectorContext.writeToDocList;
    this.queryMatchType = collectorContext.queryMatchType;
  }

  @Override
  public void collect(int doc) throws IOException {
    dataValues.advanceTo(doc);
    var entry = getEntry(dataValues);
    var queryId = dataValues.getQueryId();
    var originalMatchQuery = entry.getMatchQuery();
    var matchQuery =
        queryMatchType.needsScores
            ? originalMatchQuery
            : new ConstantScoreQuery(originalMatchQuery);
    boolean isMatch = matcherSink.matchQuery(queryId, matchQuery, entry.getMetadata());
    if (isMatch && writeToDocList) {
      super.collect(doc);
    }
  }

  private QCEVisitor getEntry(MonitorDataValues dataValues) throws IOException {
    var versionedEntry =
        monitorQueryCache == null
            ? null
            : monitorQueryCache.computeIfStale(dataValues, queryDecoder);
    return (versionedEntry == null || versionedEntry.version != dataValues.getVersion())
        ? queryDecoder.getComponent(queryDecoder.decode(dataValues), dataValues.getCacheId())
        : versionedEntry.entry;
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
    super.finish();
    matcherSink.complete();
  }

  static class CollectorContext {

    private final MonitorQueryCache queryCache;
    private final SolrMonitorQueryDecoder queryDecoder;
    private final SolrMatcherSink solrMatcherSink;
    private final boolean writeToDocList;
    private final QueryMatchType queryMatchType;

    CollectorContext(
        MonitorQueryCache queryCache,
        SolrMonitorQueryDecoder queryDecoder,
        SolrMatcherSink solrMatcherSink,
        boolean writeToDocList,
        QueryMatchType queryMatchType) {
      this.queryCache = queryCache;
      this.queryDecoder = queryDecoder;
      this.solrMatcherSink = solrMatcherSink;
      this.writeToDocList = writeToDocList;
      this.queryMatchType = queryMatchType;
    }
  }
}
