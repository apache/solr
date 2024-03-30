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

import java.util.Map;
import java.util.function.Consumer;
import org.apache.lucene.monitor.MatcherProxy;
import org.apache.lucene.monitor.MultiMatchingQueries;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.monitor.SingleMatchConsumer;
import org.apache.lucene.search.Query;

class SyncSolrMatcherSink<T extends QueryMatch> implements SolrMatcherSink {

  private final MatcherProxy<T> matcherProxy;
  private final Consumer<MultiMatchingQueries<T>> queriesConsumer;
  private int queryCount;

  SyncSolrMatcherSink(
      MatcherProxy<T> matcherProxy, Consumer<MultiMatchingQueries<T>> queriesConsumer) {
    this.matcherProxy = matcherProxy;
    this.queriesConsumer = queriesConsumer;
  }

  @Override
  public void matchQuery(
      String queryId,
      Query matchQuery,
      Map<String, String> metadata,
      SingleMatchConsumer singleMatchConsumer) {
    queryCount++;
    matcherProxy.setNextMatchConsumer(singleMatchConsumer);
    matcherProxy.matchQuery(queryId, matchQuery, metadata);
  }

  @Override
  public void complete() {
    queriesConsumer.accept(matcherProxy.finish(queryCount));
  }

  @Override
  public boolean isParallel() {
    return false;
  }
}
