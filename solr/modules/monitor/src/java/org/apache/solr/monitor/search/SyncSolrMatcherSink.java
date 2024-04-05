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
import java.util.Map;
import java.util.function.Consumer;
import org.apache.lucene.monitor.CandidateMatcher;
import org.apache.lucene.monitor.MultiMatchingQueries;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.search.Query;

class SyncSolrMatcherSink<T extends QueryMatch> implements SolrMatcherSink {

  private final CandidateMatcher<T> matcher;
  private final Consumer<MultiMatchingQueries<T>> queriesConsumer;
  private int queryCount;

  SyncSolrMatcherSink(
      CandidateMatcher<T> matcher, Consumer<MultiMatchingQueries<T>> queriesConsumer) {
    this.matcher = matcher;
    this.queriesConsumer = queriesConsumer;
  }

  @Override
  public void matchQuery(
      String queryId, Query matchQuery, Map<String, String> metadata, Runnable singleMatchConsumer)
      throws IOException {
    queryCount++;
    if (singleMatchConsumer != null) {
      matcher.setMatchConsumer((__, ___) -> singleMatchConsumer.run());
    }
    matcher.matchQuery(queryId, matchQuery, metadata);
  }

  @Override
  public void complete() {
    queriesConsumer.accept(matcher.finish(Long.MIN_VALUE, queryCount));
  }

  @Override
  public boolean isParallel() {
    return false;
  }
}
