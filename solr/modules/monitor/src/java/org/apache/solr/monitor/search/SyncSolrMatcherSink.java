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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.monitor.CandidateMatcher;
import org.apache.lucene.monitor.MatchesAggregator;
import org.apache.lucene.monitor.MultiMatchingQueries;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

class SyncSolrMatcherSink<T extends QueryMatch> implements SolrMatcherSink {

  private final Function<IndexSearcher, CandidateMatcher<T>> matcherFactory;
  private final IndexSearcher docBatchSearcher;
  private final Consumer<MultiMatchingQueries<T>> queriesConsumer;
  private final List<CandidateMatcher<T>> matchers = new ArrayList<>();
  private int queryCount;

  SyncSolrMatcherSink(
      Function<IndexSearcher, CandidateMatcher<T>> matcherFactory,
      IndexSearcher docBatchSearcher,
      Consumer<MultiMatchingQueries<T>> queriesConsumer) {
    this.matcherFactory = matcherFactory;
    this.docBatchSearcher = docBatchSearcher;
    this.queriesConsumer = queriesConsumer;
  }

  @Override
  public boolean matchQuery(String queryId, Query matchQuery, Map<String, String> metadata)
      throws IOException {
    queryCount++;
    var matcher = matcherFactory.apply(docBatchSearcher);
    matchers.add(matcher);
    matcher.matchQuery(queryId, matchQuery, metadata);
    var matches = matcher.finish(Long.MIN_VALUE, 1);
    for (int doc = 0; doc < matches.getBatchSize(); doc++) {
      var match = matches.getMatches(doc);
      if (!match.isEmpty()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void complete() {
    queriesConsumer.accept(
        MatchesAggregator.aggregate(matchers, matcherFactory.apply(docBatchSearcher), queryCount));
  }
}
