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

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

public class HighlightMatcherProxy implements MatcherProxy<HighlightsMatch> {

  private final List<MultiMatchingQueries<HighlightsMatch>> multiMatchingQueries =
      new ArrayList<>();
  private final IndexSearcher indexSearcher;
  private final CandidateMatcher<HighlightsMatch> resolverMatcher;
  private SingleMatchConsumer singleMatchConsumer;
  private MultiMatchingQueries<HighlightsMatch> cachedResult;

  public HighlightMatcherProxy(IndexSearcher indexSearcher) {
    this.indexSearcher = indexSearcher;
    this.resolverMatcher = HighlightsMatch.MATCHER.createMatcher(indexSearcher);
  }

  @Override
  public void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) {
    try {
      var highlightMatcher = HighlightsMatch.MATCHER.createMatcher(indexSearcher);
      highlightMatcher.matchQuery(queryId, matchQuery, metadata);
      var matchingQueries = highlightMatcher.finish(0, 0);
      this.multiMatchingQueries.add(matchingQueries);
      if (singleMatchConsumer != null) {
        for (int i = 0; i < matchingQueries.getBatchSize(); i++) {
          if (!matchingQueries.getMatches(i).isEmpty()) {
            singleMatchConsumer.accept(queryId, i);
          }
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("This should be impossible for an in-memory searcher", e);
    }
  }

  @Override
  public MultiMatchingQueries<HighlightsMatch> finish(int queryCount) {
    var aggregator = AggregatingMatcher.build(multiMatchingQueries, indexSearcher, this::resolve);
    cachedResult = aggregator.finish(queryCount);
    return cachedResult;
  }

  @Override
  public void setNextMatchConsumer(SingleMatchConsumer singleMatchConsumer) {
    this.singleMatchConsumer = singleMatchConsumer;
  }

  @Override
  public MultiMatchingQueries<HighlightsMatch> matches() {
    return cachedResult;
  }

  @Override
  public HighlightsMatch resolve(HighlightsMatch match1, HighlightsMatch match2) {
    return resolverMatcher.resolve(match1, match2);
  }
}
