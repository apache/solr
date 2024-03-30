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

import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Query;

public class AggregatingMatcher<T extends QueryMatch> extends CandidateMatcher<T> {

  private final CandidateMatcher<T> collector;

  private AggregatingMatcher(List<CandidateMatcher<T>> matchers, CandidateMatcher<T> collector) {
    super(collector.searcher);
    this.collector = collector;
    for (var matcher : matchers) {
      MultiMatchingQueries<T> matches = matcher.finish(0, 0);
      for (int doc = 0; doc < matches.getBatchSize(); doc++) {
        for (T match : matches.getMatches(doc)) {
          this.addMatch(match, doc);
        }
      }
      for (Map.Entry<String, Exception> error : matches.getErrors().entrySet()) {
        this.reportError(error.getKey(), error.getValue());
      }
    }
  }

  @Override
  protected void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) {
    throw new UnsupportedOperationException("only use for aggregating other matchers");
  }

  public MultiMatchingQueries<T> finish(int queryCount) {
    return finish(Long.MIN_VALUE, queryCount);
  }

  @Override
  public T resolve(T match1, T match2) {
    return collector.resolve(match1, match2);
  }

  public static <T extends QueryMatch> AggregatingMatcher<T> build(
      List<CandidateMatcher<T>> matchers, CandidateMatcher<T> collector) {
    return new AggregatingMatcher<>(matchers, collector);
  }
}
