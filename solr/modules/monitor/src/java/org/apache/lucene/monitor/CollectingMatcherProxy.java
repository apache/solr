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
import java.util.Map;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;

public abstract class CollectingMatcherProxy<T extends QueryMatch> extends CollectingMatcher<T>
    implements MatcherProxy<T> {

  private SingleMatchConsumer singleMatchConsumer;
  private MultiMatchingQueries<T> cachedResult;

  public CollectingMatcherProxy(IndexSearcher searcher, ScoreMode scoreMode) {
    super(searcher, scoreMode);
  }

  @Override
  public void matchQuery(String queryId, Query matchQuery, Map<String, String> metadata) {
    try {
      super.matchQuery(queryId, matchQuery, metadata);
    } catch (Exception e) {
      super.reportError(queryId, e);
    }
  }

  @Override
  public MultiMatchingQueries<T> finish(int queryCount) {
    cachedResult = super.finish(Long.MIN_VALUE, queryCount);
    return cachedResult;
  }

  protected void consumeMatch(String queryId, int batchDocId) throws IOException {
    if (singleMatchConsumer != null) {
      singleMatchConsumer.accept(queryId, batchDocId);
    }
  }

  @Override
  public void setNextMatchConsumer(SingleMatchConsumer singleMatchConsumer) {
    this.singleMatchConsumer = singleMatchConsumer;
  }

  @Override
  public MultiMatchingQueries<T> matches() {
    return cachedResult;
  }
}
