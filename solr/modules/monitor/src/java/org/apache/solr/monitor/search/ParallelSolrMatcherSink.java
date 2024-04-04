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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.monitor.AggregatingMatcher;
import org.apache.lucene.monitor.MatcherProxy;
import org.apache.lucene.monitor.MultiMatchingQueries;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.monitor.SingleMatchConsumer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;

class ParallelSolrMatcherSink<T extends QueryMatch> implements SolrMatcherSink {

  private final CompletionService<MultiMatchingQueries<T>> completionService;
  private final Function<IndexSearcher, MatcherProxy<T>> matcherProxyFactory;
  private final IndexSearcher docBatchSearcher;
  private final Consumer<MultiMatchingQueries<T>> queriesConsumer;
  private int tasksLeft;

  public ParallelSolrMatcherSink(
      ExecutorService executor,
      Function<IndexSearcher, MatcherProxy<T>> matcherProxyFactory,
      IndexSearcher docBatchSearcher,
      Consumer<MultiMatchingQueries<T>> queriesConsumer) {
    this.completionService = new ExecutorCompletionService<>(executor);
    this.matcherProxyFactory = matcherProxyFactory;
    this.docBatchSearcher = docBatchSearcher;
    this.queriesConsumer = queriesConsumer;
  }

  @Override
  public void matchQuery(
      String queryId,
      Query matchQuery,
      Map<String, String> metadata,
      SingleMatchConsumer matchConsumer) {
    completionService.submit(
        () -> {
          var matcherProxy = matcherProxyFactory.apply(docBatchSearcher);
          matcherProxy.matchQuery(queryId, matchQuery, metadata);
          matcherProxy.finish(1);
          return matcherProxy.matches();
        });
    tasksLeft++;
  }

  @Override
  public void complete() throws IOException {
    List<MultiMatchingQueries<T>> matchingQueries = new ArrayList<>();
    int queryCount = tasksLeft;
    while (tasksLeft-- > 0) {
      try {
        matchingQueries.add(completionService.take().get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IllegalStateException(
            MatcherProxy.class.getSimpleName() + " should never throw an exception", e);
      }
    }
    BiFunction<T, T, T> resolver = matcherProxyFactory.apply(docBatchSearcher)::resolve;
    var aggregatingMatcher = AggregatingMatcher.build(matchingQueries, docBatchSearcher, resolver);
    queriesConsumer.accept(aggregatingMatcher.finish(queryCount));
  }

  @Override
  public boolean isParallel() {
    return true;
  }
}
