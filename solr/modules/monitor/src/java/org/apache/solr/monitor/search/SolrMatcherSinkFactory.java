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
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.monitor.DocumentBatchVisitor;
import org.apache.lucene.monitor.HighlightMatcherProxy;
import org.apache.lucene.monitor.MatcherProxy;
import org.apache.lucene.monitor.MultiMatchingQueries;
import org.apache.lucene.monitor.QueryMatch;
import org.apache.lucene.monitor.SimpleMatcherProxy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;

class SolrMatcherSinkFactory {

  private final ExecutorService executorService;

  SolrMatcherSinkFactory() {
    this(null);
  }

  SolrMatcherSinkFactory(ExecutorService executorService) {
    this.executorService = executorService;
  }

  SolrMatcherSink build(
      QueryMatchType matchType,
      DocumentBatchVisitor documentBatch,
      Map<String, Object> monitorResult) {
    if (matchType == QueryMatchType.SIMPLE) {
      return buildSimple(
          documentBatch,
          matchingQueries ->
              QueryMatchResponseCodec.simpleEncode(
                  matchingQueries, monitorResult, documentBatch.size()));
    } else if (matchType == QueryMatchType.HIGHLIGHTS) {
      return build(
          documentBatch,
          HighlightMatcherProxy::new,
          matchingQueries ->
              QueryMatchResponseCodec.highlightEncode(
                  matchingQueries, monitorResult, documentBatch.size()));
    }
    return buildSimple(documentBatch, __ -> {});
  }

  private SolrMatcherSink buildSimple(
      DocumentBatchVisitor documentBatch, Consumer<MultiMatchingQueries<QueryMatch>> encoder) {
    return build(
        documentBatch,
        searcher -> new SimpleMatcherProxy(searcher, ScoreMode.COMPLETE_NO_SCORES),
        encoder);
  }

  private <T extends QueryMatch> SolrMatcherSink build(
      DocumentBatchVisitor documentBatch,
      Function<IndexSearcher, MatcherProxy<T>> matcherProxyFactory,
      Consumer<MultiMatchingQueries<T>> encoder) {
    var docBatchSearcher = new IndexSearcher(documentBatch.get());
    if (executorService == null) {
      return new SyncSolrMatcherSink<>(matcherProxyFactory.apply(docBatchSearcher), encoder);
    } else {
      return new ParallelSolrMatcherSink<>(
          executorService, matcherProxyFactory, docBatchSearcher, encoder);
    }
  }
}
