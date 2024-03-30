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
import org.apache.lucene.monitor.DocumentBatchVisitor;
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

  SolrMatcherSink buildSimple(
      DocumentBatchVisitor documentBatch, Map<String, Object> monitorResult) {
    Consumer<MultiMatchingQueries<QueryMatch>> simpleEncoder =
        matchingQueries ->
            QueryMatchResponseCodec.simpleEncode(
                matchingQueries, monitorResult, documentBatch.size());
    var docBatchSearcher = new IndexSearcher(documentBatch.get());
    if (executorService == null) {
      return new SyncSolrMatcherSink<>(buildSimpleMatcherProxy(docBatchSearcher), simpleEncoder);
    } else {
      return new ParallelSolrMatcherSink<>(
          executorService, this::buildSimpleMatcherProxy, docBatchSearcher, simpleEncoder);
    }
  }

  private MatcherProxy<QueryMatch> buildSimpleMatcherProxy(IndexSearcher docBatchSearcher) {
    return new SimpleMatcherProxy(docBatchSearcher, ScoreMode.COMPLETE_NO_SCORES);
  }
}
