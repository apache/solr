/*
 *
 *  *
 *  *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  *  * contributor license agreements.  See the NOTICE file distributed with
 *  *  * this work for additional information regarding copyright ownership.
 *  *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  *  * (the "License"); you may not use this file except in compliance with
 *  *  * the License.  You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 */

package org.apache.lucene.monitor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

public class Visitors {

  public static class MonitorFields {
    public static final String QUERY_ID = QueryIndex.FIELDS.query_id + "_";
    public static final String CACHE_ID = QueryIndex.FIELDS.cache_id + "_";
    public static final String MONITOR_QUERY = QueryIndex.FIELDS.mq + "_";

    public static final Set<String> REQUIRED_MONITOR_SCHEMA_FIELDS =
        Set.of(QUERY_ID, CACHE_ID, MONITOR_QUERY);
  }

  public static class DocumentBatchVisitor implements AutoCloseable, Supplier<LeafReader> {

    private final DocumentBatch batch;
    private final List<Document> docs;

    private DocumentBatchVisitor(DocumentBatch batch, List<Document> docs) {
      this.batch = batch;
      this.docs = docs;
    }

    public static DocumentBatchVisitor of(Analyzer analyzer, List<Document> docs) {
      return new DocumentBatchVisitor(
          DocumentBatch.of(analyzer, docs.toArray(new Document[0])), docs);
    }

    @Override
    public void close() throws IOException {
      batch.close();
    }

    @Override
    public LeafReader get() {
      return batch.get();
    }

    public int size() {
      return docs.size();
    }

    @Override
    public String toString() {
      return docs.stream().map(Document::toString).collect(Collectors.joining(" "));
    }
  }

  public static class QueryTermFilterVisitor implements BiPredicate<String, BytesRef> {

    private final QueryIndex.QueryTermFilter queryTermFilter;

    public QueryTermFilterVisitor(IndexReader reader) throws IOException {
      this.queryTermFilter = new QueryIndex.QueryTermFilter(reader);
    }

    @Override
    public boolean test(String field, BytesRef bytesRef) {
      return queryTermFilter.test(field, bytesRef);
    }
  }

  public static class QCEVisitor {

    private final QueryCacheEntry qce;

    private QCEVisitor(QueryCacheEntry qce) {
      this.qce = qce;
    }

    public static List<QCEVisitor> decompose(MonitorQuery mq, QueryDecomposer decomposer) {
      List<QCEVisitor> cacheEntries = new ArrayList<>();
      for (var queryCacheEntry : QueryCacheEntry.decompose(mq, decomposer)) {
        cacheEntries.add(new QCEVisitor(queryCacheEntry));
      }
      return cacheEntries;
    }

    public Query getMatchQuery() {
      return qce.matchQuery;
    }

    public String getCacheId() {
      return qce.cacheId;
    }

    public String getQueryId() {
      return qce.queryId;
    }

    public Map<String, String> getMetadata() {
      return qce.metadata;
    }

    @Override
    public String toString() {
      return qce.toString();
    }
  }
}
