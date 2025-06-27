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

package org.apache.solr.savedsearch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.monitor.MonitorQuery;
import org.apache.lucene.monitor.QueryDecomposer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CommonParams;

public class SavedSearchDataValues {

  public static final String QUERY_ID = "_query_id_";
  public static final String MONITOR_QUERY = "_mq_";

  public static final Set<String> REQUIRED_MONITOR_SCHEMA_FIELDS = Set.of(QUERY_ID, MONITOR_QUERY);

  private final SortedDocValues queryIdIt;
  private final SortedDocValues cacheIdIt;
  private final SortedDocValues mqIt;
  private final NumericDocValues versionIt;
  private final LeafReader reader;
  private int currentDoc;

  public SavedSearchDataValues(LeafReaderContext context, String idFieldName) throws IOException {
    reader = context.reader();
    cacheIdIt = reader.getSortedDocValues(idFieldName);
    queryIdIt = reader.getSortedDocValues(QUERY_ID);
    mqIt = reader.getSortedDocValues(MONITOR_QUERY);
    versionIt = reader.getNumericDocValues(CommonParams.VERSION_FIELD);
    currentDoc = DocIdSetIterator.NO_MORE_DOCS;
  }

  public boolean advanceTo(int doc) throws IOException {
    currentDoc = doc;
    return cacheIdIt.advanceExact(currentDoc);
  }

  public String getQueryId() throws IOException {
    queryIdIt.advanceExact(currentDoc);
    return queryIdIt.lookupOrd(queryIdIt.ordValue()).utf8ToString();
  }

  public String getCacheId() throws IOException {
    return cacheIdIt.lookupOrd(cacheIdIt.ordValue()).utf8ToString();
  }

  public String getMq() throws IOException {
    if (mqIt != null && mqIt.advanceExact(currentDoc)) {
      return mqIt.lookupOrd(mqIt.ordValue()).utf8ToString();
    }
    return reader.document(currentDoc).get(MONITOR_QUERY);
  }

  public long getVersion() throws IOException {
    if (versionIt != null && versionIt.advanceExact(currentDoc)) {
      return versionIt.longValue();
    }
    return 0;
  }

  public static class QueryDisjunct {

    private final String disjunctId;
    private final String queryId;
    private final Query matchQuery;
    private final Map<String, String> metadata;

    private QueryDisjunct(
        String disjunctId, String queryId, Query matchQuery, Map<String, String> metadata) {
      this.disjunctId = disjunctId;
      this.queryId = queryId;
      this.matchQuery = matchQuery;
      this.metadata = metadata;
    }

    public static List<QueryDisjunct> decompose(MonitorQuery mq, QueryDecomposer decomposer) {
      int upto = 0;
      List<QueryDisjunct> disjuncts = new ArrayList<>();
      for (var disjunct : decomposer.decompose(mq.getQuery())) {
        String suffix = upto == 0 ? "" : "_" + upto;
        disjuncts.add(
            new QueryDisjunct(mq.getId() + suffix, mq.getId(), disjunct, mq.getMetadata()));
        upto++;
      }
      return disjuncts;
    }

    public Query getMatchQuery() {
      return matchQuery;
    }

    public String getId() {
      return disjunctId;
    }

    public String getQueryId() {
      return queryId;
    }

    public Map<String, String> getMetadata() {
      return metadata;
    }
  }
}
