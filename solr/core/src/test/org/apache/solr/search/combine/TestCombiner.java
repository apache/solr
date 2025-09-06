/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search.combine;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;

/**
 * The TestCombiner class is an extension of QueryAndResponseCombiner that implements custom logic
 * for combining ranked lists using linear sorting of score from all rank lists. This is just for
 * testing purpose which has been used in test suite CombinedQueryComponentTest for e2e testing of
 * Plugin based Combiner approach.
 */
public class TestCombiner extends QueryAndResponseCombiner {

  private int testInt;

  public int getTestInt() {
    return testInt;
  }

  @Override
  public void init(NamedList<?> args) {
    Object kParam = args.get("var1");
    if (kParam != null) {
      this.testInt = Integer.parseInt(kParam.toString());
    }
  }

  @Override
  public QueryResult combine(List<QueryResult> rankedLists, SolrParams solrParams) {
    HashMap<Integer, Float> docIdToScore = new HashMap<>();
    QueryResult queryResult = new QueryResult();
    for (QueryResult rankedList : rankedLists) {
      DocIterator docs = rankedList.getDocList().iterator();
      while (docs.hasNext()) {
        int docId = docs.nextDoc();
        docIdToScore.put(docId, docs.score());
      }
    }
    List<Map.Entry<Integer, Float>> sortedByScoreDescending =
        docIdToScore.entrySet().stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
            .toList();
    int combinedResultsLength = docIdToScore.size();
    int[] combinedResultsDocIds = new int[combinedResultsLength];
    float[] combinedResultScores = new float[combinedResultsLength];

    int i = 0;
    for (Map.Entry<Integer, Float> scoredDoc : sortedByScoreDescending) {
      combinedResultsDocIds[i] = scoredDoc.getKey();
      combinedResultScores[i] = scoredDoc.getValue();
      i++;
    }
    DocSlice combinedResultSlice =
        new DocSlice(
            0,
            combinedResultsLength,
            combinedResultsDocIds,
            combinedResultScores,
            combinedResultsLength,
            combinedResultScores.length > 0 ? combinedResultScores[0] : 0,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    queryResult.setDocList(combinedResultSlice);
    SortedIntDocSet docSet = new SortedIntDocSet(combinedResultsDocIds, combinedResultsLength);
    queryResult.setDocSet(docSet);
    return queryResult;
  }

  @Override
  public List<ShardDoc> combine(Map<String, List<ShardDoc>> shardDocMap, SolrParams solrParams) {
    return List.of();
  }

  @Override
  public NamedList<Explanation> getExplanations(
      String[] queryKeys,
      List<Query> queries,
      List<QueryResult> queryResults,
      SolrIndexSearcher searcher,
      IndexSchema schema,
      SolrParams solrParams)
      throws IOException {
    return null;
  }
}
