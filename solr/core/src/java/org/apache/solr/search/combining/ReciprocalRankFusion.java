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

package org.apache.solr.search.combining;

import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.SortedIntDocSet;

/**
 * Reciprocal Rank Fusion (RRF) is an algorithm that takes in input multiple ranked lists to
 * produce a unified result set. Examples of use cases where RRF can be used include hybrid search
 * and multiple Knn vector queries executed concurrently. RRF is based on the concept of reciprocal
 * rank, which is the inverse of the rank of a document in a ranked list of search
 * results. The combination of search results happens taking into account the position of
 * the items in the original rankings, and giving higher score to items that are ranked higher in
 * multiple lists. RRF was introduced the first time by Cormack et al. in [1].<br>
 *
 * <p>[1] Cormack, Gordon V. et al. “Reciprocal rank fusion outperforms condorcet and individual
 * rank learning methods.” Proceedings of the 32nd international ACM SIGIR conference on Research
 * and development in information retrieval (2009)<br>
 */
public class ReciprocalRankFusion extends QueriesCombiner {
  int k;

  public ReciprocalRankFusion(SolrParams requestParams) {
    super(requestParams);
    this.k =
        requestParams.getInt(CombinerParams.COMBINER_RRF_K, CombinerParams.COMBINER_RRF_K_DEFAULT);
  }

  @Override
  public QueryResult combine(QueryResult[] rankedLists) {
    QueryResult combinedRankedList = initCombinedResult(rankedLists);
    List<DocList> docLists = new ArrayList<>(rankedLists.length);
    for (QueryResult rankedList : rankedLists) {
      docLists.add(rankedList.getDocList());
    }
    combineResults(combinedRankedList, docLists, false);
    return combinedRankedList;
  }

  private Map<Integer, Integer[]> combineResults(
      QueryResult combinedRankedList,
      List<DocList> rankedLists,
      boolean saveRankPositionsForExplain) {
    Map<Integer, Integer[]> docIdToRanks = null;
    HashMap<Integer, Float> docIdToScore = new HashMap<>();
    for (DocList rankedList : rankedLists) {
      DocIterator docs = rankedList.iterator();
      int ranking = 1;
      while (docs.hasNext() && ranking <= upTo) {
        int docId = docs.nextDoc();
        float rrfScore = 1f / (k + ranking);
        docIdToScore.compute(docId, (id, score) -> (score == null) ? rrfScore : score + rrfScore);
        ranking++;
      }
    }
    Stream<Map.Entry<Integer, Float>> sortedByScoreDescending =
        docIdToScore.entrySet().stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));

    int combinedResultsLength = docIdToScore.size();
    int[] combinedResultsDocIds = new int[combinedResultsLength];
    float[] combinedResultScores = new float[combinedResultsLength];

    int i = 0;
    for (Map.Entry<Integer, Float> scoredDoc :
        sortedByScoreDescending.collect(Collectors.toList())) {
      combinedResultsDocIds[i] = scoredDoc.getKey();
      combinedResultScores[i] = scoredDoc.getValue();
      i++;
    }

    if (saveRankPositionsForExplain) {
      docIdToRanks = getRanks(rankedLists, combinedResultsDocIds);
    }

    DocSlice combinedResultSlice =
        new DocSlice(
            0,
            combinedResultsLength,
            combinedResultsDocIds,
            combinedResultScores,
            combinedResultsLength,
            combinedResultScores[0],
            GREATER_THAN_OR_EQUAL_TO);
    combinedRankedList.setDocList(combinedResultSlice);
    SortedIntDocSet docSet = new SortedIntDocSet(combinedResultsDocIds, combinedResultsLength);
    combinedRankedList.setDocSet(docSet);

    return docIdToRanks;
  }

  private Map<Integer, Integer[]> getRanks(List<DocList> docLists, int[] combinedResultsDocIDs) {
    Map<Integer, Integer[]> docIdToRanks;
    docIdToRanks = new HashMap<>();
    for (int docID : combinedResultsDocIDs) {
      docIdToRanks.put(docID, new Integer[docLists.size()]);
    }
    for (int j = 0; j < docLists.size(); j++) {
      DocIterator iterator = docLists.get(j).iterator();
      int rank = 1;
      while (iterator.hasNext() && rank <= upTo) {
        int docId = iterator.nextDoc();
        docIdToRanks.get(docId)[j] = rank;
        rank++;
      }
    }
    return docIdToRanks;
  }

  public NamedList<Explanation> getExplanations(
      String[] queryKeys,
      List<Query> queries,
      List<DocList> rankedLists,
      SolrIndexSearcher searcher,
      IndexSchema schema)
      throws IOException {
    NamedList<Explanation> docIdsExplanations = new SimpleOrderedMap<>();
    QueryResult combinedRankedList = new QueryResult();
    Map<Integer, Integer[]> docIdToRanks = combineResults(combinedRankedList, rankedLists, true);
    DocList combinedDocList = combinedRankedList.getDocList();
    // explain each result
    DocIterator iterator = combinedDocList.iterator();
    for (int i = 0; i < combinedDocList.size(); i++) {
      int docId = iterator.nextDoc();
      Integer[] rankPerQuery = docIdToRanks.get(docId);
      Document doc = searcher.doc(docId);
      String docUniqueKey = schema.printableUniqueKey(doc);
      List<Explanation> originalExplanations = new ArrayList<>(queryKeys.length);
      for (int queryIndex = 0; queryIndex < queryKeys.length; queryIndex++) {
        Explanation originalQueryExplain = searcher.explain(queries.get(queryIndex), docId);
        Explanation originalQueryExplainWithKey =
            Explanation.match(
                originalQueryExplain.getValue(), queryKeys[queryIndex], originalQueryExplain);
        originalExplanations.add(originalQueryExplainWithKey);
      }
      Explanation fullDocIdExplanation =
          Explanation.match(
              iterator.score(),
              getReciprocalRankFusionExplain(queryKeys, rankPerQuery),
              originalExplanations);
      docIdsExplanations.add(docUniqueKey, fullDocIdExplanation);
    }
    return docIdsExplanations;
  }

  private String getReciprocalRankFusionExplain(String[] queryKeys, Integer[] rankPerQuery) {
    StringBuilder reciprocalRankFusionExplain = new StringBuilder();
    StringJoiner scoreComponents = new StringJoiner(" + ");
    for (Integer rank : rankPerQuery) {
      if (rank != null) {
        scoreComponents.add("1/(" + k + "+" + rank + ")");
      }
    }
    reciprocalRankFusionExplain.append(scoreComponents);
    reciprocalRankFusionExplain.append(" because its ranks were: ");
    StringJoiner rankComponents = new StringJoiner(", ");
    for (int i = 0; i < queryKeys.length; i++) {
      Integer rank = rankPerQuery[i];
      if (rank == null) {
        rankComponents.add("not in the results for query(" + queryKeys[i] + ")");
      } else {
        rankComponents.add(rank + " for query(" + queryKeys[i] + ")");
      }
    }
    reciprocalRankFusionExplain.append(rankComponents);
    return reciprocalRankFusionExplain.toString();
  }
}
