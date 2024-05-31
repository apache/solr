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
 * Reciprocal Rank Fusion (RRF) is an algorithm that takes in input multiple ranked results to
 * produce a unified result set. Examples of use cases where RRF can be used include hybrid search
 * and multiple Knn vector queries executed concurrently. RRF is based on the concept of reciprocal
 * rank, which is the inverse of the rank position of a document in a ranked list of search results.
 * The combination of search results happens taking into account the position of the items in the
 * original rankings, and giving higher score to items that are ranked higher in multiple lists. RRF
 * was introduced the first time by Cormack et al. in [1].<br>
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
  public QueryResult combine(QueryResult[] queryResults) {
    QueryResult rrfResult = initCombinedResult(queryResults);
    List<DocList> queriesResultsDocLists = new ArrayList<>(queryResults.length);
    for (QueryResult singleQuery : queryResults) {
      queriesResultsDocLists.add(singleQuery.getDocList());
    }
    combineResults(rrfResult, queriesResultsDocLists, null);
    return rrfResult;
  }

  private void combineResults(
      QueryResult combinedResults,
      List<DocList> queriesResults,
      Map<Integer, Integer[]> docIdToRankPositions) {
    HashMap<Integer, Float> docIdToScore = new HashMap<>();
    for (DocList singleQuery : queriesResults) {
      DocIterator iterator = singleQuery.iterator();
      int ranking = 1;
      while (iterator.hasNext() && ranking <= upTo) {
        int docId = iterator.nextDoc();
        float rrfScore = 1f / (k + ranking);
        docIdToScore.compute(docId, (id, score) -> (score == null) ? rrfScore : score + rrfScore);
        ranking++;
      }
    }

    Stream<Map.Entry<Integer, Float>> sortedResults =
        docIdToScore.entrySet().stream()
            .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));

    int combinedResultsLength = docIdToScore.size();
    int[] combinedResultsDocIDs = new int[combinedResultsLength];
    float[] combinedResultScores = new float[combinedResultsLength];

    int i = 0;
    for (Map.Entry<Integer, Float> scoredDoc : sortedResults.collect(Collectors.toList())) {
      combinedResultsDocIDs[i] = scoredDoc.getKey();
      combinedResultScores[i] = scoredDoc.getValue();
      i++;
    }
    boolean prepareForExplainability = docIdToRankPositions != null;
    if (prepareForExplainability) {
      for (int docID : combinedResultsDocIDs) {
        docIdToRankPositions.put(docID, new Integer[queriesResults.size()]);
      }
      for (int j = 0; j < queriesResults.size(); j++) {
        DocIterator iterator = queriesResults.get(j).iterator();
        int ranking = 1;
        while (iterator.hasNext() && ranking <= upTo) {
          int docId = iterator.nextDoc();
          docIdToRankPositions.get(docId)[j] = ranking;
          ranking++;
        }
      }
    }

    DocSlice combinedResultSlice =
        new DocSlice(
            0,
            combinedResultsLength,
            combinedResultsDocIDs,
            combinedResultScores,
            combinedResultsLength,
            combinedResultScores[0],
            GREATER_THAN_OR_EQUAL_TO);
    combinedResults.setDocList(combinedResultSlice);
    SortedIntDocSet docSet = new SortedIntDocSet(combinedResultsDocIDs, combinedResultsLength);
    combinedResults.setDocSet(docSet);
  }

  public NamedList<Explanation> getExplanations(
      String[] queriesKeys,
      List<Query> queriesToCombine,
      List<DocList> resultsPerQuery,
      SolrIndexSearcher searcher,
      IndexSchema schema)
      throws IOException {
    NamedList<Explanation> docIdsExplanations = new SimpleOrderedMap<>();
    Map<Integer, Integer[]> docIdToRankPositions = new HashMap<>();
    QueryResult combinedResult = new QueryResult();
    combineResults(combinedResult, resultsPerQuery, docIdToRankPositions);
    DocList combinedDocList = combinedResult.getDocList();
    // explain each result
    DocIterator iterator = combinedDocList.iterator();
    for (int i = 0; i < combinedDocList.size(); i++) {
      int docId = iterator.nextDoc();
      Integer[] rankPositions = docIdToRankPositions.get(docId);
      Document doc = searcher.doc(docId);
      String strid = schema.printableUniqueKey(doc);
      List<Explanation> originalExplanations = new ArrayList<>(queriesKeys.length);
      for (int queryIndex = 0; queryIndex < queriesKeys.length; queryIndex++) {
        Explanation originalFullExplain = searcher.explain(queriesToCombine.get(queryIndex), docId);
        Explanation originalQuery =
            Explanation.match(
                originalFullExplain.getValue(), queriesKeys[queryIndex], originalFullExplain);
        originalExplanations.add(originalQuery);
      }
      Explanation fullDocIdExplanation =
          Explanation.match(
              iterator.score(),
              getReciprocalRankFusionExplain(queriesKeys, rankPositions),
              originalExplanations);
      docIdsExplanations.add(strid, fullDocIdExplanation);
    }
    return docIdsExplanations;
  }

  private String getReciprocalRankFusionExplain(String[] queriesKeys, Integer[] rankPositions) {
    StringBuilder explainDescription = new StringBuilder();
    StringJoiner scoreComponents = new StringJoiner(" + ");
    for (Integer rank : rankPositions) {
      if (rank != null) {
        scoreComponents.add("1/(" + k + "+" + rank + ")");
      }
    }
    explainDescription.append(scoreComponents);
    explainDescription.append(" because its ranking positions were: ");
    StringJoiner rankingComponents = new StringJoiner(", ");
    for (int i = 0; i < queriesKeys.length; i++) {

      Integer rank = rankPositions[i];
      if (rank == null) {
        rankingComponents.add("not in the results for query(" + queriesKeys[i] + ")");
      } else {
        rankingComponents.add(rank + " for query(" + queriesKeys[i] + ")");
      }
    }
    explainDescription.append(rankingComponents);
    return explainDescription.toString();
  }
}
