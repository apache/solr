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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.apache.solr.search.SolrDocumentFetcher;
import org.apache.solr.search.SolrIndexSearcher;

/**
 * This class implements a query and response combiner that uses the Reciprocal Rank Fusion (RRF)
 * algorithm to combine multiple ranked lists into a single ranked list.
 */
public class ReciprocalRankFusion extends QueryAndResponseCombiner {

  private int k;

  public ReciprocalRankFusion() {
    this.k = CombinerParams.DEFAULT_COMBINER_RRF_K;
  }

  @Override
  public void init(NamedList<?> args) {
    Object kParam = args.get("k");
    if (kParam != null) {
      this.k = Integer.parseInt(kParam.toString());
    }
  }

  public int getK() {
    return k;
  }

  @Override
  public QueryResult combine(List<QueryResult> rankedLists, SolrParams solrParams) {
    int kVal = solrParams.getInt(CombinerParams.COMBINER_RRF_K, this.k);
    QueryResult combinedResult = new QueryResult();
    combineResults(combinedResult, rankedLists, false, kVal);
    return combinedResult;
  }

  /**
   * Merges per-query ranked results using Reciprocal Rank Fusion (RRF).
   *
   * <p>Each query doc list is assumed to be ordered by descending relevance. For a document at rank
   * r in the list, the contribution is {@code 1 / (k + r)} where {@code k} is read from {@link
   * CombinerParams#COMBINER_RRF_K} or falls back to {@code this.k}. Contributions for the same
   * document ID across multiple queries (if found) are summed, and documents are returned sorted by
   * the fused score (descending).
   *
   * @param queriesDocMap per-query ranked results;
   * @param solrParams parameters; optional {@link CombinerParams#COMBINER_RRF_K} overrides k.
   * @return one {@link ShardDoc} per unique document ID, ordered by fused score.
   */
  @Override
  public List<ShardDoc> combine(Map<String, List<ShardDoc>> queriesDocMap, SolrParams solrParams) {
    int kVal = solrParams.getInt(CombinerParams.COMBINER_RRF_K, this.k);
    HashMap<String, Float> docIdToScore = new HashMap<>();
    Map<String, ShardDoc> docIdToShardDoc = new HashMap<>();
    List<ShardDoc> finalShardDocList = new ArrayList<>();
    for (Map.Entry<String, List<ShardDoc>> shardDocEntry : queriesDocMap.entrySet()) {
      List<ShardDoc> shardDocList = shardDocEntry.getValue();
      int ranking = 1;
      while (ranking <= shardDocList.size()) {
        String docId = shardDocList.get(ranking - 1).id.toString();
        docIdToShardDoc.put(docId, shardDocList.get(ranking - 1));
        float rrfScore = 1f / (kVal + ranking);
        docIdToScore.compute(docId, (id, score) -> (score == null) ? rrfScore : score + rrfScore);
        ranking++;
      }
    }
    List<Map.Entry<String, Float>> sortedByScoreDescending =
        docIdToScore.entrySet().stream()
            .sorted(
                Comparator.comparing(Map.Entry<String, Float>::getValue, Comparator.reverseOrder())
                    .thenComparing(Map.Entry::getKey))
            .toList();
    for (Map.Entry<String, Float> scoredDoc : sortedByScoreDescending) {
      String docId = scoredDoc.getKey();
      Float score = scoredDoc.getValue();
      ShardDoc shardDoc = docIdToShardDoc.get(docId);
      shardDoc.score = score;
      finalShardDocList.add(shardDoc);
    }
    return finalShardDocList;
  }

  private Map<Integer, Integer[]> combineResults(
      QueryResult combinedRankedList,
      List<QueryResult> queryResults,
      boolean saveRankPositionsForExplain,
      int kVal) {
    Map<Integer, Integer[]> docIdToRanks = null;
    DocSet combinedDocSet = null;
    HashMap<Integer, Float> docIdToScore = new HashMap<>();
    List<DocList> docLists = new ArrayList<>();
    long totalMatches = 0;
    for (QueryResult queryResult : queryResults) {
      DocIterator docs = queryResult.getDocList().iterator();
      totalMatches = Math.max(totalMatches, queryResult.getDocList().matches());
      int ranking = 1;
      while (docs.hasNext()) {
        int docId = docs.nextDoc();
        float rrfScore = 1f / (kVal + ranking);
        docIdToScore.compute(docId, (id, score) -> (score == null) ? rrfScore : score + rrfScore);
        ranking++;
      }
      docLists.add(queryResult.getDocList());
      if (combinedDocSet == null) {
        combinedDocSet = queryResult.getDocSet();
      } else if (queryResult.getDocSet() != null) {
        combinedDocSet = combinedDocSet.union(queryResult.getDocSet());
      }
    }
    List<Map.Entry<Integer, Float>> sortedByScoreDescending =
        docIdToScore.entrySet().stream()
            .sorted(
                Comparator.comparing(Map.Entry<Integer, Float>::getValue, Comparator.reverseOrder())
                    .thenComparing(Map.Entry::getKey))
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

    if (saveRankPositionsForExplain) {
      docIdToRanks = getRanks(docLists, combinedResultsDocIds);
    }

    DocSlice combinedResultSlice =
        new DocSlice(
            0,
            combinedResultsLength,
            combinedResultsDocIds,
            combinedResultScores,
            Math.max(combinedResultsLength, totalMatches),
            combinedResultScores.length > 0 ? combinedResultScores[0] : 0,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
    combinedRankedList.setDocList(combinedResultSlice);
    combinedRankedList.setDocSet(combinedDocSet);
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
      while (iterator.hasNext()) {
        int docId = iterator.nextDoc();
        docIdToRanks.get(docId)[j] = rank;
        rank++;
      }
    }
    return docIdToRanks;
  }

  @Override
  public SimpleOrderedMap<Explanation> getExplanations(
      String[] queryKeys,
      List<Query> queries,
      List<QueryResult> queryResult,
      SolrIndexSearcher searcher,
      IndexSchema schema,
      SolrParams solrParams)
      throws IOException {
    int kVal = solrParams.getInt(CombinerParams.COMBINER_RRF_K, this.k);
    SimpleOrderedMap<Explanation> docIdsExplanations = new SimpleOrderedMap<>();
    QueryResult combinedRankedList = new QueryResult();
    Map<Integer, Integer[]> docIdToRanks =
        combineResults(combinedRankedList, queryResult, true, kVal);
    DocList combinedDocList = combinedRankedList.getDocList();

    DocIterator iterator = combinedDocList.iterator();
    SolrDocumentFetcher docFetcher = searcher.getDocFetcher();
    for (int i = 0; i < combinedDocList.size(); i++) {
      int docId = iterator.nextDoc();
      Integer[] rankPerQuery = docIdToRanks.get(docId);
      Document doc = docFetcher.doc(docId);
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
              getReciprocalRankFusionExplain(queryKeys, rankPerQuery, kVal),
              originalExplanations);
      docIdsExplanations.add(docUniqueKey, fullDocIdExplanation);
    }
    return docIdsExplanations;
  }

  private String getReciprocalRankFusionExplain(
      String[] queryKeys, Integer[] rankPerQuery, int kVal) {
    StringBuilder reciprocalRankFusionExplain = new StringBuilder();
    StringJoiner scoreComponents = new StringJoiner(" + ");
    for (Integer rank : rankPerQuery) {
      if (rank != null) {
        scoreComponents.add("1/(" + kVal + "+" + rank + ")");
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
