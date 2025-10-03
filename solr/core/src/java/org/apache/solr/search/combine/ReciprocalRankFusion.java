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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.lucene.search.Explanation;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.handler.component.ShardDoc;

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

  private Map<String, Integer[]> getRanks(
      Collection<List<ShardDoc>> shardDocs, List<ShardDoc> combinedShardDocs) {
    Map<String, Integer[]> docIdToRanks;
    docIdToRanks = new HashMap<>();
    for (ShardDoc shardDoc : combinedShardDocs) {
      docIdToRanks.put(shardDoc.id.toString(), new Integer[shardDocs.size()]);
    }
    int docIdx = 0;
    for (List<ShardDoc> shardDocList : shardDocs) {
      int rank = 1;
      for (ShardDoc shardDoc : shardDocList) {
        String docId = shardDoc.id.toString();
        docIdToRanks.get(docId)[docIdx] = rank;
        rank++;
      }
      docIdx++;
    }
    return docIdToRanks;
  }

  @Override
  public SimpleOrderedMap<Explanation> getExplanations(
      String[] queryKeys,
      Map<String, List<ShardDoc>> queriesDocMap,
      List<ShardDoc> combinedQueriesDocs,
      SolrParams solrParams) {
    int kVal = solrParams.getInt(CombinerParams.COMBINER_RRF_K, this.k);
    SimpleOrderedMap<Explanation> docIdsExplanations = new SimpleOrderedMap<>();
    Map<String, Integer[]> docIdToRanks = getRanks(queriesDocMap.values(), combinedQueriesDocs);
    for (ShardDoc shardDoc : combinedQueriesDocs) {
      String docId = shardDoc.id.toString();
      Integer[] rankPerQuery = docIdToRanks.get(docId);
      Explanation fullDocIdExplanation =
          Explanation.match(
              shardDoc.score, getReciprocalRankFusionExplain(queryKeys, rankPerQuery, kVal));
      docIdsExplanations.add(docId, fullDocIdExplanation);
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
