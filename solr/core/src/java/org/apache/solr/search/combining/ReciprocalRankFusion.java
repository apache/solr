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

import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.search.TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;

/**
 * Reciprocal Rank Fusion (RRF) is an algorithm that takes in input multiple
 * ranked results to produce a unified result set.
 * Examples of use cases where RRF can be used include hybrid search and multiple Knn vector queries executed concurrently.
 * RRF is based on the concept of reciprocal rank, which is the inverse of the rank position of a document
 * in a ranked list of search results.
 * The combination of search results happens taking into account the position of the items in the original rankings,
 * and giving higher score to items that are ranked higher in multiple lists.
 * RRF was introduced the first time by Cormack et al. in [1].<br>
 *
 * <p>[1] Cormack, Gordon V. et al. “Reciprocal rank fusion outperforms condorcet and individual rank learning methods.”
 * Proceedings of the 32nd international ACM SIGIR conference on Research and development in information retrieval (2009)<br>
 */
public class ReciprocalRankFusion extends Combiner {
    private int k;

    public ReciprocalRankFusion(SolrParams requestParams) {
        super(requestParams);
        this.k = requestParams.getInt(
                CombinerParams.COMBINER_RRF_K, CombinerParams.COMBINER_RRF_K_DEFAULT);
    }

    @Override
    public QueryResult combine(QueryResult[] queryResults) {
        QueryResult rrfResult = initCombinedResult(queryResults);

        HashMap<Integer, Float> docIdToScore = new HashMap<>();

        for (QueryResult singleQuery : queryResults) {
            DocList docList = singleQuery.getDocList();
            DocIterator iterator = docList.iterator();
            int ranking = 0;
            while (iterator.hasNext() && ranking < upTo) {
                ranking++;
                int docId = iterator.nextDoc();
                float rrfScore = 1f / (k + ranking);
                docIdToScore.compute(docId, (id, score) -> (score == null) ? rrfScore: score + rrfScore);
            }
        }
        Stream<Map.Entry<Integer, Float>> sorted = docIdToScore.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()));

        int combinedResultsLength = docIdToScore.size();
        int[] combinedResultsDocIDs = new int[combinedResultsLength];
        float[] combinedResultScores = new float[combinedResultsLength];

        int i=0;
        for (Map.Entry<Integer, Float> scoredDoc : sorted.collect(Collectors.toList())) {
            combinedResultsDocIDs[i] = scoredDoc.getKey();
            combinedResultScores[i] = scoredDoc.getValue();
            i++;
        }

        DocSlice combinedResultSlice = new DocSlice(0, combinedResultsLength, combinedResultsDocIDs, combinedResultScores, combinedResultsLength, combinedResultScores[0], GREATER_THAN_OR_EQUAL_TO);
        rrfResult.setDocList(combinedResultSlice);
        return rrfResult;
    }
}
