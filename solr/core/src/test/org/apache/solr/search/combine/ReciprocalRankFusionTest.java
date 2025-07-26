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

import static org.apache.solr.common.params.CombinerParams.COMBINER_RRF_K;
import static org.apache.solr.common.params.CombinerParams.RECIPROCAL_RANK_FUSION;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.QueryResult;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The ReciprocalRankFusionTest class is a unit test suite for the {@link ReciprocalRankFusion}
 * class. It verifies the correctness of the fusion algorithm and its supporting methods.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ReciprocalRankFusionTest extends SolrTestCaseJ4 {

  public static ReciprocalRankFusion reciprocalRankFusion;

  /**
   * Initializes the test environment by setting up the {@link ReciprocalRankFusion} instance with
   * specific parameters.
   */
  @BeforeClass
  public static void beforeClass() {
    NamedList<?> args = new NamedList<>(Map.of("k", "20"));
    reciprocalRankFusion = new ReciprocalRankFusion();
    reciprocalRankFusion.init(args);
  }

  /** Tests the functionality of combining using RRF across local search indices. */
  @Test
  public void testSearcherCombine() {
    List<QueryResult> rankedList = getQueryResults();
    SolrParams solrParams = params(COMBINER_RRF_K, "10");
    QueryResult result = reciprocalRankFusion.combine(rankedList, solrParams);
    assertEquals(20, reciprocalRankFusion.getK());
    assertEquals(3, result.getDocList().size());
  }

  private static List<QueryResult> getQueryResults() {
    QueryResult r1 = new QueryResult();
    r1.setDocList(
        new DocSlice(
            0,
            2,
            new int[] {1, 2},
            new float[] {0.67f, 0, 62f},
            3,
            0.67f,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    QueryResult r2 = new QueryResult();
    r2.setDocList(
        new DocSlice(
            0,
            1,
            new int[] {0},
            new float[] {0.87f},
            2,
            0.87f,
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO));
    return List.of(r1, r2);
  }

  /** Test shard combine for RRF. */
  @Test
  public void testShardCombine() {
    Map<String, List<ShardDoc>> shardDocMap = new HashMap<>();
    ShardDoc shardDoc = new ShardDoc();
    shardDoc.id = "id1";
    shardDoc.shard = "shard1";
    shardDoc.orderInShard = 1;
    List<ShardDoc> shardDocList = new ArrayList<>();
    shardDocList.add(shardDoc);
    shardDoc = new ShardDoc();
    shardDoc.id = "id2";
    shardDoc.shard = "shard1";
    shardDoc.orderInShard = 2;
    shardDocList.add(shardDoc);
    shardDocMap.put(shardDoc.shard, shardDocList);

    shardDoc = new ShardDoc();
    shardDoc.id = "id2";
    shardDoc.shard = "shard2";
    shardDoc.orderInShard = 1;
    shardDocMap.put(shardDoc.shard, List.of(shardDoc));
    SolrParams solrParams = params();
    List<ShardDoc> shardDocs = reciprocalRankFusion.combine(shardDocMap, solrParams);
    assertEquals(2, shardDocs.size());
    assertEquals("id2", shardDocs.getFirst().id);
  }

  @Test
  public void testImplementationFactory() {
    Map<String, QueryAndResponseCombiner> combinerMap = new HashMap<>(1);
    SolrParams emptySolrParms = params();
    String emptyParamAlgorithm =
        emptySolrParms.get(CombinerParams.COMBINER_ALGORITHM, CombinerParams.DEFAULT_COMBINER);
    assertThrows(
        SolrException.class,
        () -> QueryAndResponseCombiner.getImplementation(emptyParamAlgorithm, combinerMap));
    SolrParams solrParams = params(CombinerParams.COMBINER_ALGORITHM, RECIPROCAL_RANK_FUSION);
    String algorithm =
        solrParams.get(CombinerParams.COMBINER_ALGORITHM, CombinerParams.DEFAULT_COMBINER);
    combinerMap.put(RECIPROCAL_RANK_FUSION, new ReciprocalRankFusion());
    assertTrue(
        QueryAndResponseCombiner.getImplementation(algorithm, combinerMap)
            instanceof ReciprocalRankFusion);
  }
}
