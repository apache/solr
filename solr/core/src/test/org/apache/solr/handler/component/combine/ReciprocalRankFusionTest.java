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
package org.apache.solr.handler.component.combine;

import static org.apache.solr.common.params.CombinerParams.RECIPROCAL_RANK_FUSION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.component.ShardDoc;
import org.apache.solr.search.QueryResult;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The ReciprocalRankFusionTest class is a unit test suite for the {@link ReciprocalRankFusion}
 * class. It verifies the correctness of the fusion algorithm and its supporting methods.
 */
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

  /** Tests the functionality of combining the QueryResults across local search indices. */
  @Test
  public void testSimpleCombine() {
    List<QueryResult> rankedList = QueryAndResponseCombinerTest.getQueryResults();
    QueryResult result = QueryAndResponseCombiner.simpleCombine(rankedList);
    assertEquals(3, result.getDocList().size());
    assertEquals(4, result.getDocSet().size());
  }

  /** Test combine docs per queries using RRF. */
  @Test
  public void testQueryListCombine() {
    Map<String, List<ShardDoc>> queriesDocMap = new HashMap<>();
    ShardDoc shardDoc = new ShardDoc();
    shardDoc.id = "id1";
    shardDoc.shard = "shard1";
    shardDoc.orderInShard = 1;
    List<ShardDoc> shardDocList = new ArrayList<>();
    shardDocList.add(shardDoc);
    shardDoc = new ShardDoc();
    shardDoc.id = "id2";
    shardDoc.shard = "shard2";
    shardDoc.orderInShard = 2;
    shardDocList.add(shardDoc);
    queriesDocMap.put(shardDoc.shard, shardDocList);

    shardDoc = new ShardDoc();
    shardDoc.id = "id2";
    shardDoc.shard = "shard1";
    shardDoc.orderInShard = 1;
    queriesDocMap.put(shardDoc.shard, List.of(shardDoc));
    SolrParams solrParams = params();
    assertEquals(20, reciprocalRankFusion.getK());
    List<ShardDoc> shardDocs = reciprocalRankFusion.combine(queriesDocMap, solrParams);
    assertEquals(2, shardDocs.size());
    assertEquals("id2", shardDocs.get(0).id);
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
