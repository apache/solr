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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.Explanation;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CombinerParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
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
    QueryResult result = QueryAndResponseCombiner.simpleCombine(rankedList, null, null);
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
    assertEquals("id2", shardDocs.getFirst().id);
  }

  /**
   * Builds a two-list map: lexical=[A,B,C] and vector=[C,A,B], so ranks differ across lists and a
   * weight change can flip the fused order.
   */
  private Map<String, List<ShardDoc>> twoListDocMap() {
    Map<String, List<ShardDoc>> queriesDocMap = new HashMap<>();
    List<ShardDoc> lexical = new ArrayList<>();
    for (String id : List.of("A", "B", "C")) {
      ShardDoc d = new ShardDoc();
      d.id = id;
      d.shard = "shard1";
      lexical.add(d);
    }
    List<ShardDoc> vector = new ArrayList<>();
    for (String id : List.of("C", "A", "B")) {
      ShardDoc d = new ShardDoc();
      d.id = id;
      d.shard = "shard2";
      vector.add(d);
    }
    queriesDocMap.put("lexical", lexical);
    queriesDocMap.put("vector", vector);
    return queriesDocMap;
  }

  /** Without weights the fused order is A, C, B (k=20). */
  @Test
  public void testQueryListCombineNoWeights() {
    SolrParams solrParams = params();
    List<ShardDoc> shardDocs = reciprocalRankFusion.combine(twoListDocMap(), solrParams);
    assertEquals(3, shardDocs.size());
    assertEquals("A", shardDocs.get(0).id);
    assertEquals("C", shardDocs.get(1).id);
    assertEquals("B", shardDocs.get(2).id);
  }

  /** A higher vector weight flips the winner from A to C. */
  @Test
  public void testQueryListCombineWithWeights() {
    SolrParams solrParams =
        params(
            CombinerParams.COMBINER_RRF_WEIGHT + ".lexical", "1",
            CombinerParams.COMBINER_RRF_WEIGHT + ".vector", "3");
    List<ShardDoc> shardDocs = reciprocalRankFusion.combine(twoListDocMap(), solrParams);
    assertEquals(3, shardDocs.size());
    assertEquals("C", shardDocs.get(0).id);
    assertEquals("A", shardDocs.get(1).id);
    assertEquals("B", shardDocs.get(2).id);
  }

  /** Omitting one weight defaults it to 1, so only the supplied weight biases the list. */
  @Test
  public void testQueryListCombineOneWeightOmitted() {
    SolrParams solrParams = params(CombinerParams.COMBINER_RRF_WEIGHT + ".lexical", "3");
    List<ShardDoc> shardDocs = reciprocalRankFusion.combine(twoListDocMap(), solrParams);
    assertEquals(3, shardDocs.size());
    assertEquals("A", shardDocs.get(0).id);
  }

  /** A weight of 0, infinity, NaN or an unparsable value is rejected with a 400. */
  @Test
  public void testInvalidWeightThrows() {
    for (String bad : List.of("0", "-0.5", "Infinity", "-Infinity", "NaN", "abc")) {
      SolrParams solrParams = params(CombinerParams.COMBINER_RRF_WEIGHT + ".lexical", bad);
      assertThrows(
          SolrException.class, () -> reciprocalRankFusion.combine(twoListDocMap(), solrParams));
    }
  }

  /** A weight for a query name not in combiner.query is rejected with a 400. */
  @Test
  public void testUnknownWeightNameThrows() {
    SolrParams solrParams = params(CombinerParams.COMBINER_RRF_WEIGHT + ".unknown", "2");
    assertThrows(
        SolrException.class, () -> reciprocalRankFusion.combine(twoListDocMap(), solrParams));
  }

  /**
   * The explain string carries the weight and labels each rank with the correct query name, even
   * when the doc-map iteration order is the reverse of {@code combiner.query} order.
   */
  @Test
  public void testExplainWithWeights() {
    // vector is inserted first, so map order is the reverse of the request order below.
    // A HashMap can iterate in request order by chance and hide a rank/label mismatch.
    Map<String, List<ShardDoc>> source = twoListDocMap();
    Map<String, List<ShardDoc>> queriesDocMap = new LinkedHashMap<>();
    queriesDocMap.put("vector", source.get("vector"));
    queriesDocMap.put("lexical", source.get("lexical"));
    assertEquals(List.of("vector", "lexical"), List.copyOf(queriesDocMap.keySet()));
    SolrParams solrParams =
        params(
            CombinerParams.COMBINER_RRF_WEIGHT + ".lexical", "2",
            CombinerParams.COMBINER_RRF_WEIGHT + ".vector", "1");
    List<ShardDoc> combined = reciprocalRankFusion.combine(queriesDocMap, solrParams);
    SimpleOrderedMap<Explanation> explanations =
        reciprocalRankFusion.getExplanations(
            new String[] {"lexical", "vector"}, queriesDocMap, combined, solrParams);
    Explanation docA = explanations.get("A");
    // A is rank 1 in lexical (weight 2) and rank 2 in vector (weight 1)
    assertTrue(docA.getDescription().contains("2.0/(20+1)"));
    assertTrue(docA.getDescription().contains("1 for query(lexical)"));
    assertTrue(docA.getDescription().contains("2 for query(vector)"));
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
