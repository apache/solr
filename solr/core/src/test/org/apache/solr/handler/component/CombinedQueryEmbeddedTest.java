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
package org.apache.solr.handler.component;

import java.util.ArrayList;
import java.util.List;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Combined queries on embedded Solr, which has no HTTP layer to distribute a request through: the
 * core fuses the sub-queries' lists itself.
 */
public class CombinedQueryEmbeddedTest extends SolrTestCaseJ4 {

  private static final float DELTA = 1e-6f;

  // ranks: 2, 6, 3, 5
  private static final String LEXICAL1 = "id:(2^=4 OR 3^=2 OR 6^=3 OR 5^=1)";
  // ranks: 7, 5, 10, 4
  private static final String LEXICAL2 = "id:(4^=1 OR 5^=3 OR 7^=4 OR 10^=2)";
  // the two fused by reciprocal rank, k = 60
  private static final List<String> FUSED = List.of("5", "7", "2", "6", "3", "10", "4");

  @ClassRule
  public static final EmbeddedSolrServerTestRule solrTestRule = new EmbeddedSolrServerTestRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    solrTestRule.startSolr();
    CombinedQueryTestCore.create(solrTestRule, random());
    SolrClient client = solrTestRule.getSolrClient();
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(i));
      doc.addField("title", "title test for doc " + i);
      doc.addField("mod3_idv", i % 3);
      doc.addField("color_sdv", i % 2 == 0 ? "red" : "blue");
      docs.add(doc);
    }
    client.add(docs);
    client.commit();
  }

  private static ModifiableSolrParams combined(String... extra) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("combiner", "true");
    params.set("combiner.query", "lexical1", "lexical2");
    params.set("lexical1", LEXICAL1);
    params.set("lexical2", LEXICAL2);
    params.set("fl", "id,score");
    params.set("rows", "10");
    for (int i = 0; i < extra.length; i += 2) {
      params.set(extra[i], extra[i + 1]);
    }
    return params;
  }

  private static QueryResponse search(ModifiableSolrParams params) throws Exception {
    QueryRequest request = new QueryRequest(params);
    request.setPath("/search");
    return request.process(solrTestRule.getSolrClient());
  }

  private static List<String> ids(SolrDocumentList docs) {
    List<String> ids = new ArrayList<>();
    for (SolrDocument doc : docs) {
      ids.add((String) doc.getFieldValue("id"));
    }
    return ids;
  }

  private static float rrf(int... ranks) {
    float score = 0;
    for (int rank : ranks) {
      score += 1f / (60 + rank);
    }
    return score;
  }

  @Test
  public void testFusesByReciprocalRank() throws Exception {
    SolrDocumentList docs = search(combined()).getResults();
    // 5 is in both lists; 2 and 7 tie at rank 1, as 10 and 3 tie at rank 3: a tie goes to the
    // greater id, as on a distributed search
    assertEquals(FUSED, ids(docs));
    assertEquals(7, docs.getNumFound());
    assertEquals(Boolean.TRUE, docs.getNumFoundExact());
    assertEquals(rrf(4, 2), (Float) docs.get(0).getFieldValue("score"), DELTA);
    assertEquals(rrf(1), (Float) docs.get(1).getFieldValue("score"), DELTA);
    assertEquals(rrf(4), (Float) docs.get(6).getFieldValue("score"), DELTA);
    assertEquals(rrf(4, 2), docs.getMaxScore(), DELTA);
  }

  @Test
  public void testWeightedReciprocalRank() throws Exception {
    SolrDocumentList docs = search(combined("combiner.rrf.weight.lexical2", "2")).getResults();
    assertEquals(List.of("5", "7", "10", "4", "2", "6", "3"), ids(docs));
    assertEquals(1f / 64 + 2f / 62, (Float) docs.get(0).getFieldValue("score"), DELTA);
  }

  @Test
  public void testRrfK() throws Exception {
    SolrDocumentList docs = search(combined("combiner.rrf.k", "0")).getResults();
    // with k=0 rank 1 alone (1/1) beats ranks 4 and 2 together (1/4 + 1/2)
    assertEquals(List.of("7", "2", "5", "6", "3", "10", "4"), ids(docs));
  }

  @Test
  public void testPagesTheFusedList() throws Exception {
    SolrDocumentList docs = search(combined("start", "2", "rows", "3")).getResults();
    assertEquals(List.of("2", "6", "3"), ids(docs));
    assertEquals(2, docs.getStart());
    assertEquals(7, docs.getNumFound());
    assertEquals(rrf(1), docs.getMaxScore(), DELTA);

    docs = search(combined("start", "6", "rows", "3")).getResults();
    assertEquals(List.of("4"), ids(docs));
    docs = search(combined("start", "20", "rows", "3")).getResults();
    assertEquals(0, docs.size());
    assertEquals(20, docs.getStart());
    assertEquals(7, docs.getNumFound());
  }

  @Test
  public void testFusesOnlyTheFirstStartPlusRowsOfEachQuery() throws Exception {
    // each list is cut to its first two: 2, 6 and 7, 5; numFound is then the larger query's 4
    SolrDocumentList docs = search(combined("rows", "2")).getResults();
    assertEquals(List.of("7", "2"), ids(docs));
    assertEquals(4, docs.getNumFound());
    assertEquals(Boolean.FALSE, docs.getNumFoundExact());
    // shards.rows widens each list, as it widens a shard's
    docs = search(combined("rows", "2", "shards.rows", "10")).getResults();
    assertEquals(List.of("5", "7"), ids(docs));
    assertEquals(Boolean.TRUE, docs.getNumFoundExact());
  }

  @Test
  public void testSortsTheFusedDocuments() throws Exception {
    SolrDocumentList docs = search(combined("sort", "mod3_idv asc, id desc")).getResults();
    // mod3: 3,6 -> 0; 4,7,10 -> 1; 2,5 -> 2
    assertEquals(List.of("6", "3", "7", "4", "10", "5", "2"), ids(docs));
    docs = search(combined("sort", "mod3_idv desc, score desc")).getResults();
    assertEquals(List.of("5", "2", "7", "10", "4", "6", "3"), ids(docs));
  }

  @Test
  public void testFacetsCountTheUnion() throws Exception {
    QueryResponse rsp = search(combined("rows", "1", "facet", "true", "facet.field", "color_sdv"));
    assertEquals(1, rsp.getResults().size());
    // 2, 4, 6, 10 red; 3, 5, 7 blue
    assertEquals("[red (4), blue (3)]", rsp.getFacetField("color_sdv").getValues().toString());
  }

  @Test
  public void testCollapsesAcrossQueries() throws Exception {
    SolrDocumentList docs =
        search(combined("fq", "{!collapse field=mod3_idv}", "fl", "id,mod3_idv")).getResults();
    List<Object> groups = new ArrayList<>();
    for (SolrDocument doc : docs) {
      groups.add(doc.getFieldValue("mod3_idv"));
    }
    assertEquals(3, docs.size());
    assertEquals(3, groups.stream().distinct().count());
  }

  @Test
  public void testExplainsTheFusion() throws Exception {
    QueryResponse rsp = search(combined("debug", "results"));
    NamedList<?> explanations = (NamedList<?>) rsp.getDebugMap().get("combinerExplanations");
    assertNotNull(explanations);
    assertEquals(7, explanations.size());
    assertTrue(
        explanations.get("5").toString(),
        explanations.get("5").toString().contains("4 for query(lexical1), 2 for query(lexical2)"));
  }

  @Test
  public void testJsonRequest() throws Exception {
    JsonQueryRequest request =
        new JsonQueryRequest()
            .withParam("combiner", true)
            .withParam("combiner.query", List.of("lexical1", "lexical2"))
            .returnFields("id", "score")
            .setLimit(10);
    request.setPath("/search");
    request.withParam("lexical1", LEXICAL1).withParam("lexical2", LEXICAL2);
    // JsonQueryRequest cannot set "queries"; the same sub-queries as params
    QueryResponse rsp = request.process(solrTestRule.getSolrClient());
    assertEquals(FUSED, ids(rsp.getResults()));
  }

  @Test
  public void testDistribFalseFusesToo() throws Exception {
    assertEquals(FUSED, ids(search(combined("distrib", "false")).getResults()));
  }
}
