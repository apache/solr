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
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.util.EmbeddedSolrServerTestRule;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Every combined-query feature on a single core: each request runs on embedded Solr, on standalone
 * Solr, and on standalone Solr distributed to that one core, and the three answers must agree. The
 * first two fuse on the core; the third fuses on the coordinator, as SolrCloud does.
 */
public class CombinedQuerySingleCoreTest extends SolrTestCaseJ4 {

  private static final float DELTA = 1e-6f;

  /** Sections of a response a combined query shapes, compared in full. */
  private static final List<String> SECTIONS =
      List.of("facet_counts", "facets", "highlighting", "stats");

  @ClassRule
  public static final EmbeddedSolrServerTestRule embedded = new EmbeddedSolrServerTestRule();

  @ClassRule public static final SolrJettyTestRule standalone = new SolrJettyTestRule();

  private static String shardUrl;

  @BeforeClass
  public static void beforeClass() throws Exception {
    System.setProperty(AllowListUrlChecker.ENABLE_URL_ALLOW_LIST, "false");
    System.setProperty(
        "solr.security.allow.paths", CombinedQueryTestCore.CONF.getParent().toString());
    embedded.startSolr();
    standalone.startSolr(createTempDir());
    long seed = random().nextLong();
    CombinedQueryTestCore.create(embedded, new Random(seed));
    CombinedQueryTestCore.create(standalone, new Random(seed));
    shardUrl = standalone.getBaseUrl() + "/" + DEFAULT_TEST_COLLECTION_NAME;
    for (SolrClient client : List.of(embedded.getSolrClient(), standalone.getSolrClient())) {
      List<SolrInputDocument> docs = new ArrayList<>();
      for (int i = 1; i <= 20; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", Integer.toString(i));
        doc.addField("title", "title test for doc " + i + (i % 4 == 0 ? " blue" : ""));
        doc.addField("text", "text " + (i % 2 == 0 ? "even" : "odd") + " doc " + i);
        doc.addField("mod3_idv", i % 3);
        doc.addField("mod3_sdv", "m" + (i % 3));
        doc.addField("color_sdv", i % 2 == 0 ? "red" : "blue");
        doc.addField("vector", List.of((float) i, 21f - i, 1f + i % 5, 1f + (i * 7) % 11));
        docs.add(doc);
        if (i == 10) {
          client.add(docs);
          client.commit();
          docs.clear();
        }
      }
      client.add(docs);
      client.commit();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // requests

  private static final String LEXICAL = "{\"lucene\": {\"query\": \"title:(test blue)\"}}";
  private static final String LEXICAL_EVEN = "{\"lucene\": {\"query\": \"text:(even doc)\"}}";
  private static final String VECTOR =
      "{\"knn\": {\"f\": \"vector\", \"topK\": 8, \"query\": \"[3.0, 17.0, 2.0, 9.0]\"}}";

  /** A JSON combined query of {@code lexical} and {@code vector}, plus extra JSON params. */
  private static ModifiableSolrParams hybrid(String extraParams) {
    return jsonRequest(
        String.format(
            Locale.ROOT,
            """
        {
          "queries": {"lexical": %s, "vector": %s},
          "limit": 10,
          "fields": ["id", "score", "title"],
          "params": {"combiner": true, "combiner.query": ["lexical", "vector"] %s}
        }""",
            LEXICAL,
            VECTOR,
            extraParams.isEmpty() ? "" : ", " + extraParams));
  }

  private static ModifiableSolrParams jsonRequest(String body) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.JSON, body);
    return params;
  }

  private static ModifiableSolrParams with(ModifiableSolrParams params, String... extra) {
    ModifiableSolrParams copy = new ModifiableSolrParams(params);
    for (int i = 0; i < extra.length; i += 2) {
      copy.set(extra[i], extra[i + 1]);
    }
    return copy;
  }

  // ---------------------------------------------------------------------------------------------
  // the comparison

  private static QueryResponse run(SolrClient client, String path, ModifiableSolrParams params)
      throws Exception {
    QueryRequest request = new QueryRequest(params);
    request.setPath(path);
    return request.process(client);
  }

  private static QueryResponse assertSame(ModifiableSolrParams params) throws Exception {
    return assertSame("/search", params);
  }

  /**
   * Runs the request on the three and asserts they agree; returns the embedded answer.
   *
   * @return the embedded core's response
   */
  private static QueryResponse assertSame(String path, ModifiableSolrParams params)
      throws Exception {
    QueryResponse inProcess = run(embedded.getSolrClient(), path, params);
    QueryResponse local = run(standalone.getSolrClient(), path, params);
    QueryResponse distributed =
        run(standalone.getSolrClient(), path, with(params, "shards", shardUrl));
    String context = path + " " + params;
    assertNull(context, inProcess.getResponse().get("response_per_query"));
    assertNull(context, local.getResponse().get("response_per_query"));
    assertSameResponse(context + "\n[embedded vs distributed]", distributed, inProcess);
    assertSameResponse(context + "\n[standalone vs distributed]", distributed, local);
    return inProcess;
  }

  private static void assertSameResponse(String context, QueryResponse expected, QueryResponse r) {
    SolrDocumentList e = expected.getResults();
    SolrDocumentList a = r.getResults();
    context += "\nexpected: " + e + "\nactual:   " + a;
    assertEquals(context, e.getNumFound(), a.getNumFound());
    assertEquals(context, e.getNumFoundExact(), a.getNumFoundExact());
    assertEquals(context, e.getStart(), a.getStart());
    assertEquals(context, e.size(), a.size());
    for (int i = 0; i < e.size(); i++) {
      assertSameDocument(context + "\nat " + i, e.get(i), a.get(i));
    }
    if (a.getMaxScore() != null) {
      assertEquals(context, e.getMaxScore(), a.getMaxScore(), DELTA);
    }
    for (String section : SECTIONS) {
      assertEquals(
          context + "\nsection " + section,
          String.valueOf(expected.getResponse().get(section)),
          String.valueOf(r.getResponse().get(section)));
    }
    assertSameExpanded(context, expected.getExpandedResults(), r.getExpandedResults());
    if (expected.getDebugMap() != null) {
      for (String key : List.of("combinerExplanations", "queryBoosting")) {
        assertEquals(
            context + "\ndebug " + key,
            String.valueOf(expected.getDebugMap().get(key)),
            String.valueOf(r.getDebugMap().get(key)));
      }
    }
  }

  private static void assertSameDocument(String context, SolrDocument e, SolrDocument a) {
    Map<String, Object> expected = new TreeMap<>(e);
    Map<String, Object> actual = new TreeMap<>(a);
    Object scoreE = expected.remove("score");
    Object scoreA = actual.remove("score");
    // written by each index on its own
    for (String perIndex : List.of("_version_", "timestamp")) {
      assertEquals(context, expected.containsKey(perIndex), actual.containsKey(perIndex));
      expected.remove(perIndex);
      actual.remove(perIndex);
    }
    assertEquals(context, expected.toString(), actual.toString());
    if (scoreE == null) {
      assertNull(context, scoreA);
    } else {
      assertEquals(context, (Float) scoreE, (Float) scoreA, DELTA);
    }
  }

  private static void assertSameExpanded(
      String context, Map<String, SolrDocumentList> e, Map<String, SolrDocumentList> a) {
    if (e == null) {
      assertNull(context, a);
      return;
    }
    assertEquals(context, new TreeMap<>(e).keySet(), new TreeMap<>(a).keySet());
    for (Map.Entry<String, SolrDocumentList> group : e.entrySet()) {
      SolrDocumentList docsE = group.getValue();
      SolrDocumentList docsA = a.get(group.getKey());
      assertEquals(context, docsE.getNumFound(), docsA.getNumFound());
      assertEquals(context, ids(docsE), ids(docsA));
    }
  }

  private static List<String> ids(SolrDocumentList docs) {
    List<String> ids = new ArrayList<>();
    for (SolrDocument doc : docs) {
      ids.add(String.valueOf(doc.getFieldValue("id")));
    }
    return ids;
  }

  /** The request fails alike everywhere: same status, message containing {@code expected}. */
  private static void assertSameError(ModifiableSolrParams params, String expected) {
    for (ModifiableSolrParams p : List.of(params, with(params, "shards", shardUrl))) {
      for (SolrClient client : List.of(embedded.getSolrClient(), standalone.getSolrClient())) {
        if (p.get("shards") != null && client != standalone.getSolrClient()) {
          continue;
        }
        SolrException e = expectThrows(SolrException.class, () -> run(client, "/search", p));
        assertEquals(p.toString(), 400, e.code());
        assertTrue(e.getMessage(), e.getMessage().contains(expected));
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // features

  @Test
  public void testHybridLexicalAndVector() throws Exception {
    QueryResponse rsp = assertSame(hybrid(""));
    assertEquals(10, rsp.getResults().size());
    // standalone fuses on the core, sending no shard request, not even to itself
    QueryResponse local =
        run(standalone.getSolrClient(), "/search", with(hybrid(""), "shards.info", "true"));
    assertNull(local.getResponse().get("shards.info"));
    QueryResponse distributed =
        run(
            standalone.getSolrClient(),
            "/search",
            with(hybrid(""), "shards.info", "true", "shards", shardUrl));
    assertNotNull(distributed.getResponse().get("shards.info"));
  }

  @Test
  public void testLexicalOnly() throws Exception {
    assertSame(
        jsonRequest(
            String.format(
                Locale.ROOT,
                """
            {
              "queries": {"a": %s, "b": %s},
              "limit": 10,
              "fields": ["id", "score"],
              "params": {"combiner": true, "combiner.query": ["a", "b"]}
            }""",
                LEXICAL,
                LEXICAL_EVEN)));
  }

  @Test
  public void testSingleQuery() throws Exception {
    assertSame(
        jsonRequest(
            String.format(
                Locale.ROOT,
                """
            {"queries": {"vector": %s}, "limit": 5, "fields": ["id", "score"],
             "params": {"combiner": true, "combiner.query": ["vector"]}}""",
                VECTOR)));
  }

  @Test
  public void testSubQueryParsers() throws Exception {
    assertSame(
        jsonRequest(
            """
            {
              "queries": {
                "edismax": {"edismax": {"query": "blue even", "qf": "title text", "mm": "1"}},
                "func": {"func": {"query": "mod3_idv"}}
              },
              "limit": 10,
              "fields": ["id", "score"],
              "params": {"combiner": true, "combiner.query": ["edismax", "func"]}
            }"""));
    // the classic parameter form, sub-queries given as plain parameters
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("combiner", "true");
    params.set("combiner.query", "q1", "q2");
    params.set("q1", "{!edismax qf=title}blue");
    params.set("q2", "{!knn f=vector topK=5}[3.0, 17.0, 2.0, 9.0]");
    params.set("fl", "id,score");
    assertSame(params);
    // a vector query that matches by similarity threshold rather than a top K
    params.set("q2", "{!vectorSimilarity f=vector minReturn=0.9}[3.0, 17.0, 2.0, 9.0]");
    assertSame(params);
  }

  @Test
  public void testWeightsAndK() throws Exception {
    assertSame(hybrid("\"combiner.rrf.weight.lexical\": 2.5"));
    assertSame(hybrid("\"combiner.rrf.weight.vector\": 0.5, \"combiner.rrf.k\": 5"));
    assertSame(hybrid("\"combiner.algorithm\": \"rrf\", \"combiner.rrf.k\": 0"));
  }

  @Test
  public void testInvalidWeights() {
    assertSameError(hybrid("\"combiner.rrf.weight.nope\": 2"), "RRF weight for unknown query");
    assertSameError(hybrid("\"combiner.rrf.weight.vector\": -1"), "must be a positive finite");
    assertSameError(hybrid("\"combiner.rrf.weight.vector\": 0"), "must be a positive finite");
  }

  @Test
  public void testCombinerPlugin() throws Exception {
    assertSame(hybrid("\"combiner.algorithm\": \"test\", \"debug\": \"results\""));
    assertSameError(hybrid("\"combiner.algorithm\": \"nope\""), "Unknown Combining algorithm");
  }

  @Test
  public void testLimitsAndUnsupported() {
    assertSameError(
        jsonRequest(
            String.format(
                Locale.ROOT,
                """
            {"queries": {"a": %s, "b": %s, "c": %s}, "limit": 5,
             "params": {"combiner": true, "combiner.query": ["a", "b", "c"]}}""",
                LEXICAL,
                LEXICAL_EVEN,
                VECTOR)),
        "Too many queries to combine: limit is 2");
    assertSameError(with(hybrid(""), "group", "true", "group.field", "color_sdv"), "Unsupported");
    assertSameError(with(hybrid("\"sort\": \"id asc\""), "cursorMark", "*"), "Unsupported");
  }

  @Test
  public void testPagination() throws Exception {
    for (int offset = 0; offset <= 16; offset += 4) {
      assertSame(with(hybrid(""), "start", Integer.toString(offset), "rows", "4"));
    }
    assertSame(with(hybrid(""), "rows", "0"));
    assertSame(with(hybrid(""), "rows", "2"));
    assertSame(with(hybrid(""), "rows", "2", "shards.rows", "15"));
    assertSame(with(hybrid(""), "start", "3", "rows", "2", "shards.rows", "15"));
  }

  @Test
  public void testSort() throws Exception {
    assertSame(with(hybrid(""), "sort", "mod3_idv asc, id desc"));
    assertSame(with(hybrid(""), "sort", "mod3_sdv desc, score desc", "start", "3"));
    assertSame(with(hybrid(""), "sort", "color_sdv asc, score asc"));
    assertSame(with(hybrid(""), "sort", "score desc, id asc"));
  }

  @Test
  public void testFilters() throws Exception {
    assertSame(with(hybrid(""), "fq", "color_sdv:red"));
    assertSame(with(hybrid(""), "fq", "{!frange l=1}mod3_idv"));
  }

  @Test
  public void testFields() throws Exception {
    assertSame(with(hybrid(""), "fl", "id"));
    assertSame(with(hybrid(""), "fl", "id,title,mod3_idv,color_sdv"));
    assertSame(with(hybrid(""), "fl", "id,score,mod3_idv,plus:sum(mod3_idv,1)"));
    assertSame(with(hybrid(""), "fl", "*,score"));
  }

  @Test
  public void testFacets() throws Exception {
    assertSame(
        with(hybrid(""), "facet", "true", "facet.field", "color_sdv", "facet.mincount", "1"));
    assertSame(
        with(
            hybrid(""),
            "rows",
            "3",
            "facet",
            "true",
            "facet.field",
            "mod3_idv",
            "facet.query",
            "color_sdv:red"));
    assertSame(
        with(
            hybrid(""),
            "json.facet",
            "{colors:{type:terms,field:color_sdv,facet:{m:\"max(mod3_idv)\"}}}"));
  }

  @Test
  public void testStats() throws Exception {
    assertSame(with(hybrid(""), "stats", "true", "stats.field", "mod3_idv"));
  }

  @Test
  public void testHighlighting() throws Exception {
    assertSame(with(hybrid(""), "rows", "4", "hl", "true", "hl.fl", "title", "hl.q", "test blue"));
  }

  @Test
  public void testElevation() throws Exception {
    assertSame(
        "/search-elevate", with(hybrid(""), "elevateIds", "19,2", "rows", "5", "debug", "true"));
    assertSame(
        "/search-elevate",
        with(
            hybrid(""),
            "elevateIds",
            "19",
            "excludeIds",
            "4",
            "facet",
            "true",
            "facet.field",
            "color_sdv",
            "hl",
            "true",
            "hl.fl",
            "title",
            "hl.q",
            "test"));
    assertSame(
        "/search-elevate", with(hybrid(""), "elevateIds", "19,2", "enableElevation", "false"));
    // elevate.xml elevates doc 7 for "AAAA", matched against each sub-query's own text
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("combiner", "true");
    params.set("combiner.query", "q1", "q2");
    params.set("q1", "AAAA");
    params.set("q2", "{!knn f=vector topK=5}[3.0, 17.0, 2.0, 9.0]");
    params.set("fl", "id,score,[elevated]");
    QueryResponse rsp = assertSame("/search-elevate", params);
    assertEquals("7", rsp.getResults().getFirst().getFieldValue("id"));
    assertEquals(Boolean.TRUE, rsp.getResults().getFirst().getFieldValue("[elevated]"));
  }

  @Test
  public void testCollapseAndExpand() throws Exception {
    assertSame(with(hybrid(""), "fq", "{!collapse field=mod3_idv}"));
    assertSame(with(hybrid(""), "fq", "{!collapse field=mod3_sdv sort='id desc'}"));
    assertSame(
        with(
            hybrid(""),
            "fq",
            "{!collapse field=mod3_idv}",
            "expand",
            "true",
            "expand.rows",
            "3",
            "fl",
            "id,score,mod3_idv"));
  }

  @Test
  public void testDebug() throws Exception {
    QueryResponse rsp = assertSame(with(hybrid(""), "debug", "results"));
    // every fused document is explained, the page and beyond
    NamedList<?> explanations = (NamedList<?>) rsp.getDebugMap().get("combinerExplanations");
    assertTrue(explanations.toString(), explanations.size() >= rsp.getResults().size());
    for (SolrDocument doc : rsp.getResults()) {
      assertNotNull(doc.toString(), explanations.get(String.valueOf(doc.getFieldValue("id"))));
    }
    assertSame(with(hybrid(""), "debugQuery", "true"));
  }

  @Test
  public void testDistribFalse() throws Exception {
    // distrib=false on standalone, and embedded, still fuse on the core
    QueryResponse inProcess = run(embedded.getSolrClient(), "/search", hybrid(""));
    QueryResponse local =
        run(standalone.getSolrClient(), "/search", with(hybrid(""), "distrib", "false"));
    assertSameResponse("distrib=false", inProcess, local);
  }

  @Test
  public void testNoMatches() throws Exception {
    assertSame(
        jsonRequest(
            """
            {"queries": {"a": {"lucene": {"query": "title:nothing"}},
                         "b": {"lucene": {"query": "text:nowhere"}}},
             "limit": 5, "fields": ["id", "score"],
             "params": {"combiner": true, "combiner.query": ["a", "b"],
                        "facet": true, "facet.field": "color_sdv"}}"""));
  }
}
