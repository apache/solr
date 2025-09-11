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

import static org.apache.solr.common.params.CursorMarkParams.CURSOR_MARK_START;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The CombinedQueryComponentTest class is an integration test suite for the CombinedQueryComponent
 * in Solr. It verifies the functionality of the component by performing few basic queries in single
 * sharded mode and validating the responses including limitations and combiner plugin.
 */
public class CombinedQueryComponentTest extends BaseDistributedSearchTestCase {

  private static final int NUM_DOCS = 10;
  private static final String vectorField = "vector";

  public CombinedQueryComponentTest() {
    super();
    fixShardCount(1);
  }

  /**
   * Sets up the test class by initializing the core and setting system properties. This method is
   * executed before all test methods in the class.
   *
   * @throws Exception if any exception occurs during initialization
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    initCore("solrconfig-combined-query.xml", "schema-vector-catchall.xml");
    System.setProperty("validateAfterInactivity", "200");
    System.setProperty("solr.httpclient.retries", "0");
    System.setProperty("distribUpdateSoTimeout", "5000");
  }

  /**
   * Prepares Solr input documents for indexing, including adding sample data and vector fields.
   * This method populates the Solr index with test data, including text, title, and vector fields.
   * The vector fields are used to calculate cosine distance for testing purposes.
   *
   * @throws Exception if any error occurs during the indexing process.
   */
  private synchronized void prepareIndexDocs() throws Exception {
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 1; i <= NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(i));
      doc.addField("text", "test text for doc " + i);
      doc.addField("title", "title test for doc " + i);
      doc.addField("mod3_idv", (i % 3));
      docs.add(doc);
    }
    // cosine distance vector1= 1.0
    docs.get(0).addField(vectorField, Arrays.asList(1f, 2f, 3f, 4f));
    // cosine distance vector1= 0.998
    docs.get(1).addField(vectorField, Arrays.asList(1.5f, 2.5f, 3.5f, 4.5f));
    // cosine distance vector1= 0.992
    docs.get(2).addField(vectorField, Arrays.asList(7.5f, 15.5f, 17.5f, 22.5f));
    // cosine distance vector1= 0.999
    docs.get(3).addField(vectorField, Arrays.asList(1.4f, 2.4f, 3.4f, 4.4f));
    // cosine distance vector1= 0.862
    docs.get(4).addField(vectorField, Arrays.asList(30f, 22f, 35f, 20f));
    // cosine distance vector1= 0.756
    docs.get(5).addField(vectorField, Arrays.asList(40f, 1f, 1f, 200f));
    // cosine distance vector1= 0.970
    docs.get(6).addField(vectorField, Arrays.asList(5f, 10f, 20f, 40f));
    // cosine distance vector1= 0.515
    docs.get(7).addField(vectorField, Arrays.asList(120f, 60f, 30f, 15f));
    // cosine distance vector1= 0.554
    docs.get(8).addField(vectorField, Arrays.asList(200f, 50f, 100f, 25f));
    // cosine distance vector1= 0.997
    docs.get(9).addField(vectorField, Arrays.asList(1.8f, 2.5f, 3.7f, 4.9f));
    del("*:*");
    for (SolrInputDocument doc : docs) {
      indexDoc(doc);
    }
    commit();
  }

  /** Performs a single lexical query using the provided JSON request and verifies the response. */
  public void testSingleLexicalQuery() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 5\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\"]}}",
            CommonParams.QT,
            "/search");
    assertEquals(5, rsp.getResults().size());
  }

  /** Performs multiple lexical queries and verifies the results. */
  public void testMultipleLexicalQueryWithDebug() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"debug\":[\"results\"],\"combiner.query\":[\"lexical1\",\"lexical2\"],"
                + " \"combiner.method\": \"pre\", \"rid\": \"test-1\"}}",
            CommonParams.QT,
            "/search");
    assertEquals(10, rsp.getResults().getNumFound());
    assertTrue(rsp.getDebugMap().containsKey("combinerExplanations"));
  }

  /** Test no results in combined queries. */
  @Test
  public void testNoResults() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:Solr is the blazing-fast, open source search platform\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:Solr powers the search\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"], \"combiner.method\": \"pre\"}}",
            CommonParams.QT,
            "/search");
    assertEquals(0, rsp.getResults().size());
  }

  /** Test max combiner queries limit set from solrconfig to 2. */
  @Test
  public void testMaxQueriesLimit() {
    assertQEx(
        "Too many queries to combine: limit is 2",
        req(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"vector\", \"lexical2\"], \"combiner.method\": \"pre\"}}",
            CommonParams.QT,
            "/search"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  /**
   * Test to ensure the TestCombiner Algorithm is injected through solrconfigs and is being executed
   * when sent the command through SolrParams
   */
  @Test
  public void testCombinerPlugin() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.algorithm\":test,\"combiner.query\""
                + ":[\"lexical1\",\"lexical2\"], \"combiner.method\": \"pre\",\"debug\":[\"results\"]}}",
            CommonParams.QT,
            "/search");
    assertEquals(10, rsp.getResults().getNumFound());
    assertEquals(
        "org.apache.lucene.search.Explanation:30 = this is test combiner\n",
        ((SimpleOrderedMap<?>) rsp.getDebugMap().get("combinerExplanations"))
            .get("combinerDetails"));
  }

  /**
   * Tests that using unsupported features with Combined Queries throws the expected exception.
   *
   * <p>This test case verifies that requests for Combined Queries that include either the
   * 'cursorMark' or 'group' parameters.
   */
  @Test
  public void testNonEnabledFeature() throws Exception {
    prepareIndexDocs();
    String combinedQueryStr =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
            + "\"sort\":\"id asc\","
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.algorithm\":test,\"combiner.query\":[\"lexical1\",\"lexical2\"], \"combiner.method\": \"pre\"}}";

    RuntimeException exceptionThrown =
        expectThrows(
            SolrException.class,
            () ->
                query(
                    CommonParams.JSON,
                    combinedQueryStr,
                    CommonParams.QT,
                    "/search",
                    "cursorMark",
                    CURSOR_MARK_START));
    assertTrue(
        exceptionThrown.getMessage().contains("Unsupported functionality for Combined Queries."));
    exceptionThrown =
        expectThrows(
            SolrException.class,
            () ->
                query(
                    CommonParams.JSON,
                    combinedQueryStr,
                    CommonParams.QT,
                    "/search",
                    "group",
                    "true"));
    assertTrue(
        exceptionThrown.getMessage().contains("Unsupported functionality for Combined Queries."));
  }
}
