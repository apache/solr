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
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The CombinedQueryComponentTest class is an integration test suite for the CombinedQueryComponent
 * in Solr. It verifies the functionality of the component by performing various queries and
 * validating the responses.
 */
public class CombinedQueryComponentTest extends SolrTestCaseJ4 {

  private static final int NUM_DOCS = 10;
  private static final String vectorField = "vector";

  /**
   * Sets up the test class by initializing the core and adding test documents to the index. This
   * method prepares the Solr index with a set of documents for subsequent test cases.
   *
   * @throws Exception if any error occurs during setup, such as initialization failures or indexing
   *     issues.
   */
  @BeforeClass
  public static void setUpClass() throws Exception {
    initCore("solrconfig-combined-query.xml", "schema-vector-catchall.xml");
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 1; i <= NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(i));
      doc.addField("text", "test text for doc " + i);
      doc.addField("title", "title test for doc " + i);
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
    for (SolrInputDocument doc : docs) {
      assertU(adoc(doc));
    }
    assertU(commit());
  }

  /** Performs a single lexical query using the provided JSON request and verifies the response. */
  public void testSingleLexicalQuery() {
    assertQ(
        req(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 5\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\"]}}",
            CommonParams.QT,
            "/search"),
        "//result[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='5']");
  }

  /** Performs multiple lexical queries and verifies the results. */
  public void testMultipleLexicalQueryWithDebug() {
    assertQ(
        req(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"debugQuery\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
            CommonParams.QT,
            "/search"),
        "//result[@numFound='10']",
        "//result/doc[1]/str[@name='id'][.='1']",
        "//result/doc[2]/str[@name='id'][.='2']",
        "//lst[@name='debug']/lst[@name='combinerExplanations'][node()]");
  }

  /** Tests the functionality of a hybrid query that combines lexical and vector search. */
  public void testHybridQuery() {
    // lexical => 2,3
    // vector => 1,4,2,10,3
    assertQ(
        req(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"]}}",
            CommonParams.QT,
            "/search"),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='3']",
        "//result/doc[3]/str[@name='id'][.='1']");
  }

  /** Test no results in combined queries. */
  @Test
  public void testNoResults() {
    assertQ(
        req(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:Solr is the blazing-fast, open source search platform\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:Solr powers the search\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
            CommonParams.QT,
            "/search"),
        "//result[@numFound='0']");
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
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"vector\", \"lexical2\"]}}",
            CommonParams.QT,
            "/search"),
        SolrException.ErrorCode.BAD_REQUEST);
  }

  /**
   * Test to ensure the TestCombiner Algorithm is injected through solrconfigs and is being executed
   * when sent the command through SolrParams
   */
  @Test
  public void testCombinerPlugin() {
    assertQ(
        req(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.algorithm\":test,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
            CommonParams.QT,
            "/search"),
        "//result[@numFound='5']",
        "//result/doc[1]/str[@name='id'][.='2']",
        "//result/doc[2]/str[@name='id'][.='1']");
  }

  /**
   * Tests that using unsupported features with Combined Queries throws the expected exception.
   *
   * <p>This test case verifies that requests for Combined Queries that include either the
   * 'cursorMark' or 'group' parameters.
   */
  @Test
  public void testNonEnabledFeature() {
    String combinedQueryStr =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
            + "\"sort\":\"id asc\","
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.algorithm\":test,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}";
    RuntimeException exceptionThrown =
        expectThrows(
            RuntimeException.class,
            () ->
                assertQ(
                    req(
                        CommonParams.JSON,
                        combinedQueryStr,
                        CommonParams.QT,
                        "/search",
                        "cursorMark",
                        CURSOR_MARK_START)));
    assertTrue(
        exceptionThrown
            .getCause()
            .getMessage()
            .contains("Unsupported functionality for Combined Queries."));
    exceptionThrown =
        expectThrows(
            RuntimeException.class,
            () ->
                assertQ(
                    req(
                        CommonParams.JSON,
                        combinedQueryStr,
                        CommonParams.QT,
                        "/search",
                        "group",
                        "true")));
    assertTrue(
        exceptionThrown
            .getCause()
            .getMessage()
            .contains("Unsupported functionality for Combined Queries."));
  }
}
