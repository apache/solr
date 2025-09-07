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
import java.util.Arrays;
import java.util.List;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The DistributedCombinedQueryComponentTest class is a JUnit test suite that evaluates the
 * functionality of the CombinedQueryComponent in a Solr non-distributed search environment. It
 * focuses on testing the integration of combined query using both methods - pre and post.
 */
public class NonDistributedCombinedQueryComponentTest extends BaseDistributedSearchTestCase {

  private static final int NUM_DOCS = 10;
  private static final String vectorField = "vector";

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
    controlClient.deleteByQuery("*:*");
    for (SolrInputDocument doc : docs) {
      controlClient.add(doc);
    }
    controlClient.commit();
  }

  /**
   * Tests a single lexical query against a controlled non-distributed solr client.
   *
   * @throws Exception if any exception occurs during the test execution
   */
  @Test
  public void testSingleLexicalQuery() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"id:2^=10\"}}},"
            + "\"limit\":5,"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\"], \"combiner.method\": \"%s\"}}";
    QueryResponse rsp;
    // Combiner Method: pre
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "pre"), CommonParams.QT, "/search"));
    assertEquals(1, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2");
    // Combiner Method: post
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "post"), CommonParams.QT, "/search"));
    assertEquals(1, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2");
  }

  /**
   * Tests multiple lexical queries using a controlled non-distributed solr client.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testMultipleLexicalQuery() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
            + "\"limit\":5,"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"], \"combiner.method\": \"%s\"}}";
    QueryResponse rsp;
    // Combiner Method: pre
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "pre"), CommonParams.QT, "/search"));
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "1", "2", "4", "5", "7");
    // Combiner Method: post
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "post"), CommonParams.QT, "/search"));
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "1", "2", "4", "3", "7");
  }

  /**
   * Test multiple query execution with sort.
   *
   * @throws Exception the exception
   */
  @Test
  public void testMultipleQueryWithSort() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
            + "\"limit\":5,\"sort\":\"mod3_idv desc\""
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"], \"combiner.method\": \"%s\"}}";
    QueryResponse rsp;
    // Combiner Method: pre
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "pre"), CommonParams.QT, "/search"));
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2", "5", "8", "1", "4");
    // Combiner Method: post
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "post"), CommonParams.QT, "/search"));
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "8", "5", "2", "4", "1");
  }

  /**
   * Tests the hybrid query functionality of the system with various setting of pagination using
   * combiner.method: pre.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testHybridQueryWithPaginationPre() throws Exception {
    prepareIndexDocs();
    // lexical => 2,3
    // vector => 1,4,2,10,3,6
    QueryResponse rsp;
    // Combiner Method: pre
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"], \"combiner.method\": \"pre\"}}",
                CommonParams.QT,
                "/search"));
    assertFieldValues(rsp.getResults(), id, "2", "3", "1", "4", "10");
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":4,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"], \"combiner.method\": \"pre\"}}",
                CommonParams.QT,
                "/search"));
    assertFieldValues(rsp.getResults(), id, "2", "1", "3", "4");
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":4,\"offset\":3,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"], \"combiner.method\": \"pre\"}}",
                CommonParams.QT,
                "/search"));
    assertEquals(2, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "4", "10");
  }

  /**
   * Tests the hybrid query functionality of the system with various setting of pagination using
   * combiner.method: post.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testHybridQueryWithPaginationPost() throws Exception {
    prepareIndexDocs();
    // lexical => 2,3
    // vector => 1,4,2,10,3,6
    QueryResponse rsp;
    // Combiner Method: post
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"], \"combiner.method\": \"post\"}}",
                CommonParams.QT,
                "/search"));
    assertFieldValues(rsp.getResults(), id, "2", "3", "1", "4", "10");
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":4,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"], \"combiner.method\": \"post\"}}",
                CommonParams.QT,
                "/search"));
    assertFieldValues(rsp.getResults(), id, "2", "1", "3", "4");
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":4,\"offset\":3,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"], \"combiner.method\": \"post\"}}",
                CommonParams.QT,
                "/search"));
    assertEquals(2, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "4", "10");
  }

  /**
   * Tests the vector query functionality with faceting by setting the k value asserting if top-K
   * influences the facets.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testVectorQueryWithFaceting() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 4, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
            + "\"limit\":2,\"offset\":1"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"facet\":true,\"facet.field\":\"mod3_idv\","
            + "\"combiner.query\":[\"vector\"], \"combiner.method\": \"%s\"}}";
    QueryResponse rsp;
    // Combiner Method: pre
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "pre"), CommonParams.QT, "/search"));
    assertEquals(2, rsp.getResults().size());
    assertEquals(4, rsp.getResults().getNumFound());
    assertEquals("[1 (3), 2 (1)]", rsp.getFacetFields().getFirst().getValues().toString());
    // Combiner Method: post
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "post"), CommonParams.QT, "/search"));
    assertEquals(2, rsp.getResults().size());
    assertEquals(4, rsp.getResults().getNumFound());
    assertEquals("[1 (3), 2 (1)]", rsp.getFacetFields().getFirst().getValues().toString());
  }

  /**
   * Tests the combined query feature with faceting and highlighting.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testQueriesWithFacetAndHighlights() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"id:(4^=2 OR 5^=1)\"}}},"
            + "\"limit\":3,"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"facet\":true,\"facet.field\":\"mod3_idv\","
            + "\"combiner.query\":[\"lexical1\",\"lexical2\"], \"hl\": true,"
            + " \"combiner.method\": \"%s\", \"hl.fl\": \"title\",\"hl.q\":\"test doc\"}}";
    QueryResponse rsp;
    // Combiner Method: pre
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "pre"), CommonParams.QT, "/search"));
    assertEquals(3, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2", "4", "3");
    assertEquals("mod3_idv", rsp.getFacetFields().getFirst().getName());
    assertEquals("[2 (2), 0 (1), 1 (1)]", rsp.getFacetFields().getFirst().getValues().toString());
    assertEquals(3, rsp.getHighlighting().size());
    assertEquals(
        "title <em>test</em> for <em>doc</em> 2",
        rsp.getHighlighting().get("2").get("title").getFirst());
    assertEquals(
        "title <em>test</em> for <em>doc</em> 4",
        rsp.getHighlighting().get("4").get("title").getFirst());
    // Combiner Method: post
    rsp =
        queryNonDistribControlClient(
            createParams(
                CommonParams.JSON, String.format(jsonQuery, "pre"), CommonParams.QT, "/search"));
    assertEquals(3, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2", "4", "3");
    assertEquals("mod3_idv", rsp.getFacetFields().getFirst().getName());
    assertEquals("[2 (2), 0 (1), 1 (1)]", rsp.getFacetFields().getFirst().getValues().toString());
    assertEquals(3, rsp.getHighlighting().size());
    assertEquals(
        "title <em>test</em> for <em>doc</em> 2",
        rsp.getHighlighting().get("2").get("title").getFirst());
    assertEquals(
        "title <em>test</em> for <em>doc</em> 4",
        rsp.getHighlighting().get("4").get("title").getFirst());
  }

  private QueryResponse queryNonDistribControlClient(ModifiableSolrParams solrParams)
      throws Exception {
    solrParams.set("distrib", "false");
    final QueryResponse controlRsp = controlClient.query(solrParams);
    validateControlData(controlRsp);
    return controlRsp;
  }
}
