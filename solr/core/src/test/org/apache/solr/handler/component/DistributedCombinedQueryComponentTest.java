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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.solr.BaseDistributedSearchTestCase;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * The DistributedCombinedQueryComponentTest class is a JUnit test suite that evaluates the
 * functionality of the CombinedQueryComponent in a Solr distributed search environment. It focuses
 * on testing the integration of lexical and vector queries using the combiner component.
 */
public class DistributedCombinedQueryComponentTest extends BaseDistributedSearchTestCase {

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
    fixShardCount(2);
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
    clients.sort(
        (client1, client2) -> {
          try {
            if (client2 instanceof HttpSolrClient httpClient2
                && client1 instanceof HttpSolrClient httpClient1)
              return new URI(httpClient1.getBaseURL()).getPort()
                  - new URI(httpClient2.getBaseURL()).getPort();
          } catch (URISyntaxException e) {
            throw new RuntimeException("Unable to get URI from SolrClient", e);
          }
          return 0;
        });
    for (SolrInputDocument doc : docs) {
      indexDoc(doc);
    }
    commit();
  }

  /**
   * Tests a single lexical query against the Solr server.
   *
   * @throws Exception if any exception occurs during the test execution
   */
  @Test
  public void testSingleLexicalQuery() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp;
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical1\":{\"lucene\":{\"query\":\"id:2^=10\"}}},"
                    + "\"limit\":5,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertEquals(1, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2");
  }

  @Override
  protected String getShardsString() {
    if (deadServers == null) return shards;
    Arrays.sort(shardsArr);
    StringBuilder sb = new StringBuilder();
    for (String shard : shardsArr) {
      if (!sb.isEmpty()) sb.append(',');
      sb.append(shard);
    }
    return sb.toString();
  }

  /**
   * Tests multiple lexical queries using the Solr server.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testMultipleLexicalQuery() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp;
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
                    + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                    + "\"limit\":5,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2", "1", "4", "5", "8");
  }

  /**
   * Test multiple query execution with sort.
   *
   * @throws Exception the exception
   */
  @Test
  public void testMultipleQueryWithSort() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp;
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical1\":{\"lucene\":{\"query\":\"title:title test for doc 1\"}},"
                    + "\"lexical2\":{\"lucene\":{\"query\":\"text:test text for doc 2\"}}},"
                    + "\"limit\":5,\"sort\":\"mod3_idv desc\""
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2", "8", "5", "4", "1");
  }

  /**
   * Tests the hybrid query functionality of the system with various setting of pagination.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testHybridQueryWithPagination() throws Exception {
    prepareIndexDocs();
    // lexical => 2,3
    // vector => 1,4,2,10,3,6
    QueryResponse rsp;
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertFieldValues(rsp.getResults(), id, "2", "3", "4", "1", "6", "10", "8", "7", "5");
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":4,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertFieldValues(rsp.getResults(), id, "2", "3", "4", "1");
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 5, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":4,\"offset\":3,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical\",\"vector\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertEquals(4, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "1", "6", "10", "8");
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
    QueryResponse rsp;
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"vector\":{\"knn\":{ \"f\": \"vector\", \"topK\": 4, \"query\": \"[1.0, 2.0, 3.0, 4.0]\"}}},"
                    + "\"limit\":2,\"offset\":1"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"facet\":true,\"facet.field\":\"mod3_idv\","
                    + "\"combiner.query\":[\"vector\"]}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertEquals(2, rsp.getResults().size());
    assertEquals(8, rsp.getResults().getNumFound());
    assertEquals("[1 (4), 0 (2), 2 (2)]", rsp.getFacetFields().getFirst().getValues().toString());
  }

  /**
   * Tests the combined query feature with faceting and highlighting.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testQueriesWithFacetAndHighlights() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp;
    rsp =
        queryServer(
            createParams(
                CommonParams.JSON,
                "{\"queries\":"
                    + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^=2 OR 3^=1)\"}},"
                    + "\"lexical2\":{\"lucene\":{\"query\":\"id:(4^=2 OR 5^=1)\"}}},"
                    + "\"limit\":3,"
                    + "\"fields\":[\"id\",\"score\",\"title\"],"
                    + "\"params\":{\"combiner\":true,\"facet\":true,\"facet.field\":\"mod3_idv\","
                    + "\"combiner.query\":[\"lexical1\",\"lexical2\"], \"hl\": true, \"hl.fl\": \"title\",\"hl.q\":\"test doc\"}}",
                "shards",
                getShardsString(),
                CommonParams.QT,
                "/search"));
    assertEquals(3, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2", "3", "4");
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
}
