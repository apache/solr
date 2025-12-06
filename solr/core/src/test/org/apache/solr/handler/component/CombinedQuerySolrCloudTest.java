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
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.Test;

/**
 * Contains integration tests for the combined query functionality in a SolrCloud environment. This
 * class extends AbstractFullDistribZkTestBase to leverage the distributed testing framework. The
 * tests cover various scenarios including single and multiple lexical queries, sorting, pagination,
 * faceting, and highlighting similar to {@link DistributedCombinedQueryComponentTest}
 */
public class CombinedQuerySolrCloudTest extends AbstractFullDistribZkTestBase {

  private static final int NUM_DOCS = 10;
  private static final String vectorField = "vector";

  public CombinedQuerySolrCloudTest() {
    super();
    sliceCount = 2;
    fixShardCount(2);
  }

  @Override
  protected String getCloudSchemaFile() {
    return "schema-vector-catchall.xml";
  }

  @Override
  protected String getCloudSolrConfig() {
    return "solrconfig-combined-query.xml";
  }

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

  @Test
  public void testSingleLexicalQuery() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"id:2^10\"}}},"
                + "\"limit\":5,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\"]}}",
            CommonParams.QT,
            "/search");
    assertEquals(1, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "2");
  }

  /**
   * Tests multiple lexical queries using the solr cloud client against control client.
   *
   * @throws Exception if any error occurs during the test execution
   */
  @Test
  public void testMultipleLexicalQuery() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"id:(8^1 OR 5^2 OR 7^3 OR 10^2)\"}}},"
            + "\"limit\":5,"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}";
    QueryResponse rsp = query(CommonParams.JSON, jsonQuery, CommonParams.QT, "/search");
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "5", "7", "2", "6", "3");
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
            + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"id:(8^1 OR 5^2 OR 7^3 OR 10^1)\"}}},"
            + "\"limit\":5,\"sort\":\"mod3_idv desc\""
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}";
    QueryResponse rsp = query(CommonParams.JSON, jsonQuery, CommonParams.QT, "/search");
    assertEquals(5, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "8", "5", "2", "7", "10");
  }

  /**
   * Tests the hybrid query functionality of the system with various setting of pagination.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testHybridQueryWithPagination() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"id:(8^1 OR 5^2 OR 7^3 OR 10^2)\"}}},"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
            CommonParams.QT,
            "/search");
    assertFieldValues(rsp.getResults(), id, "5", "7", "2", "6", "3", "10", "8");
    rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"id:(8^1 OR 5^2 OR 7^3 OR 10^2)\"}}},"
                + "\"limit\":4,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
            CommonParams.QT,
            "/search");
    assertFieldValues(rsp.getResults(), id, "5", "7", "2", "6");
    rsp =
        query(
            CommonParams.JSON,
            "{\"queries\":"
                + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}},"
                + "\"lexical2\":{\"lucene\":{\"query\":\"id:(8^1 OR 5^2 OR 7^3 OR 10^2)\"}}},"
                + "\"limit\":4,\"offset\":3,"
                + "\"fields\":[\"id\",\"score\",\"title\"],"
                + "\"params\":{\"combiner\":true,\"combiner.query\":[\"lexical1\",\"lexical2\"]}}",
            CommonParams.QT,
            "/search");
    assertEquals(4, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "6", "3", "10", "8");
  }

  /**
   * Tests the single query functionality with faceting only.
   *
   * @throws Exception if any unexpected error occurs during the test execution.
   */
  @Test
  public void testQueryWithFaceting() throws Exception {
    prepareIndexDocs();
    String jsonQuery =
        "{\"queries\":"
            + "{\"lexical\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}}},"
            + "\"limit\":3,\"offset\":1"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"facet\":true,\"facet.field\":\"mod3_idv\",\"facet.mincount\":1,"
            + "\"combiner.query\":[\"lexical\"]}}";
    QueryResponse rsp = query(CommonParams.JSON, jsonQuery, CommonParams.QT, "/search");
    assertEquals(3, rsp.getResults().size());
    assertEquals(4, rsp.getResults().getNumFound());
    assertEquals("[0 (2), 2 (2)]", rsp.getFacetFields().getFirst().getValues().toString());
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
            + "{\"lexical1\":{\"lucene\":{\"query\":\"id:(2^2 OR 3^1 OR 6^2 OR 5^1)\"}},"
            + "\"lexical2\":{\"lucene\":{\"query\":\"id:(8^1 OR 5^2 OR 7^3 OR 10^2)\"}}},"
            + "\"limit\":4,"
            + "\"fields\":[\"id\",\"score\",\"title\"],"
            + "\"params\":{\"combiner\":true,\"facet\":true,\"facet.field\":\"mod3_idv\","
            + "\"combiner.query\":[\"lexical1\",\"lexical2\"], \"hl\": true,"
            + "\"hl.fl\": \"title\",\"hl.q\":\"test doc\"}}";
    QueryResponse rsp = query(CommonParams.JSON, jsonQuery, CommonParams.QT, "/search");
    assertEquals(4, rsp.getResults().size());
    assertFieldValues(rsp.getResults(), id, "5", "7", "2", "6");
    assertEquals("mod3_idv", rsp.getFacetFields().getFirst().getName());
    assertEquals("[2 (3), 0 (2), 1 (2)]", rsp.getFacetFields().getFirst().getValues().toString());
    assertEquals(4, rsp.getHighlighting().size());
    assertEquals(
        "title <em>test</em> for <em>doc</em> 2",
        rsp.getHighlighting().get("2").get("title").getFirst());
    assertEquals(
        "title <em>test</em> for <em>doc</em> 5",
        rsp.getHighlighting().get("5").get("title").getFirst());
  }

  /**
   * Tests the forced distribution functionality.
   *
   * <p>This test prepares index documents, executes a query with specific parameters, and verifies
   * that the debug map contains expected process and prepare entries.
   *
   * @throws Exception if any error occurs during test execution
   */
  @Test
  public void testForcedDistrib() throws Exception {
    prepareIndexDocs();
    QueryResponse rsp =
        query(
            "q", "id:2", "rows", "0", "debug", "query", "rid", "fd-test", CommonParams.QT, "/tfd");
    assertTrue(rsp.getDebugMap().containsKey("process()"));
    assertTrue(rsp.getDebugMap().containsKey("prepare()"));
  }
}
