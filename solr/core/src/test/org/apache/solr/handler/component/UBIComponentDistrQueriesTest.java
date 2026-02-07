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

import java.util.List;
import java.util.Map;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.cluster.api.SimpleMap;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class UBIComponentDistrQueriesTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", TEST_PATH().resolve("configsets").resolve("ubi-enabled").resolve("conf"))
        .configure();

    String collection;
    useAlias = false; // random().nextBoolean();
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection(collection, 2, 2);

    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        collection, cluster.getZkStateReader(), false, true, TIMEOUT);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection)
          .process(cluster.getSolrClient());
    }

    // -------------------

    CollectionAdminRequest.createCollection(
            "ubi_queries", // it seems like a hardcoded name why?
            "_default",
            1,
            1)
        .process(cluster.getSolrClient());

    cluster.waitForActiveCollection("ubi_queries", 1, 1);

    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        "ubi_queries", cluster.getZkStateReader(), false, true, TIMEOUT);
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void testUBIQueryStream() throws Exception {
    // Add 3 documents - with 2 shards, these should be distributed
    cluster
        .getSolrClient(COLLECTIONORALIAS)
        .add(
            List.of(
                new SolrInputDocument("id", "1", "subject", "aa"),
                new SolrInputDocument("id", "2", "subject", "aa"),
                new SolrInputDocument("id", "3", "subject", "aa")));
    cluster.getSolrClient(COLLECTIONORALIAS).commit(true, true);

    // Query with ubi=true, requesting only 2 documents
    QueryResponse queryResponse =
        cluster
            .getSolrClient(COLLECTIONORALIAS)
            .query(
                new MapSolrParams(Map.of("q", "aa", "df", "subject", "rows", "2", "ubi", "true")));

    // Verify UBI response was added
    String qid = (String) ((SimpleMap<?>) queryResponse.getResponse().get("ubi")).get("query_id");
    assertNotNull("Query ID should be present", qid);
    assertTrue("Query ID should be a valid UUID", qid.length() > 10);

    // Verify we got results from the distributed query
    assertEquals("Should get 2 results as requested", 2, queryResponse.getResults().size());

    // Force commit on ubi_queries to ensure visibility
    cluster.getSolrClient("ubi_queries").commit(true, true);

    // Poll for the query record with timeout instead of sleeping
    boolean found = awaitQueryRecord(qid, 5000);
    assertTrue("UBI query record should be found within 5 seconds", found);

    // Verify the record contents in the ubi_queries collection
    QueryResponse queryCheck =
        cluster.getSolrClient("ubi_queries").query(new MapSolrParams(Map.of("q", "id:" + qid)));

    assertEquals("Should find exactly one UBI record", 1L, queryCheck.getResults().getNumFound());
    assertEquals("Query ID should match", qid, queryCheck.getResults().get(0).get("id"));

    // Application field may be returned as a list
    Object appObj = queryCheck.getResults().get(0).get("application");
    String application =
        (appObj instanceof List) ? ((List<?>) appObj).get(0).toString() : appObj.toString();
    assertEquals("Application should be the collection name", COLLECTIONORALIAS, application);

    // Verify document IDs were captured from the distributed query
    // Note: doc_ids is stored as a comma-separated string value in a single-valued field
    // but may be returned as a List wrapper by Solr
    Object docIdsObj = queryCheck.getResults().get(0).get("doc_ids");
    assertNotNull("Document IDs should be recorded", docIdsObj);

    String docIds;
    if (docIdsObj instanceof List) {
      @SuppressWarnings("unchecked")
      List<?> docIdsList = (List<?>) docIdsObj;
      assertFalse("Document IDs list should not be empty", docIdsList.isEmpty());
      // The list contains a single string with comma-separated IDs
      docIds = docIdsList.get(0).toString();
    } else {
      docIds = docIdsObj.toString();
    }

    assertFalse("Document IDs should not be empty", docIds.isEmpty());

    // Verify we have at least 1 document ID captured in distributed mode
    // (The exact count may vary depending on shard distribution)
    String[] docIdArray = docIds.split(",");
    assertTrue("Should have recorded at least 1 document ID", docIdArray.length >= 1);

    // Verify the captured doc IDs match what was returned in the query response
    String expectedId1 = queryResponse.getResults().get(0).getFieldValue("id").toString();
    String expectedId2 = queryResponse.getResults().get(1).getFieldValue("id").toString();
    assertTrue(
        "Captured doc_ids should contain first result: " + expectedId1,
        docIds.contains(expectedId1));
    assertTrue(
        "Captured doc_ids should contain second result: " + expectedId2,
        docIds.contains(expectedId2));
  }

  @Test
  public void testUBIInDistributedMode_MultipleShards() throws Exception {
    // Add more documents to ensure they're distributed across shards
    for (int i = 1; i <= 10; i++) {
      cluster
          .getSolrClient(COLLECTIONORALIAS)
          .add(new SolrInputDocument("id", String.valueOf(i), "subject", "distributed"));
    }
    cluster.getSolrClient(COLLECTIONORALIAS).commit(true, true);

    // Query that should hit both shards
    QueryResponse queryResponse =
        cluster
            .getSolrClient(COLLECTIONORALIAS)
            .query(
                new MapSolrParams(
                    Map.of(
                        "q", "distributed",
                        "df", "subject",
                        "rows", "5",
                        "ubi", "true",
                        "user_query", "test distributed query")));

    // Verify we got results
    assertTrue("Should get results from distributed query", queryResponse.getResults().size() > 0);

    String qid = (String) ((SimpleMap<?>) queryResponse.getResponse().get("ubi")).get("query_id");
    assertNotNull("Query ID should be present in distributed mode", qid);

    // Force commit and wait for record
    cluster.getSolrClient("ubi_queries").commit(true, true);
    assertTrue("UBI query should be recorded in distributed mode", awaitQueryRecord(qid, 5000));

    // Verify the record
    QueryResponse queryCheck =
        cluster.getSolrClient("ubi_queries").query(new MapSolrParams(Map.of("q", "id:" + qid)));

    assertEquals("Should find exactly one UBI record", 1L, queryCheck.getResults().getNumFound());

    // Verify user_query was captured (may be returned as a list)
    Object userQueryObj = queryCheck.getResults().get(0).get("user_query");
    String userQuery =
        (userQueryObj instanceof List)
            ? ((List<?>) userQueryObj).get(0).toString()
            : userQueryObj.toString();
    assertEquals("User query should be captured", "test distributed query", userQuery);

    // Verify document IDs were captured
    Object docIdsObj = queryCheck.getResults().get(0).get("doc_ids");
    assertNotNull("Document IDs should be recorded in distributed mode", docIdsObj);

    String docIds;
    if (docIdsObj instanceof List) {
      @SuppressWarnings("unchecked")
      List<?> docIdsList = (List<?>) docIdsObj;
      assertFalse("Document IDs list should not be empty", docIdsList.isEmpty());
      // The list contains a single string with comma-separated IDs
      docIds = docIdsList.get(0).toString();
    } else {
      docIds = docIdsObj.toString();
    }

    assertFalse("Document IDs should not be empty in distributed mode", docIds.isEmpty());

    // Verify we have at least 1 document ID captured in distributed mode
    // (The exact count may vary depending on shard distribution)
    String[] docIdArray = docIds.split(",");
    assertTrue("Should have recorded at least 1 document ID", docIdArray.length >= 1);
  }

  /** Poll for a UBI query record with exponential backoff */
  private boolean awaitQueryRecord(String queryId, long timeoutMs) throws Exception {
    long start = System.currentTimeMillis();
    long sleep = 50;

    while (System.currentTimeMillis() - start < timeoutMs) {
      QueryResponse qr =
          cluster
              .getSolrClient("ubi_queries")
              .query(new MapSolrParams(Map.of("q", "id:" + queryId)));

      if (qr.getResults().getNumFound() > 0) {
        return true;
      }

      Thread.sleep(sleep);
      sleep = Math.min(sleep * 2, 500); // Exponential backoff, max 500ms
    }

    return false;
  }
}
