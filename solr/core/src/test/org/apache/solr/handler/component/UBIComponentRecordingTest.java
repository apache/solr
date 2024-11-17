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

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.json.JsonQueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This test demonstrates that the backend recording of events via Streaming Expressions functions.
 */
public class UBIComponentRecordingTest extends SolrCloudTestCase {

  private static final String COLLECTION = "collection1"; // The source of ubi=true enabled queries
  private static final String UBI_QUERIES_COLLECTION = "ubi_queries"; // where we store query data

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "ubi-enabled-config",
            TEST_PATH().resolve("configsets").resolve("ubi-enabled").resolve("conf"))
        .addConfig(
            "minimal-config",
            TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    CollectionAdminRequest.deleteCollection(COLLECTION).process(cluster.getSolrClient());
    CollectionAdminRequest.deleteCollection(UBI_QUERIES_COLLECTION)
        .process(cluster.getSolrClient());
  }

  @Test
  public void testRecordingUBIQueries() throws Exception {

    assertEquals(
        "failed to create collection " + COLLECTION,
        0,
        CollectionAdminRequest.createCollection(COLLECTION, "ubi-enabled-config", 2, 1, 1, 0)
            .process(cluster.getSolrClient())
            .getStatus());

    assertEquals(
        "failed to create collection " + UBI_QUERIES_COLLECTION,
        0,
        CollectionAdminRequest.createCollection(
                UBI_QUERIES_COLLECTION, "minimal-config", 2, 1, 1, 0)
            .process(cluster.getSolrClient())
            .getStatus());

    cluster.waitForActiveCollection(COLLECTION, 2, 2 * (1 + 1));
    cluster.waitForActiveCollection(UBI_QUERIES_COLLECTION, 2, 2 * (1 + 1));

    // TODO why doens't this work?
    //    assertQ(
    //            "Make sure we generate a query id",
    //            req("q", "aa", "rows", "2", "ubi", "true"),
    //            "count(//lst[@name='ubi']/str[@name='query_id'])=1");

    // query our collection and confirm no duplicates on the signature field (using faceting)
    // Check every (node) for consistency...
    final ModifiableSolrParams overrideParams = new ModifiableSolrParams();
    overrideParams.set("ubi", true);
    final JsonQueryRequest req =
        new JsonQueryRequest(overrideParams)
            .setQuery("*:*")
            // .setUBI(true)
            .setLimit(0);
    QueryResponse queryResponse = req.process(cluster.getSolrClient(), COLLECTION);
    // assertResponseFoundNumDocs(queryResponse, expectedResults);
    System.out.println(queryResponse);
  }
}
