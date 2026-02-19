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
package org.apache.solr.handler.admin;

import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.SimpleSolrResponse;
import org.apache.solr.client.solrj.response.json.JsonMapResponseParser;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;

/** Basic tests for {@link ZookeeperInfoHandler} */
public class ZookeeperInfoHandlerTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void testZkInfoHandlerDetailView() throws SolrServerException, IOException {
    SolrClient client = cluster.getSolrClient();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.PATH, "/");
    params.set("detail", "true");
    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper", params);
    req.setResponseParser(new JsonMapResponseParser());

    NamedList<Object> response = client.request(req);
    assertNotNull("Response should not be null", response);

    // ZK handler should return 'znode' for detail requests
    assertNotNull(response.get("znode"));
  }

  @Test
  public void testZkInfoHandlerGraphView() throws Exception {
    // Create a test collection first
    String collectionName = "zkinfo_test_collection";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);

    SolrClient client = cluster.getSolrClient();
    // Return the data to power the Solr Admin UI - Graph.
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("view", "graph");

    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper", params);
    req.setResponseParser(new JsonMapResponseParser());

    // Verify the request completes and returns collection data
    SimpleSolrResponse response = req.process(client);
    NamedList<Object> responseData = response.getResponse();

    assertNotNull("Response should not be null", responseData);
    assertNotNull(
        "Response should contain 'znode' for collections view", responseData.get("znode"));
  }

  @Test
  public void testZkGraphResponseBuilderWithPagination() throws Exception {
    // Create multiple test collections for pagination testing
    String[] collectionNames = {
      "zkgraph_collection_a", "zkgraph_collection_b", "zkgraph_collection_c", "zkgraph_collection_d"
    };

    for (String collectionName : collectionNames) {
      CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collectionName, 1, 1);
    }

    SolrClient client = cluster.getSolrClient();

    // Test pagination with start=0, rows=2
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("view", "graph");
    params.set("start", "0");
    params.set("rows", "2");

    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper", params);
    req.setResponseParser(new JsonMapResponseParser());

    SimpleSolrResponse response = req.process(client);
    NamedList<Object> responseData = response.getResponse();

    assertNotNull("Response should not be null", responseData);
    @SuppressWarnings("unchecked")
    Map<String, Object> znode = (Map<String, Object>) responseData.get("znode");
    assertNotNull("Response should contain 'znode'", znode);

    // Verify paging information is present
    String paging = (String) znode.get("paging");
    assertNotNull("Paging information should be present", paging);
    assertTrue("Paging should include start position", paging.contains("0|"));
    assertTrue("Paging should include rows", paging.contains("|2|"));

    // Verify data field contains collection state (already parsed by JsonMapResponseParser)
    Object dataObj = znode.get("data");
    assertNotNull("Data field should be present", dataObj);

    // Data should be a Map containing collection information
    @SuppressWarnings("unchecked")
    Map<String, Object> collectionData = (Map<String, Object>) dataObj;
    assertNotNull("Collection data should not be null", collectionData);
    assertFalse("Collection data should contain collections", collectionData.isEmpty());
  }

  @Test
  public void testZkGraphResponseBuilderWithNameFilter() throws Exception {
    // Create test collections with specific naming pattern
    String[] collectionNames = {"filter_test_alpha", "filter_test_beta", "other_collection"};

    for (String collectionName : collectionNames) {
      CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
          .process(cluster.getSolrClient());
      cluster.waitForActiveCollection(collectionName, 1, 1);
    }

    SolrClient client = cluster.getSolrClient();

    // Test name filter with pattern
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("view", "graph");
    params.set("filterType", "name");
    params.set("filter", "filter_test*");

    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper", params);
    req.setResponseParser(new JsonMapResponseParser());

    SimpleSolrResponse response = req.process(client);
    NamedList<Object> responseData = response.getResponse();

    assertNotNull("Response should not be null", responseData);
    @SuppressWarnings("unchecked")
    Map<String, Object> znode = (Map<String, Object>) responseData.get("znode");
    assertNotNull("Response should contain 'znode'", znode);

    // Verify paging information includes filter
    String paging = (String) znode.get("paging");
    assertNotNull("Paging information should be present", paging);
    assertTrue("Paging should include filter type", paging.contains("name"));
    assertTrue("Paging should include filter pattern", paging.contains("filter_test*"));

    // Verify data field contains collection state (already parsed by JsonMapResponseParser)
    Object dataObj = znode.get("data");
    assertNotNull("Data field should be present", dataObj);

    // Data should be a Map containing collection information
    @SuppressWarnings("unchecked")
    Map<String, Object> collectionData = (Map<String, Object>) dataObj;
    assertNotNull("Collection data should not be null", collectionData);

    // Verify filtered collections are present in the data
    assertTrue(
        "Should contain filter_test_alpha or filter_test_beta",
        collectionData.containsKey("filter_test_alpha")
            || collectionData.containsKey("filter_test_beta"));
  }

  @Test
  public void testZkGraphResponseBuilderWithDetailParameter() throws Exception {
    // Create a test collection
    String collectionName = "zkgraph_detail_test";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);

    SolrClient client = cluster.getSolrClient();

    // Test with detail parameter
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("view", "graph");
    params.set("detail", "true");

    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper", params);
    req.setResponseParser(new JsonMapResponseParser());

    SimpleSolrResponse response = req.process(client);
    NamedList<Object> responseData = response.getResponse();

    assertNotNull("Response should not be null", responseData);
    @SuppressWarnings("unchecked")
    Map<String, Object> znode = (Map<String, Object>) responseData.get("znode");
    assertNotNull("Response should contain 'znode'", znode);

    // Verify data field contains collection state (already parsed by JsonMapResponseParser)
    Object dataObj = znode.get("data");
    assertNotNull("Data field should be present", dataObj);

    // Data should be a Map containing collection information
    @SuppressWarnings("unchecked")
    Map<String, Object> collectionData = (Map<String, Object>) dataObj;
    assertNotNull("Collection data should not be null", collectionData);
    assertFalse("Collection data should contain collections", collectionData.isEmpty());

    // Verify the collection exists in the data
    assertTrue(
        "Data should contain the test collection", collectionData.containsKey(collectionName));

    // Verify collection has expected structure (shards, replicas, etc.)
    @SuppressWarnings("unchecked")
    Map<String, Object> collectionState = (Map<String, Object>) collectionData.get(collectionName);
    assertNotNull("Collection state should not be null", collectionState);
    assertTrue("Collection should have shards", collectionState.containsKey("shards"));
  }

  @Test
  public void testZkInfoHandlerForcesJsonResponse() throws Exception {
    // Create a test collection
    String collectionName = "zkinfo_wt_test";
    CollectionAdminRequest.createCollection(collectionName, "conf", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);

    SolrClient client = cluster.getSolrClient();

    // Try to request XML format (wt=xml), but handler should force JSON
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("view", "graph");
    params.set(CommonParams.WT, "xml");

    GenericSolrRequest req =
        new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/zookeeper", params);
    req.setResponseParser(new JsonMapResponseParser());

    // Should still get valid JSON response despite wt=xml parameter
    SimpleSolrResponse response = req.process(client);
    NamedList<Object> responseData = response.getResponse();

    assertNotNull("Response should not be null", responseData);
    @SuppressWarnings("unchecked")
    Map<String, Object> znode = (Map<String, Object>) responseData.get("znode");
    assertNotNull("Response should contain 'znode' (JSON format)", znode);

    // Verify we got proper JSON structure with data as Map
    Object dataObj = znode.get("data");
    assertNotNull("Data field should be present", dataObj);
    assertTrue("Data should be a Map (JSON was parsed), not XML string", dataObj instanceof Map);
  }
}
