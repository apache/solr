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
}
