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

package org.apache.solr.client.solrj.impl;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.NamedList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BaseHttpClusterStateProviderTest extends SolrCloudTestCase {

  private CloudSolrClient cloudSolrClient;

  private BaseHttpClusterStateProvider baseHttpClusterStateProvider;

  @Before
  public void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf",
            getFile("solrj")
                .toPath()
                .resolve("solr")
                .resolve("configsets")
                .resolve("streaming")
                .resolve("conf"))
        .configure();

    cloudSolrClient = cluster.getSolrClient();

    List<String> solrUrls = new ArrayList<>();
    solrUrls.add(cluster.getJettySolrRunner(0).getBaseUrl().toString());

    baseHttpClusterStateProvider = new HttpClusterStateProviderForTest(solrUrls);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    try {
      baseHttpClusterStateProvider.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    shutdownCluster();
    super.tearDown();
  }

  @Test
  public void getClusterState() throws Exception {
    // given
    createCollection("collectionName");
    createCollection("collectionName2");

    // when
    ClusterState clusterState = baseHttpClusterStateProvider.getClusterState();

    // then
    DocCollection docCollection = clusterState.getCollectionOrNull("collectionName");
    assertNotNull(docCollection);
    assertEquals(
        getCreationTimeFromClusterStatus("collectionName"), docCollection.getCreationTime());

    docCollection = clusterState.getCollectionOrNull("collectionName2");
    assertNotNull(docCollection);
    assertEquals(
        getCreationTimeFromClusterStatus("collectionName2"), docCollection.getCreationTime());
  }

  @Test
  public void getState() throws Exception {
    // given
    createCollection("collectionName");

    // when
    ClusterState.CollectionRef collectionRef =
        baseHttpClusterStateProvider.getState("collectionName");

    // then
    DocCollection docCollection = collectionRef.get();
    assertNotNull(docCollection);
    assertEquals(
        getCreationTimeFromClusterStatus("collectionName"), docCollection.getCreationTime());
  }

  private void createCollection(String collectionName) throws SolrServerException, IOException {
    CollectionAdminRequest.Create request =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 0, 1, 0);
    request.process(cloudSolrClient);
    cluster.waitForActiveCollection(collectionName, 1, 1);
  }

  @SuppressWarnings("unchecked")
  private Instant getCreationTimeFromClusterStatus(String collectionName)
      throws SolrServerException, IOException {
    CollectionAdminRequest.ClusterStatus request = CollectionAdminRequest.getClusterStatus();
    request.setCollectionName(collectionName);
    CollectionAdminResponse clusterStatusResponse = request.process(cloudSolrClient);
    NamedList<Object> response = clusterStatusResponse.getResponse();

    NamedList<Object> cluster = (NamedList<Object>) response.get("cluster");
    NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
    Map<String, Object> collection = (Map<String, Object>) collections.get(collectionName);
    return Instant.ofEpochMilli((long) collection.get("creationTimeMillis"));
  }

  /** Implementation of abstract methods, that are not under test in this test class */
  private static class HttpClusterStateProviderForTest extends BaseHttpClusterStateProvider {
    private final Http2SolrClient httpClient;

    private HttpClusterStateProviderForTest(List<String> solrUrls) throws Exception {
      this.httpClient = new Http2SolrClient.Builder().build();
      init(solrUrls);
    }

    @Override
    protected SolrClient getSolrClient(String baseUrl) {
      return new Http2SolrClient.Builder(baseUrl).withHttpClient(httpClient).build();
    }

    @Override
    public void close() {
      httpClient.close();
    }
  }
}
