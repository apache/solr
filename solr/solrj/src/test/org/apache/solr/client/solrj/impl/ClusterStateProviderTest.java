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

import static org.apache.solr.common.util.URLUtil.getNodeNameForBaseUrl;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.util.NamedList;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClusterStateProviderTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(2)
        .addConfig(
            "conf",
            getFile("solrj")
                .resolve("solr")
                .resolve("configsets")
                .resolve("streaming")
                .resolve("conf"))
        .configure();
    cluster.waitForAllNodes(30);
    System.setProperty("solr.solrj.cache.timeout.sec", "1");
  }

  @After
  public void cleanup() throws Exception {
    while (cluster.getJettySolrRunners().size() < 2) {
      cluster.startJettySolrRunner();
    }
  }

  @ParametersFactory
  public static Iterable<String[]> parameters() throws NoSuchMethodException {
    return List.of(
        new String[] {"http2ClusterStateProvider"}, new String[] {"zkClientClusterStateProvider"});
  }

  private static Http2ClusterStateProvider http2ClusterStateProvider() {
    try {
      return new Http2ClusterStateProvider(
          List.of(
              cluster.getJettySolrRunner(0).getBaseUrl().toString(),
              cluster.getJettySolrRunner(1).getBaseUrl().toString()),
          null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static ClusterStateProvider zkClientClusterStateProvider() {
    return new ZkClientClusterStateProvider(cluster.getZkStateReader());
  }

  private final Supplier<ClusterStateProvider> cspSupplier;

  public ClusterStateProviderTest(String method) throws Exception {
    this.cspSupplier =
        () -> {
          try {
            return (ClusterStateProvider) getClass().getDeclaredMethod(method).invoke(getClass());
          } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
          }
        };

    cluster.deleteAllCollections();
  }

  private ClusterStateProvider createClusterStateProvider() {
    return cspSupplier.get();
  }

  @Test
  public void testGetClusterState() throws Exception {

    createCollection("testGetClusterState");
    createCollection("testGetClusterState2");

    try (ClusterStateProvider provider = createClusterStateProvider()) {

      ClusterState clusterState = provider.getClusterState();

      DocCollection docCollection = clusterState.getCollection("testGetClusterState");
      assertEquals(
          getCreationTimeFromClusterStatus("testGetClusterState"), docCollection.getCreationTime());

      docCollection = clusterState.getCollection("testGetClusterState2");
      assertEquals(
          getCreationTimeFromClusterStatus("testGetClusterState2"),
          docCollection.getCreationTime());
    }
  }

  @Test
  public void testGetState() throws Exception {

    createCollection("testGetState");

    try (ClusterStateProvider provider = createClusterStateProvider()) {

      ClusterState.CollectionRef collectionRef = provider.getState("testGetState");

      DocCollection docCollection = collectionRef.get();
      assertNotNull(docCollection);
      assertEquals(
          getCreationTimeFromClusterStatus("testGetState"), docCollection.getCreationTime());
    }
  }

  private void createCollection(String collectionName) throws SolrServerException, IOException {
    CollectionAdminRequest.Create request =
        CollectionAdminRequest.createCollection(collectionName, "conf", 1, 0, 1, 0);
    request.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collectionName, 1, 1);
  }

  @SuppressWarnings("unchecked")
  private Instant getCreationTimeFromClusterStatus(String collectionName)
      throws SolrServerException, IOException {
    CollectionAdminRequest.ClusterStatus request = CollectionAdminRequest.getClusterStatus();
    request.setCollectionName(collectionName);
    CollectionAdminResponse clusterStatusResponse = request.process(cluster.getSolrClient());
    NamedList<Object> response = clusterStatusResponse.getResponse();

    NamedList<Object> cluster = (NamedList<Object>) response.get("cluster");
    NamedList<Object> collections = (NamedList<Object>) cluster.get("collections");
    Map<String, Object> collection = (Map<String, Object>) collections.get(collectionName);
    return Instant.ofEpochMilli((long) collection.get("creationTimeMillis"));
  }

  @Test
  public void testClusterStateProvider() throws SolrServerException, IOException {
    // we'll test equivalency of the two cluster state providers

    CollectionAdminRequest.setClusterProperty("ext.foo", "bar").process(cluster.getSolrClient());

    createCollection("col1");
    createCollection("col2");

    try (var cspZk = zkClientClusterStateProvider();
        var cspHttp = http2ClusterStateProvider()) {
      assertThat(cspZk.getClusterProperties(), Matchers.hasEntry("ext.foo", "bar"));
      assertThat(
          cspZk.getClusterProperties().entrySet(),
          containsInAnyOrder(cspHttp.getClusterProperties().entrySet().toArray()));

      assertThat(cspHttp.getCollection("col1"), equalTo(cspZk.getCollection("col1")));

      final var clusterStateZk = cspZk.getClusterState();
      final var clusterStateHttp = cspHttp.getClusterState();
      assertThat(
          clusterStateHttp.getLiveNodes(),
          containsInAnyOrder(clusterStateHttp.getLiveNodes().toArray()));
      assertEquals(2, clusterStateZk.size());
      assertEquals(clusterStateZk.size(), clusterStateHttp.size());
      assertThat(
          clusterStateHttp.collectionStream().toList(),
          containsInAnyOrder(clusterStateHttp.collectionStream().toArray()));

      assertThat(
          clusterStateZk.getCollection("col2"), equalTo(clusterStateHttp.getCollection("col2")));
    }
  }

  @Test
  public void testClusterStateProviderDownedInitialLiveNodes() throws Exception {
    try (var cspHttp = http2ClusterStateProvider()) {
      var jettyNode1 = cluster.getJettySolrRunner(0);
      var jettyNode2 = cluster.getJettySolrRunner(1);

      String nodeName1 = getNodeNameForBaseUrl(jettyNode1.getBaseUrl().toString());
      String nodeName2 = getNodeNameForBaseUrl(jettyNode2.getBaseUrl().toString());

      Set<String> actualLiveNodes = cspHttp.getLiveNodes();
      assertEquals(2, actualLiveNodes.size());
      assertEquals(Set.of(nodeName1, nodeName2), actualLiveNodes);

      cluster.stopJettySolrRunner(jettyNode1);
      waitForCSPCacheTimeout();

      actualLiveNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualLiveNodes.size());
      assertEquals(Set.of(nodeName2), actualLiveNodes);

      cluster.startJettySolrRunner(jettyNode1, true);
      cluster.stopJettySolrRunner(jettyNode2);
      waitForCSPCacheTimeout();

      // Should still be reachable because backup nodes
      actualLiveNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualLiveNodes.size());
      assertEquals(Set.of(nodeName1), actualLiveNodes);
    }
  }

  @Test
  public void testClusterStateProviderLiveNodesWithNewNode() throws Exception {
    try (var cspHttp = http2ClusterStateProvider()) {
      var jettyNode1 = cluster.getJettySolrRunner(0);
      var jettyNode2 = cluster.getJettySolrRunner(1);
      var jettyNode3 = cluster.startJettySolrRunner();

      String nodeName1 = getNodeNameForBaseUrl(jettyNode1.getBaseUrl().toString());
      String nodeName2 = getNodeNameForBaseUrl(jettyNode2.getBaseUrl().toString());
      String nodeName3 = getNodeNameForBaseUrl(jettyNode3.getBaseUrl().toString());
      waitForCSPCacheTimeout();

      Set<String> actualKnownNodes = cspHttp.getLiveNodes();
      assertEquals(3, actualKnownNodes.size());
      assertEquals(Set.of(nodeName1, nodeName2, nodeName3), actualKnownNodes);

      // Stop all backup nodes
      cluster.stopJettySolrRunner(jettyNode1);
      cluster.stopJettySolrRunner(jettyNode2);
      waitForCSPCacheTimeout();

      actualKnownNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualKnownNodes.size());
      assertEquals(Set.of(nodeName3), actualKnownNodes);

      // Bring back a backup node and take down the new node
      cluster.startJettySolrRunner(jettyNode2, true);
      cluster.stopJettySolrRunner(jettyNode3);
      waitForCSPCacheTimeout();

      actualKnownNodes = cspHttp.getLiveNodes();
      assertEquals(1, actualKnownNodes.size());
      assertEquals(Set.of(nodeName2), actualKnownNodes);
    }
  }

  private void waitForCSPCacheTimeout() throws InterruptedException {
    Thread.sleep(2000);
  }
}
