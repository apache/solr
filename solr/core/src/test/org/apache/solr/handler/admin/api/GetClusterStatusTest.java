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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.client.solrj.SolrRequest.METHOD.GET;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.api.model.GetClusterStatusResponse;
import org.apache.solr.client.api.model.GetClusterStatusResponse.CollectionState;
import org.apache.solr.client.api.model.GetClusterStatusResponse.ReplicaState;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.request.ClusterApi;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * HTTP tests for {@code GET /api/cluster}.
 *
 * <p>The response is the collections, shards, and replicas tree. Live nodes, the alias map, and
 * cluster properties stay on their own endpoints. v1 {@code CLUSTERSTATUS} still returns them.
 */
public class GetClusterStatusTest extends SolrCloudTestCase {

  private static final String COLLECTION = "clusterstatuscoll";
  private static final String ALIAS = "clusterstatusalias";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
    CollectionAdminRequest.createCollection(COLLECTION, "conf", 1, 1)
        .process(cluster.getSolrClient());
    CollectionAdminRequest.createAlias(ALIAS, COLLECTION).process(cluster.getSolrClient());
  }

  @Test
  public void testReturnsCollectionTreeWithoutClusterLevelExtras() throws Exception {
    Map<String, Object> clusterState = clusterObject(getCluster(""));
    assertNull(clusterState.get("live_nodes"));
    assertNull(clusterState.get("aliases"));
    assertNull(clusterState.get("properties"));

    Map<String, Object> collection = collection(clusterState, COLLECTION);
    assertNotNull(collection.get("health"));
    assertEquals("conf", collection.get("configName"));
    assertNotNull(collection.get("router"));
    assertEquals(Set.of("shard1"), shards(collection).keySet());
    assertNotNull(replica(collection).get("node_name"));
    assertNotNull(replica(collection).get("state"));
    assertTrue(aliasesOf(collection).contains(ALIAS));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGeneratedClientReadsTheTree() throws Exception {
    GetClusterStatusResponse response =
        new ClusterApi.GetClusterStatus().process(cluster.getSolrClient());
    assertNull(response.error);
    CollectionState collection = response.cluster.collections.get(COLLECTION);
    assertNotNull(collection);
    assertEquals("conf", collection.configName);
    assertNotNull(collection.health);
    assertTrue(collection.aliases.contains(ALIAS));
    assertNotNull(collection.router);
    assertEquals("compositeId", collection.router.get("name"));
    assertEquals(Integer.valueOf(1), collection.replicationFactor);

    ReplicaState replica = collection.shards.get("shard1").replicas.values().iterator().next();
    assertNotNull(replica.nodeName);
    assertNotNull(replica.state);
    assertEquals("true", replica.leader);
  }

  @Test
  public void testV1SelectionParamsAreIgnored() throws Exception {
    Map<String, Object> clusterState =
        clusterObject(
            getCluster("?liveNodes=true&aliases=true&clusterProperties=true&includeAll=true"));
    assertNull(clusterState.get("live_nodes"));
    assertNull(clusterState.get("aliases"));
    assertNull(clusterState.get("properties"));
    assertNotNull(collections(clusterState).get(COLLECTION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPerReplicaState() throws Exception {
    final String prsCollection = "prsclusterstatus";
    CollectionAdminRequest.createCollection(prsCollection, "conf", 1, 1)
        .setPerReplicaState(Boolean.TRUE)
        .process(cluster.getSolrClient());

    Map<String, Object> prs =
        (Map<String, Object>)
            collection(
                    clusterObject(getCluster("?collection=" + prsCollection + "&prs=true")),
                    prsCollection)
                .get("PRS");
    assertNotNull(prs);
    assertNotNull(prs.get("states"));

    var request = new ClusterApi.GetClusterStatus();
    request.setCollection(prsCollection);
    request.setPrs(true);
    CollectionState typed =
        request.process(cluster.getSolrClient()).cluster.collections.get(prsCollection);
    assertNotNull(typed.unknownProperties().get("PRS"));
  }

  @Test
  public void testCollectionShardAndAliasFilters() throws Exception {
    Map<String, Object> byCollection =
        collections(clusterObject(getCluster("?collection=" + COLLECTION)));
    assertEquals(Set.of(COLLECTION), byCollection.keySet());

    Map<String, Object> oneShard =
        shards(
            collection(
                clusterObject(getCluster("?collection=" + COLLECTION + "&shard=shard1")),
                COLLECTION));
    assertEquals(Set.of("shard1"), oneShard.keySet());

    ContentResponse missingShard = httpGet("?collection=" + COLLECTION + "&shard=nosuchshard");
    assertEquals(400, missingShard.getStatus());

    assertNotNull(collections(clusterObject(getCluster("?collection=" + ALIAS))).get(COLLECTION));
  }

  @Test
  public void testUnknownCollectionIsRejected() throws Exception {
    ContentResponse response = httpGet("?collection=not-a-collection");
    assertEquals(400, response.getStatus());
    assertTrue(response.getContentAsString().contains("not found"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testV1ClusterStatusStillReturnsLiveNodes() throws Exception {
    NamedList<Object> response =
        CollectionAdminRequest.getClusterStatus().process(cluster.getSolrClient()).getResponse();
    Map<String, Object> clusterState = (Map<String, Object>) response.get("cluster");
    assertNotNull(clusterState.get("live_nodes"));
    assertNotNull(clusterState.get("collections"));

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "CLUSTERSTATUS");
    params.set("liveNodes", false);
    params.set("aliases", false);
    params.set("clusterProperties", false);
    NamedList<Object> collectionsOnly =
        cluster
            .getSolrClient()
            .request(
                new GenericSolrRequest(GET, "/admin/collections", SolrRequestType.ADMIN, params));
    Map<String, Object> filtered = (Map<String, Object>) collectionsOnly.get("cluster");
    assertNull(filtered.get("live_nodes"));
    assertNull(filtered.get("aliases"));
    assertNull(filtered.get("properties"));
    assertNotNull(((Map<String, Object>) filtered.get("collections")).get(COLLECTION));
  }

  private static Map<String, Object> getCluster(String query) throws Exception {
    ContentResponse response = httpGet(query);
    assertEquals(response.getContentAsString(), 200, response.getStatus());
    return parsed(response);
  }

  private static ContentResponse httpGet(String query) throws Exception {
    HttpClient httpClient = cluster.getJettySolrRunner(0).getSolrClient().getHttpClient();
    String url = cluster.getJettySolrRunner(0).getBaseURLV2().toString() + "/cluster" + query;
    return httpClient.GET(url);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parsed(ContentResponse response) {
    return (Map<String, Object>) Utils.fromJSONString(response.getContentAsString());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> clusterObject(Map<String, Object> body) {
    assertNotNull(body.get("cluster"));
    return (Map<String, Object>) body.get("cluster");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> collections(Map<String, Object> clusterState) {
    return (Map<String, Object>) clusterState.get("collections");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> collection(Map<String, Object> clusterState, String name) {
    Map<String, Object> collection = (Map<String, Object>) collections(clusterState).get(name);
    assertNotNull(collection);
    return collection;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> shards(Map<String, Object> collection) {
    return (Map<String, Object>) collection.get("shards");
  }

  @SuppressWarnings("unchecked")
  private static List<String> aliasesOf(Map<String, Object> collection) {
    return (List<String>) collection.get("aliases");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> replica(Map<String, Object> collection) {
    Map<String, Object> shard = (Map<String, Object>) shards(collection).get("shard1");
    Map<String, Object> replicas = (Map<String, Object>) shard.get("replicas");
    assertFalse(replicas.isEmpty());
    return (Map<String, Object>) replicas.values().iterator().next();
  }
}
