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
import org.apache.solr.client.api.model.ListCollectionsResponse;
import org.apache.solr.client.api.model.ListCollectionsResponse.CollectionState;
import org.apache.solr.client.api.model.ListCollectionsResponse.ReplicaState;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionsApi;
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
 * HTTP tests for {@code GET /api/collections?detailed=true}.
 *
 * <p>The response is the collections, shards, and replicas tree. Live nodes, the alias map, and
 * cluster properties stay on their own endpoints (not this one). v1 {@code CLUSTERSTATUS} still
 * returns them.
 */
public class ListCollectionsDetailedTest extends SolrCloudTestCase {

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
    Map<String, Object> body = getCollections("?detailed=true");
    assertNull(body.get("cluster"));
    assertNull(body.get("live_nodes"));
    assertNull(body.get("aliases"));
    assertNull(body.get("properties"));

    Map<String, Object> collection = collection(body, COLLECTION);
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
    var req = new CollectionsApi.ListCollections();
    req.setDetailed(true);
    ListCollectionsResponse response = req.process(cluster.getSolrClient());
    assertNull(response.error);
    assertNull(response.collections);
    CollectionState collection = response.collectionsDetail.get(COLLECTION);
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
  public void testWithoutDetailedReturnsPlainNameList() throws Exception {
    var req = new CollectionsApi.ListCollections();
    ListCollectionsResponse response = req.process(cluster.getSolrClient());
    assertNull(response.error);
    assertNull(response.collectionsDetail);
    assertTrue(response.collections.contains(COLLECTION));
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
                    getCollections("?detailed=true&collection=" + prsCollection + "&prs=true"),
                    prsCollection)
                .get("PRS");
    assertNotNull(prs);
    assertNotNull(prs.get("states"));

    var request = new CollectionsApi.ListCollections();
    request.setDetailed(true);
    request.setCollection(prsCollection);
    request.setPrs(true);
    CollectionState typed =
        request.process(cluster.getSolrClient()).collectionsDetail.get(prsCollection);
    assertNotNull(typed.unknownProperties().get("PRS"));
  }

  @Test
  public void testCollectionShardAndAliasFilters() throws Exception {
    Map<String, Object> byCollection =
        collections(getCollections("?detailed=true&collection=" + COLLECTION));
    assertEquals(Set.of(COLLECTION), byCollection.keySet());

    Map<String, Object> oneShard =
        shards(
            collection(
                getCollections("?detailed=true&collection=" + COLLECTION + "&shard=shard1"),
                COLLECTION));
    assertEquals(Set.of("shard1"), oneShard.keySet());

    ContentResponse missingShard =
        httpGet("?detailed=true&collection=" + COLLECTION + "&shard=nosuchshard");
    assertEquals(400, missingShard.getStatus());

    assertNotNull(
        collections(getCollections("?detailed=true&collection=" + ALIAS)).get(COLLECTION));
  }

  @Test
  public void testUnknownCollectionIsRejected() throws Exception {
    ContentResponse response = httpGet("?detailed=true&collection=not-a-collection");
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

  private static Map<String, Object> getCollections(String query) throws Exception {
    ContentResponse response = httpGet(query);
    assertEquals(response.getContentAsString(), 200, response.getStatus());
    return parsed(response);
  }

  private static ContentResponse httpGet(String query) throws Exception {
    HttpClient httpClient = cluster.getJettySolrRunner(0).getSolrClient().getHttpClient();
    String url = cluster.getJettySolrRunner(0).getBaseURLV2().toString() + "/collections" + query;
    return httpClient.GET(url);
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> parsed(ContentResponse response) {
    return (Map<String, Object>) Utils.fromJSONString(response.getContentAsString());
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> collections(Map<String, Object> body) {
    return (Map<String, Object>) body.get("collectionsDetail");
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> collection(Map<String, Object> body, String name) {
    Map<String, Object> collection = (Map<String, Object>) collections(body).get(name);
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
