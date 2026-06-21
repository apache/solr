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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.routing.ShufflingReplicaListTransformer;
import org.apache.solr.cloud.MockZkStateReader;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.CompositeIdRouter;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link CloudHttp2SolrClient} document-routing logic ({@code buildUrlMap} and
 * {@link UpdateRequest#getRoutesToCollection}).
 *
 * <p>This was extracted from the (deleted) WireMock-based cloud client test. Routing is pure
 * cluster-state computation that needs no HTTP server, so it is wired against a {@link
 * MockZkStateReader} carrying a fixed two-shard collection — no WireMock (which is
 * binary-incompatible with this fork's Jetty 10) and no live Jetty are required.
 */
public class CloudHttp2SolrClientRouteLogicTest extends SolrTestCase {

  private static final String COLLECTION = "wireMock";
  private static final String HOST_PORT = "127.0.0.1:65260";
  private static final String NODE_NAME = HOST_PORT + "_solr";
  private static final String BASE_URL = "http://" + HOST_PORT;
  private static final String SHARD1_PATH = "/solr/wireMock_s1_r_n1";
  private static final String SHARD2_PATH = "/solr/wireMock_s2_r_n2";

  private DocCollection mockDocCollection;
  private CloudHttp2SolrClient testClient;
  private ClusterStateProvider stateProvider;

  private static Map<String, Object> replica(String name, long id) {
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("core", name);
    r.put("base_url", BASE_URL + "/solr");
    r.put("node_name", NODE_NAME);
    r.put("state", "active");
    r.put("type", "NRT");
    r.put("force_set_state", "false");
    r.put("id", id);
    r.put("leader", "true");
    return r;
  }

  private static Map<String, Object> shard(String range, String replicaName, long replicaId) {
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("range", range);
    s.put("state", "active");
    s.put("replicas", Collections.singletonMap(replicaName, replica(replicaName, replicaId)));
    return s;
  }

  private static DocCollection buildMockDocCollection() {
    Map<String, Object> slices = new LinkedHashMap<>();
    slices.put("s2", shard("0-7fffffff", "wireMock_s2_r_n2", 1L));
    slices.put("s1", shard("80000000-ffffffff", "wireMock_s1_r_n1", 2L));

    Map<String, Object> props = new HashMap<>();
    props.put("replicationFactor", "1");
    props.put("maxShardsPerNode", "1");
    props.put("autoAddReplicas", "false");
    props.put("nrtReplicas", "1");
    props.put("id", 1L);
    return new DocCollection(
        COLLECTION,
        Slice.loadAllFromMap(COLLECTION, -1, slices),
        props,
        CompositeIdRouter.DEFAULT);
  }

  @Before
  public void createTestClient() throws Exception {
    mockDocCollection = buildMockDocCollection();
    Map<String, ClusterState.CollectionRef> collectionStates =
        Collections.singletonMap(COLLECTION, new ClusterState.CollectionRef(mockDocCollection));
    ClusterState clusterState = new ClusterState(collectionStates, 1);
    MockZkStateReader zkStateReader =
        new MockZkStateReader(clusterState, Collections.singleton(NODE_NAME));
    stateProvider = new ZkClientClusterStateProvider(zkStateReader, true);

    List<String> solrUrls = Collections.singletonList(BASE_URL);
    testClient =
        new CloudHttp2SolrClient.Builder(solrUrls)
            .sendDirectUpdatesToShardLeadersOnly()
            .withClusterStateProvider(stateProvider)
            .build();
  }

  @After
  public void closeTestClient() throws IOException {
    if (testClient != null) {
      testClient.close();
    }
    if (stateProvider != null) {
      stateProvider.close();
    }
  }

  private UpdateRequest buildUpdateRequest(final int numDocs) {
    String threadName = Thread.currentThread().getName();
    UpdateRequest req = new UpdateRequest();
    for (int i = 0; i < numDocs; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", threadName + (1000 + i));
      req.add(doc);
    }
    return req;
  }

  // Basic regression test for route logic, should expand to encompass
  // ReplicaListTransformer logic and more complex collection layouts
  @Test
  public void testUpdateRequestRouteLogic() {
    final String shard1Route = BASE_URL + SHARD1_PATH;
    final String shard2Route = BASE_URL + SHARD2_PATH;

    final int numDocs = 20;
    UpdateRequest ur = buildUpdateRequest(numDocs);
    Object2ObjectMap<String, ObjectList<String>> urlMap =
        testClient.buildUrlMap(mockDocCollection, new ShufflingReplicaListTransformer(random()));
    assertEquals(2, urlMap.size());
    List<String> shard1 = urlMap.get("s1");
    assertEquals(1, shard1.size());
    assertEquals(shard1Route, shard1.get(0));
    List<String> shard2 = urlMap.get("s2");
    assertEquals(1, shard2.size());
    assertEquals(shard2Route, shard2.get(0));

    Map<String, LBSolrClient.Req> routes =
        ur.getRoutesToCollection(
            mockDocCollection.getRouter(), mockDocCollection, urlMap, ur.getParams(), "id");
    assertEquals(2, routes.size());
    assertNotNull(routes.get(shard1Route));
    assertNotNull(routes.get(shard1Route).getRequest());
    assertNotNull(routes.get(shard2Route));
    assertNotNull(routes.get(shard2Route).getRequest());

    final String threadName = Thread.currentThread().getName();
    ur = new UpdateRequest();
    for (int i = 0; i < numDocs; i++) {
      ur.deleteById(threadName + (1000 + i));
    }
    routes =
        ur.getRoutesToCollection(
            mockDocCollection.getRouter(), mockDocCollection, urlMap, ur.getParams(), "id");
    assertEquals(2, routes.size());
    assertNotNull(routes.get(shard1Route));
    assertNotNull(routes.get(shard1Route).getRequest());
    assertNotNull(routes.get(shard2Route));
    assertNotNull(routes.get(shard2Route).getRequest());
  }
}
