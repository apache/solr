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

package org.apache.solr.cloud;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.api.model.BalanceReplicasRequestBody;
import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.MapWriterMap;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BalanceReplicasTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @BeforeClass
  public static void setupCluster() {
    System.setProperty("metricsEnabled", "true");
  }

  @Before
  public void clearPreviousCluster() throws Exception {
    // Clear the previous cluster before each test, since they use different numbers of nodes.
    shutdownCluster();
  }

  @Test
  public void testAllNodes() throws Exception {
    configureCluster(6)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "balancereplicastest_allnodes_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    CollectionAdminRequest.Create create;

    create =
        pickRandom(
            CollectionAdminRequest.createCollection(coll, "conf1", 3, 2, 0, 0),
            // check also replicationFactor 1
            CollectionAdminRequest.createCollection(coll, "conf1", 6, 1, 0, 0));
    create.setCreateNodeSet(StrUtils.join(l.subList(0, 2), ','));
    cloudClient.request(create);

    cluster.waitForActiveCollection(
        coll,
        create.getNumShards(),
        create.getNumShards()
            * (create.getNumNrtReplicas()
                + create.getNumPullReplicas()
                + create.getNumTlogReplicas()));

    DocCollection collection = cloudClient.getClusterState().getCollection(coll);
    log.debug("### Before balancing: {}", collection);

    postDataAndGetResponse(
        cluster.getSolrClient(),
        "/api/cluster/replicas/balance",
        new MapWriterMap(Collections.emptyMap()));

    collection = cloudClient.getClusterState().getCollectionOrNull(coll, false);
    log.debug("### After balancing: {}", collection);
    Set<String> replicaNodes =
        collection.getReplicas().stream().map(Replica::getNodeName).collect(Collectors.toSet());
    assertEquals("Incorrect nodes for replicas after balancing", liveNodes, replicaNodes);
  }

  @Test
  public void testSomeNodes() throws Exception {
    configureCluster(5)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "balancereplicastest_somenodes_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    CollectionAdminRequest.Create create;

    create =
        pickRandom(
            CollectionAdminRequest.createCollection(coll, "conf1", 3, 2, 0, 0),
            // check also replicationFactor 1
            CollectionAdminRequest.createCollection(coll, "conf1", 6, 1, 0, 0));
    create.setCreateNodeSet(StrUtils.join(l.subList(0, 2), ','));
    cloudClient.request(create);

    cluster.waitForActiveCollection(
        coll,
        create.getNumShards(),
        create.getNumShards()
            * (create.getNumNrtReplicas()
                + create.getNumPullReplicas()
                + create.getNumTlogReplicas()));

    DocCollection collection = cloudClient.getClusterState().getCollection(coll);
    log.debug("### Before balancing: {}", collection);

    postDataAndGetResponse(
        cluster.getSolrClient(),
        "/api/cluster/replicas/balance",
        Utils.getReflectWriter(
            new BalanceReplicasRequestBody(new HashSet<>(l.subList(1, 4)), true, null)));

    collection = cloudClient.getClusterState().getCollectionOrNull(coll, false);
    log.debug("### After balancing: {}", collection);
    Set<String> replicaNodes =
        collection.getReplicas().stream().map(Replica::getNodeName).collect(Collectors.toSet());
    assertEquals("Incorrect nodes for replicas after balancing", 4, replicaNodes.size());
    assertTrue(
        "A non-balanced node lost replicas during balancing", replicaNodes.contains(l.get(0)));
    assertFalse(
        "A non-balanced node gained replicas during balancing", replicaNodes.contains(l.get(4)));
  }

  public Map<?, ?> postDataAndGetResponse(CloudSolrClient cloudClient, String uri, Object body)
      throws IOException {
    HttpEntityEnclosingRequestBase httpRequest = null;
    HttpEntity entity;
    String response = null;
    Map<?, ?> m = null;

    uri = cluster.getJettySolrRunners().get(0).getBaseUrl().toString().replace("/solr", "") + uri;
    try {
      httpRequest = new HttpPost(uri);

      httpRequest.setEntity(new ByteArrayEntity(Utils.toJSON(body), ContentType.APPLICATION_JSON));
      httpRequest.setHeader("Accept", "application/json");
      entity =
          ((CloudLegacySolrClient) cloudClient).getHttpClient().execute(httpRequest).getEntity();
      try {
        response = EntityUtils.toString(entity, UTF_8);
        m = (Map<?, ?>) Utils.fromJSONString(response);
      } catch (JSONParser.ParseException e) {
        log.error("err response: {}", response);
        throw new AssertionError(e);
      }
    } finally {
      httpRequest.releaseConnection();
    }
    return m;
  }
}
