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

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.api.model.MigrateReplicasRequestBody;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.util.SolrMetricTestUtils;
import org.eclipse.jetty.client.BytesRequestContent;
import org.eclipse.jetty.client.HttpClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.noggit.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrateReplicasTest extends SolrCloudTestCase {
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
  public void test() throws Exception {
    configureCluster(6)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "replacenodetest_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    ArrayList<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    String emptyNode = l.remove(0);
    String nodeToBeDecommissioned = l.get(0);
    CollectionAdminRequest.Create create;
    // NOTE: always using the createCollection that takes in 'int' for all types of replicas, so we
    // never have to worry about null checking when comparing the Create command with the final
    // Slices

    // TODO: tlog replicas do not work correctly in tests due to fault
    // TestInjection#waitForInSyncWithLeader
    create =
        pickRandom(
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 2, 0, 0),
            // CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,1,0),
            // CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,1,1),
            // CollectionAdminRequest.createCollection(coll, "conf1", 5, 1,0,1),
            // CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,2,0),
            // check also replicationFactor 1
            CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 0, 0)
            // CollectionAdminRequest.createCollection(coll, "conf1", 5, 0,1,0)
            );
    create.setCreateNodeSet(StrUtils.join(l, ','));
    cloudClient.request(create);

    cluster.waitForActiveCollection(
        coll,
        5,
        5
            * (create.getNumNrtReplicas()
                + create.getNumPullReplicas()
                + create.getNumTlogReplicas()));

    DocCollection collection = cloudClient.getClusterState().getCollection(coll);
    log.debug("### Before decommission: {}", collection);
    log.info("excluded_node : {}  ", emptyNode);
    Map<?, ?> response =
        callMigrateReplicas(
            new MigrateReplicasRequestBody(
                Set.of(nodeToBeDecommissioned), Set.of(emptyNode), true, null));
    assertEquals(
        "MigrateReplicas request was unsuccessful",
        0L,
        ((Map<?, ?>) response.get("responseHeader")).get("status"));
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    try (SolrClient coreClient =
        getHttpSolrClient(zkStateReader.getBaseUrlForNodeName(nodeToBeDecommissioned))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreClient);
      assertEquals(
          "There should not be any cores left on decommissioned node",
          0,
          status.getCoreStatus().size());
    }

    Thread.sleep(5000);
    collection = cloudClient.getClusterState().getCollectionOrNull(coll, false);
    log.debug("### After decommission: {}", collection);
    // check what are replica states on the decommissioned node
    assertTrue(
        "There should not be any replicas left on decommissioned node",
        collection.getReplicasOnNode(nodeToBeDecommissioned).isEmpty());

    // let's do it back - this time wait for recoveries
    response =
        callMigrateReplicas(
            new MigrateReplicasRequestBody(
                Set.of(emptyNode), Set.of(nodeToBeDecommissioned), true, null));
    assertEquals(
        "MigrateReplicas request was unsuccessful",
        0L,
        ((Map<?, ?>) response.get("responseHeader")).get("status"));

    try (SolrClient coreClient =
        getHttpSolrClient(zkStateReader.getBaseUrlForNodeName(emptyNode))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreClient);
      assertEquals(
          "Expecting no cores but found some: " + status.getCoreStatus(),
          0,
          status.getCoreStatus().size());
    }

    collection = cluster.getSolrClient().getClusterState().getCollection(coll);
    assertEquals(create.getNumShards().intValue(), collection.getSlices().size());
    for (Slice s : collection.getSlices()) {
      assertEquals(
          create.getNumNrtReplicas().intValue(),
          s.getReplicas(EnumSet.of(Replica.Type.NRT)).size());
      assertEquals(
          create.getNumTlogReplicas().intValue(),
          s.getReplicas(EnumSet.of(Replica.Type.TLOG)).size());
      assertEquals(
          create.getNumPullReplicas().intValue(),
          s.getReplicas(EnumSet.of(Replica.Type.PULL)).size());
    }
    // make sure all newly created replicas on node are active
    List<Replica> newReplicas = collection.getReplicasOnNode(nodeToBeDecommissioned);
    assertFalse("There should be replicas on the migrated-to node", newReplicas.isEmpty());
    for (Replica r : newReplicas) {
      assertEquals(r.toString(), Replica.State.ACTIVE, r.getState());
    }
    // make sure all replicas on emptyNode are not active
    List<Replica> replicas = collection.getReplicasOnNode(emptyNode);
    for (Replica r : replicas) {
      assertNotEquals(r.toString(), Replica.State.ACTIVE, r.getState());
    }

    // check replication metrics on this jetty - see SOLR-14924
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (emptyNode.equals(jetty.getNodeName())) {
        // No cores on this node, ignore it
        continue;
      }

      CoreContainer coreContainer = jetty.getCoreContainer();
      List<String> coreNames = jetty.getCoreContainer().getAllCoreNames();
      for (String coreName : coreNames) {
        try (var core = coreContainer.getCore(coreName)) {
          var dp =
              SolrMetricTestUtils.getGaugeDatapoint(
                  core,
                  "solr_core_replication_is_replicating",
                  SolrMetricTestUtils.newCloudLabelsBuilder(core)
                      .label("category", SolrInfoBean.Category.REPLICATION.toString())
                      .label("handler", "/replication")
                      .build());
          if (dp == null) continue;

          double isReplicating = dp.getValue();
          assertTrue(
              "solr_core_replication_is_replicating should be 0 or 1, got: " + isReplicating,
              isReplicating == 0.0 || isReplicating == 1.0);
        }
      }
    }
  }

  @Test
  public void testWithNoTarget() throws Exception {
    configureCluster(5)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "migratereplicastest_notarget_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    List<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    List<String> nodesToBeDecommissioned = l.subList(0, 2);
    List<String> eventualTargetNodes = l.subList(2, l.size());

    // TODO: tlog replicas do not work correctly in tests due to fault
    // TestInjection#waitForInSyncWithLeader
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(coll, "conf1", 3, 2, 0, 0);
    cloudClient.request(create);

    cluster.waitForActiveCollection(
        coll,
        create.getNumShards(),
        create.getNumShards()
            * (create.getNumNrtReplicas()
                + create.getNumPullReplicas()
                + create.getNumTlogReplicas()));

    DocCollection initialCollection = cloudClient.getClusterState().getCollection(coll);
    log.info("### Before decommission: {}", initialCollection);
    Map<?, ?> response =
        callMigrateReplicas(
            new MigrateReplicasRequestBody(
                new HashSet<>(nodesToBeDecommissioned), Set.of(), true, null));
    assertEquals(
        "MigrateReplicas request was unsuccessful",
        0L,
        ((Map<?, ?>) response.get("responseHeader")).get("status"));

    DocCollection collection = cloudClient.getClusterState().getCollectionOrNull(coll, false);
    assertNotNull("Collection cannot be null: " + coll, collection);
    log.info("### After decommission: {}", collection);
    // check what are replica states on the decommissioned nodes
    for (String nodeToBeDecommissioned : nodesToBeDecommissioned) {
      List<Replica> replicas = collection.getReplicasOnNode(nodeToBeDecommissioned);
      assertTrue(
          "There should be no more replicas on the sourceNode after a migrateReplicas request.",
          replicas.isEmpty());
    }

    for (String node : eventualTargetNodes) {
      assertNotEquals(
          "The non-source node '" + node + "' should not receive all replicas from the migration",
          4,
          collection.getReplicasOnNode(node).size());
    }
  }

  @Test
  public void testFailOnSingleNode() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "migratereplicastest_singlenode_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    cloudClient.request(CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 0, 0));

    cluster.waitForActiveCollection(coll, 5, 5);

    String liveNode = cloudClient.getClusterState().getLiveNodes().iterator().next();
    Map<?, ?> response =
        callMigrateReplicas(new MigrateReplicasRequestBody(Set.of(liveNode), Set.of(), true, null));
    assertNotNull(
        "No error in response, when the request should have failed", response.get("error"));
    assertEquals(
        "Wrong error message",
        "No nodes other than the source nodes are live, therefore replicas cannot be migrated",
        ((Map<?, ?>) response.get("error")).get("msg"));
  }

  public Map<?, ?> callMigrateReplicas(MigrateReplicasRequestBody body) throws IOException {
    String rspStr = null;
    var jetty = cluster.getRandomJetty(random());
    HttpClient httpClient = jetty.getSolrClient().getHttpClient();
    try {
      var rsp =
          httpClient
              .POST(jetty.getBaseURLV2() + "/cluster/replicas/migrate")
              .body(
                  new BytesRequestContent(
                      "application/json", Utils.toJSON(Utils.getReflectWriter(body))))
              .send();
      rspStr = rsp.getContentAsString();
      var rspMap = (Map<?, ?>) Utils.fromJSONString(rspStr);
      assertNotNull("No response given from MigrateReplicas API", rspMap);
      assertNotNull(
          "No responseHeader given from MigrateReplicas API", rspMap.get("responseHeader"));
      return rspMap;
    } catch (JSONParser.ParseException e) {
      log.error("err response: {}", rspStr);
      throw new AssertionError(e);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
