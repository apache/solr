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

import static org.apache.solr.common.params.CollectionParams.SOURCE_NODE;
import static org.apache.solr.common.params.CollectionParams.TARGET_NODE;

import com.codahale.metrics.Metric;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.embedded.JettySolrRunner;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceNodeTest extends SolrCloudTestCase {
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
    createReplaceNodeRequest(nodeToBeDecommissioned, emptyNode, null)
        .processAndWait("000", cloudClient, 15);
    ZkStateReader zkStateReader = ZkStateReader.from(cloudClient);
    try (SolrClient coreClient =
        getHttpSolrClient(zkStateReader.getBaseUrlForNodeName(nodeToBeDecommissioned))) {
      CoreAdminResponse status = CoreAdminRequest.getStatus(null, coreClient);
      assertEquals(0, status.getCoreStatus().size());
    }

    Thread.sleep(5000);
    collection = cloudClient.getClusterState().getCollection(coll);
    log.debug("### After decommission: {}", collection);
    // check what are replica states on the decommissioned node
    List<Replica> replicas = collection.getReplicasOnNode(nodeToBeDecommissioned);
    if (replicas == null) {
      replicas = Collections.emptyList();
    }
    log.debug("### Existing replicas on decommissioned node: {}", replicas);

    // let's do it back - this time wait for recoveries
    CollectionAdminRequest.AsyncCollectionAdminRequest replaceNodeRequest =
        createReplaceNodeRequest(emptyNode, nodeToBeDecommissioned, Boolean.TRUE);
    replaceNodeRequest.setWaitForFinalState(true);
    replaceNodeRequest.processAndWait("001", cloudClient, 10);

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
    replicas.forEach(r -> newReplicas.removeIf(nr -> nr.getName().equals(r.getName())));
    assertFalse(newReplicas.isEmpty());
    for (Replica r : newReplicas) {
      assertEquals(r.toString(), Replica.State.ACTIVE, r.getState());
    }
    // make sure all replicas on emptyNode are not active
    replicas = collection.getReplicasOnNode(emptyNode);
    if (replicas != null) {
      for (Replica r : replicas) {
        assertNotEquals(r.toString(), Replica.State.ACTIVE, r.getState());
      }
    }

    // check replication metrics on this jetty - see SOLR-14924
    for (JettySolrRunner jetty : cluster.getJettySolrRunners()) {
      if (emptyNode.equals(jetty.getNodeName())) {
        // No cores on this node, ignore it
        continue;
      }
      SolrMetricManager metricManager = jetty.getCoreContainer().getMetricManager();
      String registryName = null;
      for (String name : metricManager.registryNames()) {
        if (name.startsWith("solr.core.")) {
          registryName = name;
        }
      }
      Map<String, Metric> metrics = metricManager.registry(registryName).getMetrics();
      if (!metrics.containsKey("REPLICATION./replication.fetcher")) {
        continue;
      }
      MetricsMap fetcherGauge =
          (MetricsMap)
              ((SolrMetricManager.GaugeWrapper<?>) metrics.get("REPLICATION./replication.fetcher"))
                  .getGauge();
      assertNotNull("no IndexFetcher gauge in metrics", fetcherGauge);
      Map<String, Object> value = fetcherGauge.getValue();
      if (value.isEmpty()) {
        continue;
      }
      assertNotNull("isReplicating missing: " + value, value.get("isReplicating"));
      assertTrue(
          "isReplicating should be a boolean: " + value,
          value.get("isReplicating") instanceof Boolean);
      if (value.get("indexReplicatedAt") == null) {
        continue;
      }
      assertNotNull("timesIndexReplicated missing: " + value, value.get("timesIndexReplicated"));
      assertTrue(
          "timesIndexReplicated should be a number: " + value,
          value.get("timesIndexReplicated") instanceof Number);
    }
  }

  @Test
  public void testGoodSpreadDuringAssignWithNoTarget() throws Exception {
    configureCluster(4)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "replacenodetest_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    Set<String> liveNodes = cloudClient.getClusterState().getLiveNodes();
    List<String> l = new ArrayList<>(liveNodes);
    Collections.shuffle(l, random());
    List<String> emptyNodes = l.subList(0, 2);
    l = l.subList(2, l.size());
    String nodeToBeDecommissioned = l.get(0);

    int numShards = 3;

    // TODO: tlog replicas do not work correctly in tests due to fault
    // TestInjection#waitForInSyncWithLeader
    CollectionAdminRequest.Create create =
        CollectionAdminRequest.createCollection(coll, "conf1", numShards, 2, 0, 0);
    create.setCreateNodeSet(StrUtils.join(l, ','));
    cloudClient.request(create);

    cluster.waitForActiveCollection(
        coll,
        numShards,
        numShards
            * (create.getNumNrtReplicas()
                + create.getNumPullReplicas()
                + create.getNumTlogReplicas()));

    DocCollection initialCollection = cloudClient.getClusterState().getCollection(coll);
    log.debug("### Before decommission: {}", initialCollection);
    log.info("excluded_nodes : {}  ", emptyNodes);
    List<Integer> initialReplicaCounts =
        l.stream()
            .map(node -> initialCollection.getReplicasOnNode(node).size())
            .collect(Collectors.toList());
    createReplaceNodeRequest(nodeToBeDecommissioned, null, true)
        .processAndWait("000", cloudClient, 15);

    DocCollection collection = cloudClient.getClusterState().getCollectionOrNull(coll, false);
    assertNotNull("Collection cannot be null: " + coll, collection);
    log.debug("### After decommission: {}", collection);
    // check what are replica states on the decommissioned node
    List<Replica> replicas = collection.getReplicasOnNode(nodeToBeDecommissioned);
    if (replicas == null) {
      replicas = Collections.emptyList();
    }
    assertEquals(
        "There should be no more replicas on the sourceNode after a replaceNode request.",
        Collections.emptyList(),
        replicas);
    int sizeA = collection.getReplicasOnNode(emptyNodes.get(0)).size();
    int sizeB = collection.getReplicasOnNode(emptyNodes.get(1)).size();
    assertEquals(
        "The empty nodes should have a similar number of replicas placed on each", sizeA, sizeB, 1);
    assertEquals(
        "The number of replicas on the two empty nodes should equal the number of replicas removed from the source node",
        initialReplicaCounts.get(0).intValue(),
        sizeA + sizeB);
    for (int i = 1; i < l.size(); i++) {
      assertEquals(
          "The number of replicas on non-empty and non-source nodes should not change",
          initialReplicaCounts.get(i).intValue(),
          collection.getReplicasOnNode(l.get(i)).size());
    }
  }

  @Test
  public void testFailOnSingleNode() throws Exception {
    configureCluster(1)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "replacesinglenodetest_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    cloudClient.request(CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 0, 0));

    cluster.waitForActiveCollection(coll, 5, 5);

    String liveNode = cloudClient.getClusterState().getLiveNodes().iterator().next();
    expectThrows(
        SolrException.class,
        () -> createReplaceNodeRequest(liveNode, null, null).process(cloudClient));
  }

  @Test
  public void testFailIfSourceIsSameAsTarget() throws Exception {
    configureCluster(2)
        .addConfig(
            "conf1", TEST_PATH().resolve("configsets").resolve("cloud-dynamic").resolve("conf"))
        .configure();
    String coll = "replacesourceissameastarget_coll";
    if (log.isInfoEnabled()) {
      log.info("total_jettys: {}", cluster.getJettySolrRunners().size());
    }

    CloudSolrClient cloudClient = cluster.getSolrClient();
    cloudClient.request(CollectionAdminRequest.createCollection(coll, "conf1", 5, 1, 0, 0));

    cluster.waitForActiveCollection(coll, 5, 5);

    String liveNode = cloudClient.getClusterState().getLiveNodes().iterator().next();
    expectThrows(
        SolrException.class,
        () -> createReplaceNodeRequest(liveNode, liveNode, null).process(cloudClient));
  }

  public static CollectionAdminRequest.AsyncCollectionAdminRequest createReplaceNodeRequest(
      String sourceNode, String targetNode, Boolean parallel) {
    if (random().nextBoolean()) {
      return new CollectionAdminRequest.ReplaceNode(sourceNode, targetNode).setParallel(parallel);
    } else {
      // test back compat with old param names
      // todo remove in solr 8.0
      return new CollectionAdminRequest.AsyncCollectionAdminRequest(
          CollectionParams.CollectionAction.REPLACENODE) {
        @Override
        public SolrParams getParams() {
          ModifiableSolrParams params = (ModifiableSolrParams) super.getParams();
          params.set(SOURCE_NODE, sourceNode);
          if (targetNode != null && !targetNode.isEmpty()) {
            params.setNonNull(TARGET_NODE, targetNode);
          }
          if (parallel != null) params.set("parallel", parallel.toString());
          return params;
        }
      };
    }
  }
}
