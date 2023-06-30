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

package org.apache.solr.cluster.placement.plugins;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.DeleteReplicasRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.impl.BalanceRequestImpl;
import org.apache.solr.cluster.placement.impl.ModificationRequestImpl;
import org.apache.solr.cluster.placement.impl.PlacementRequestImpl;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit test for {@link AffinityPlacementFactory} */
public class AffinityPlacementFactoryTest extends AbstractPlacementFactoryTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PlacementPlugin plugin;

  private static final long MINIMAL_FREE_DISK_GB = 10L;
  private static final long PRIORITIZED_FREE_DISK_GB = 50L;
  private static final String secondaryCollectionName = "withCollection_secondary";
  private static final String primaryCollectionName = "withCollection_primary";

  static AffinityPlacementConfig defaultConfig =
      new AffinityPlacementConfig(MINIMAL_FREE_DISK_GB, PRIORITIZED_FREE_DISK_GB);

  @Before
  public void setupPlugin() {
    configurePlugin(defaultConfig);
  }

  private void configurePlugin(AffinityPlacementConfig config) {
    AffinityPlacementFactory factory = new AffinityPlacementFactory();
    factory.configure(config);
    plugin = factory.createPluginInstance();
  }

  @Test
  public void testBasicPlacementNewCollection() throws Exception {
    testBasicPlacementInternal(false);
  }

  @Test
  public void testBasicPlacementExistingCollection() throws Exception {
    testBasicPlacementInternal(true);
  }

  /**
   * When this test places a replica for a new collection, it should pick the node with fewer cores.
   *
   * <p>
   *
   * <p>When it places a replica for an existing collection, it should pick the node with fewer
   * cores that doesn't already have a replica for the shard.
   */
  private void testBasicPlacementInternal(boolean hasExistingCollection) throws Exception {
    String collectionName = "basicCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    nodeBuilders.get(0).setCoreCount(1).setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1));
    nodeBuilders.get(1).setCoreCount(10).setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1));

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);

    if (hasExistingCollection) {
      // Existing collection has replicas for its shards and is visible in the cluster state
      collectionBuilder.initializeShardsReplicas(1, 1, 0, 0, nodeBuilders);
      clusterBuilder.addCollection(collectionBuilder);
    } else {
      // New collection to create has the shards defined but no replicas and is not present in
      // cluster state
      collectionBuilder.initializeShardsReplicas(1, 0, 0, 0, List.of());
    }

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();

    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Place a new replica for the (only) existing shard of the collection
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection,
            Set.of(solrCollection.shards().iterator().next().getShardName()),
            new HashSet<>(liveNodes),
            1,
            0,
            0);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);

    assertEquals(1, pp.getReplicaPlacements().size());
    ReplicaPlacement rp = pp.getReplicaPlacements().iterator().next();
    assertEquals(hasExistingCollection ? liveNodes.get(1) : liveNodes.get(0), rp.getNode());
  }

  /** Test not placing replicas on nodes low free disk unless no other option */
  @Test
  public void testLowSpaceNode() throws Exception {
    String collectionName = "lowSpaceCollection";

    final int LOW_SPACE_NODE_INDEX = 0;
    final int NO_SPACE_NODE_INDEX = 1;

    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(8);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    for (int i = 0; i < nodeBuilders.size(); i++) {
      if (i == LOW_SPACE_NODE_INDEX) {
        nodeBuilders
            .get(i)
            .setCoreCount(1)
            .setFreeDiskGB((double) (MINIMAL_FREE_DISK_GB + 1)); // Low space
      } else if (i == NO_SPACE_NODE_INDEX) {
        nodeBuilders.get(i).setCoreCount(10).setFreeDiskGB(1.0); // Really not enough space
      } else {
        nodeBuilders.get(i).setCoreCount(10).setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1));
      }
    }
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // The collection to create (shards are defined but no replicas)
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(3, 0, 0, 0, List.of());
    SolrCollection solrCollection = collectionBuilder.build();

    // Place two replicas of each type for each shard
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 2, 2, 2);

    PlacementPlan pp =
        plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());

    assertEquals(18, pp.getReplicaPlacements().size()); // 3 shards, 6 replicas total each
    Set<Pair<String, Node>> placements = new HashSet<>();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      assertTrue(
          "two replicas for same shard placed on same node",
          placements.add(new Pair<>(rp.getShardName(), rp.getNode())));
      assertNotEquals(
          "Replica unnecessarily placed on node with low free space",
          rp.getNode(),
          liveNodes.get(LOW_SPACE_NODE_INDEX));
      assertNotEquals(
          "Replica placed on node with not enough free space",
          rp.getNode(),
          liveNodes.get(NO_SPACE_NODE_INDEX));
    }

    // Verify that if we ask for 7 replicas, the placement will use the low free space node
    placementRequest =
        new PlacementRequestImpl(
            solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 7, 0, 0);
    pp = plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());
    assertEquals(21, pp.getReplicaPlacements().size()); // 3 shards, 7 replicas each
    placements = new HashSet<>();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      assertEquals(
          "Only NRT replicas should be created", Replica.ReplicaType.NRT, rp.getReplicaType());
      assertTrue(
          "two replicas for same shard placed on same node",
          placements.add(new Pair<>(rp.getShardName(), rp.getNode())));
      assertNotEquals(
          "Replica placed on node with not enough free space",
          rp.getNode(),
          liveNodes.get(NO_SPACE_NODE_INDEX));
    }

    // Verify that if we ask for 8 replicas, the placement fails
    try {
      placementRequest =
          new PlacementRequestImpl(
              solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 8, 0, 0);
      plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());
      fail("Placing 8 replicas should not be possible given only 7 nodes have enough space");
    } catch (PlacementException e) {
      // expected
    }
  }

  /**
   * Tests that existing collection replicas are taken into account when preventing more than one
   * replica per shard to be placed on any node.
   */
  @Test
  public void testPlacementWithExistingReplicas() throws Exception {
    String collectionName = "existingCollection";

    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(5);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    int coresOnNode = 10;
    for (Builders.NodeBuilder nodeBuilder : nodeBuilders) {
      nodeBuilder.setCoreCount(coresOnNode).setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1));
      coresOnNode += 10;
    }

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    // Note that the collection as defined below is in a state that would NOT be returned by the
    // placement plugin: shard 1 has two replicas on node 0. The plugin should still be able to
    // place additional replicas as long as they don't break the rules.
    List<List<String>> shardsReplicas =
        List.of(
            List.of("NRT 0", "TLOG 0", "NRT 3"), // shard 1
            List.of("NRT 1", "NRT 3", "TLOG 2")); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);
    SolrCollection solrCollection = collectionBuilder.build();

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Place an additional NRT and an additional TLOG replica for each shard
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 1, 1, 0);

    // The replicas must be placed on the most appropriate nodes, i.e. those that do not already
    // have a replica for the shard and then on the node with the lowest number of cores. NRT are
    // placed first and given the cluster state here the placement is deterministic (easier to test,
    // only one good placement).
    PlacementPlan pp =
        plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());

    // Each expected placement is represented as a string "shard replica-type node"
    Set<String> expectedPlacements = Set.of("1 NRT 1", "1 TLOG 2", "2 NRT 0", "2 TLOG 4");
    verifyPlacements(expectedPlacements, pp, collectionBuilder.getShardBuilders(), liveNodes);
  }

  /**
   * Tests placement with multiple criteria: Replica type restricted nodes, Availability zones +
   * existing collection
   */
  @Test
  public void testPlacementMultiCriteria() throws Exception {
    String collectionName = "multiCollection";

    // Note node numbering is in purpose not following AZ structure
    final int AZ1_NRT_LOWCORES = 0;
    final int AZ1_NRT_HIGHCORES = 3;
    final int AZ1_TLOGPULL_LOWFREEDISK = 5;

    final int AZ2_NRT_MEDCORES = 2;
    final int AZ2_NRT_HIGHCORES = 1;
    final int AZ2_TLOGPULL = 7;

    final int AZ3_NRT_LOWCORES = 4;
    // final int AZ3_NRT_HIGHCORES = 6;
    final int AZ3_TLOGPULL = 8;

    final String AZ1 = "AZ1";
    final String AZ2 = "AZ2";
    final String AZ3 = "AZ3";

    final int LOW_CORES = 10;
    final int MED_CORES = 50;
    final int HIGH_CORES = 100;

    final String TLOG_PULL_REPLICA_TYPE = "TLOG, PULL";
    final String NRT_REPLICA_TYPE = "Nrt";

    // Cluster nodes and their attributes.
    // 3 AZ's with three nodes each, 2 of which can only take NRT, one that can take TLOG or PULL
    // One of the NRT has fewer cores than the other
    // The TLOG/PULL replica on AZ1 doesn't have much free disk space
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(9);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    for (int i = 0; i < 9; i++) {
      final String az;
      final int numcores;
      final double freedisk;
      final String acceptedReplicaType;

      if (i == AZ1_NRT_LOWCORES || i == AZ1_NRT_HIGHCORES || i == AZ1_TLOGPULL_LOWFREEDISK) {
        az = AZ1;
      } else if (i == AZ2_NRT_HIGHCORES || i == AZ2_NRT_MEDCORES || i == AZ2_TLOGPULL) {
        az = AZ2;
      } else {
        az = AZ3;
      }

      if (i == AZ1_NRT_LOWCORES || i == AZ3_NRT_LOWCORES) {
        numcores = LOW_CORES;
      } else if (i == AZ2_NRT_MEDCORES) {
        numcores = MED_CORES;
      } else {
        numcores = HIGH_CORES;
      }

      if (i == AZ1_TLOGPULL_LOWFREEDISK) {
        freedisk = PRIORITIZED_FREE_DISK_GB - 10;
      } else {
        freedisk = PRIORITIZED_FREE_DISK_GB + 10;
      }

      if (i == AZ1_TLOGPULL_LOWFREEDISK || i == AZ2_TLOGPULL || i == AZ3_TLOGPULL) {
        acceptedReplicaType = TLOG_PULL_REPLICA_TYPE;
      } else {
        acceptedReplicaType = NRT_REPLICA_TYPE;
      }

      nodeBuilders
          .get(i)
          .setSysprop(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP, az)
          .setSysprop(AffinityPlacementConfig.REPLICA_TYPE_SYSPROP, acceptedReplicaType)
          .setCoreCount(numcores)
          .setFreeDiskGB(freedisk);
    }

    // The collection already exists with shards and replicas.
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    List<List<String>> shardsReplicas =
        List.of(
            List.of("NRT " + AZ1_NRT_HIGHCORES, "TLOG " + AZ3_TLOGPULL), // shard 1
            List.of("TLOG " + AZ2_TLOGPULL)); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);
    SolrCollection solrCollection = collectionBuilder.build();

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Add 2 NRT and one TLOG to each shard.
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 2, 1, 0);
    PlacementPlan pp =
        plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());
    // Shard 1: The NRT's should go to the med cores node on AZ2 and low core on az3 (even though a
    // low core node can take the replica in az1, there's already an NRT replica there, and we want
    // spreading across AZ's), the TLOG to the TLOG node on AZ2 (because the tlog node on AZ1 has
    // low free disk)
    // Shard 2: The NRT's should go to AZ1 and AZ3 low cores because AZ2 has more cores (and there's
    // not NRT in any AZ for this shard). The TLOG should go to AZ3 because AZ1 TLOG node has low
    // free disk. Each expected placement is represented as a string "shard replica-type node"
    Set<String> expectedPlacements =
        Set.of(
            "1 NRT " + AZ2_NRT_MEDCORES,
            "1 NRT " + AZ3_NRT_LOWCORES,
            "1 TLOG " + AZ2_TLOGPULL,
            "2 NRT " + AZ1_NRT_LOWCORES,
            "2 NRT " + AZ3_NRT_LOWCORES,
            "2 TLOG " + AZ3_TLOGPULL);
    verifyPlacements(expectedPlacements, pp, collectionBuilder.getShardBuilders(), liveNodes);

    // If we add instead 2 PULL replicas to each shard
    placementRequest =
        new PlacementRequestImpl(
            solrCollection, solrCollection.getShardNames(), new HashSet<>(liveNodes), 0, 0, 2);
    pp = plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());
    // Shard 1: Given node AZ3_TLOGPULL is taken by the TLOG replica, the PULL should go to
    // AZ1_TLOGPULL_LOWFREEDISK and AZ2_TLOGPULL
    // Shard 2: Similarly AZ2_TLOGPULL is taken. Replicas should go to AZ1_TLOGPULL_LOWFREEDISK and
    // AZ3_TLOGPULL
    expectedPlacements =
        Set.of(
            "1 PULL " + AZ1_TLOGPULL_LOWFREEDISK,
            "1 PULL " + AZ2_TLOGPULL,
            "2 PULL " + AZ1_TLOGPULL_LOWFREEDISK,
            "2 PULL " + AZ3_TLOGPULL);
    verifyPlacements(expectedPlacements, pp, collectionBuilder.getShardBuilders(), liveNodes);
  }

  /**
   * Tests placement for new collection with nodes with a varying number of cores over multiple AZ's
   */
  @Test
  public void testPlacementAzsCores() throws Exception {
    String collectionName = "coresAzsCollection";

    // Count cores == node index, and AZ's are: AZ0, AZ0, AZ0, AZ1, AZ1, AZ1, AZ2, AZ2, AZ2.
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(9);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    for (int i = 0; i < 9; i++) {
      nodeBuilders
          .get(i)
          .setSysprop(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP, "AZ" + (i / 3))
          .setCoreCount(i)
          .setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 10));
    }

    // The collection does not exist, has 1 shard.
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    List<List<String>> shardsReplicas = List.of(List.of());
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);
    SolrCollection solrCollection = collectionBuilder.build();

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Test placing between 1 and 9 NRT replicas. check that it's done in order
    List<Set<String>> placements =
        List.of(
            Set.of("1 NRT 0"),
            Set.of("1 NRT 0", "1 NRT 3"),
            Set.of("1 NRT 0", "1 NRT 3", "1 NRT 6"),
            Set.of("1 NRT 0", "1 NRT 3", "1 NRT 6", "1 NRT 1"),
            Set.of("1 NRT 0", "1 NRT 3", "1 NRT 6", "1 NRT 1", "1 NRT 4"),
            Set.of("1 NRT 0", "1 NRT 3", "1 NRT 6", "1 NRT 1", "1 NRT 4", "1 NRT 7"),
            Set.of("1 NRT 0", "1 NRT 3", "1 NRT 6", "1 NRT 1", "1 NRT 4", "1 NRT 7", "1 NRT 2"),
            Set.of(
                "1 NRT 0", "1 NRT 3", "1 NRT 6", "1 NRT 1", "1 NRT 4", "1 NRT 7", "1 NRT 2",
                "1 NRT 5"),
            Set.of(
                "1 NRT 0", "1 NRT 3", "1 NRT 6", "1 NRT 1", "1 NRT 4", "1 NRT 7", "1 NRT 2",
                "1 NRT 5", "1 NRT 8"));

    for (int countNrtToPlace = 1; countNrtToPlace <= 9; countNrtToPlace++) {
      PlacementRequestImpl placementRequest =
          new PlacementRequestImpl(
              solrCollection,
              solrCollection.getShardNames(),
              new HashSet<>(liveNodes),
              countNrtToPlace,
              0,
              0);
      PlacementPlan pp =
          plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());
      verifyPlacements(
          placements.get(countNrtToPlace - 1), pp, collectionBuilder.getShardBuilders(), liveNodes);
    }
  }

  /**
   * Tests that if a collection has replicas on nodes not currently live, placement for new replicas
   * works ok.
   */
  @Test
  public void testCollectionOnDeadNodes() throws Exception {
    String collectionName = "walkingDead";

    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    int coreCount = 0;
    for (Builders.NodeBuilder nodeBuilder : nodeBuilders) {
      nodeBuilder.setCoreCount(coreCount++).setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1));
    }

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    // The collection below has shard 1 having replicas only on dead nodes and shard 2 no replicas
    // at all... (which is likely a challenging condition to recover from, but the placement
    // computations should still execute happily).
    List<List<String>> shardsReplicas =
        List.of(
            List.of("NRT 10", "TLOG 11"), // shard 1
            List.of()); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);
    SolrCollection solrCollection = collectionBuilder.build();

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    // Place an additional PULL replica for shard 1
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection,
            Set.of(solrCollection.iterator().next().getShardName()),
            new HashSet<>(liveNodes),
            0,
            0,
            1);

    PlacementPlan pp =
        plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());

    // Each expected placement is represented as a string "shard replica-type node"
    // Node 0 has fewer cores than node 1 (0 vs 1) so the placement should go there.
    Set<String> expectedPlacements = Set.of("1 PULL 0");
    verifyPlacements(expectedPlacements, pp, collectionBuilder.getShardBuilders(), liveNodes);

    // If we placed instead a replica for shard 2 (starting with the same initial cluster state, not
    // including the first placement above), it should go too to node 0 since it has fewer cores...
    Iterator<Shard> it = solrCollection.iterator();
    it.next(); // skip first shard to do placement for the second one...
    placementRequest =
        new PlacementRequestImpl(
            solrCollection, Set.of(it.next().getShardName()), new HashSet<>(liveNodes), 0, 0, 1);
    pp = plugin.computePlacement(placementRequest, clusterBuilder.buildPlacementContext());
    expectedPlacements = Set.of("2 PULL 0");
    verifyPlacements(expectedPlacements, pp, collectionBuilder.getShardBuilders(), liveNodes);
  }

  @Test
  public void testAvailabilityZones() throws Exception {
    String collectionName = "azCollection";
    int NUM_NODES = 6;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getLiveNodeBuilders().get(i);
      nodeBuilder.setCoreCount(0);
      nodeBuilder.setFreeDiskGB(100.0);
      if (i < NUM_NODES / 2) {
        nodeBuilder.setSysprop(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP, "az1");
      } else {
        nodeBuilder.setSysprop(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP, "az2");
      }
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection,
            StreamSupport.stream(solrCollection.shards().spliterator(), false)
                .map(Shard::getShardName)
                .collect(Collectors.toSet()),
            cluster.getLiveNodes(),
            2,
            2,
            2);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    // 2 shards, 6 replicas
    assertEquals(12, pp.getReplicaPlacements().size());
    // shard -> AZ -> replica count
    Map<Replica.ReplicaType, Map<String, Map<String, AtomicInteger>>> replicas = new HashMap<>();
    AttributeValues attributeValues = placementContext.getAttributeFetcher().fetchAttributes();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      Optional<String> azOptional =
          attributeValues.getSystemProperty(
              rp.getNode(), AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP);
      if (!azOptional.isPresent()) {
        fail("missing AZ sysprop for node " + rp.getNode());
      }
      String az = azOptional.get();
      replicas
          .computeIfAbsent(rp.getReplicaType(), type -> new HashMap<>())
          .computeIfAbsent(rp.getShardName(), shard -> new HashMap<>())
          .computeIfAbsent(az, zone -> new AtomicInteger())
          .incrementAndGet();
    }
    replicas.forEach(
        (type, perTypeReplicas) -> {
          perTypeReplicas.forEach(
              (shard, azCounts) -> {
                assertEquals("number of AZs", 2, azCounts.size());
                azCounts.forEach(
                    (az, count) -> {
                      assertTrue(
                          "too few replicas shard=" + shard + ", type=" + type + ", az=" + az,
                          count.get() >= 1);
                    });
              });
        });
  }

  @Test
  public void testReplicaType() throws Exception {
    String collectionName = "replicaTypeCollection";
    int NUM_NODES = 6;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getLiveNodeBuilders().get(i);
      nodeBuilder.setCoreCount(0);
      nodeBuilder.setFreeDiskGB(100.0);
      if (i < NUM_NODES / 3 * 2) {
        nodeBuilder.setSysprop(AffinityPlacementConfig.REPLICA_TYPE_SYSPROP, "Nrt, TlOg");
        nodeBuilder.setSysprop("group", "one");
      } else {
        nodeBuilder.setSysprop(AffinityPlacementConfig.REPLICA_TYPE_SYSPROP, "Pull,foobar");
        nodeBuilder.setSysprop("group", "two");
      }
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection,
            StreamSupport.stream(solrCollection.shards().spliterator(), false)
                .map(Shard::getShardName)
                .collect(Collectors.toSet()),
            cluster.getLiveNodes(),
            2,
            2,
            2);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    // 2 shards, 6 replicas
    assertEquals(12, pp.getReplicaPlacements().size());
    // shard -> group -> replica count
    Map<Replica.ReplicaType, Map<String, Map<String, AtomicInteger>>> replicas = new HashMap<>();
    AttributeValues attributeValues = placementContext.getAttributeFetcher().fetchAttributes();
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      Optional<String> groupOptional = attributeValues.getSystemProperty(rp.getNode(), "group");
      if (!groupOptional.isPresent()) {
        fail("missing group sysprop for node " + rp.getNode());
      }
      String group = groupOptional.get();
      if (group.equals("one")) {
        assertTrue(
            "wrong replica type in group one",
            (rp.getReplicaType() == Replica.ReplicaType.NRT)
                || rp.getReplicaType() == Replica.ReplicaType.TLOG);
      } else {
        assertEquals(
            "wrong replica type in group two", Replica.ReplicaType.PULL, rp.getReplicaType());
      }
      replicas
          .computeIfAbsent(rp.getReplicaType(), type -> new HashMap<>())
          .computeIfAbsent(rp.getShardName(), shard -> new HashMap<>())
          .computeIfAbsent(group, g -> new AtomicInteger())
          .incrementAndGet();
    }
    replicas.forEach(
        (type, perTypeReplicas) -> {
          perTypeReplicas.forEach(
              (shard, groupCounts) -> {
                assertEquals("number of groups", 1, groupCounts.size());
                groupCounts.forEach(
                    (group, count) -> {
                      assertTrue(
                          "too few replicas shard=" + shard + ", type=" + type + ", group=" + group,
                          count.get() >= 1);
                    });
              });
        });
  }

  @Test
  public void testFreeDiskConstraints() throws Exception {
    String collectionName = "freeDiskCollection";
    int NUM_NODES = 3;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    Node smallNode = null;
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getLiveNodeBuilders().get(i);
      nodeBuilder.setCoreCount(0);
      if (i == 0) {
        // default minimalFreeDiskGB == 20
        nodeBuilder.setFreeDiskGB(1.0);
        smallNode = nodeBuilder.build();
      } else {
        nodeBuilder.setFreeDiskGB(100.0);
      }
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(2, 0, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection,
            StreamSupport.stream(solrCollection.shards().spliterator(), false)
                .map(Shard::getShardName)
                .collect(Collectors.toSet()),
            cluster.getLiveNodes(),
            1,
            0,
            1);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    assertEquals(4, pp.getReplicaPlacements().size());
    for (ReplicaPlacement rp : pp.getReplicaPlacements()) {
      assertNotEquals("should not put any replicas on " + smallNode, rp.getNode(), smallNode);
    }
  }

  @Test
  public void testFreeDiskConstraintsWithNewReplicas() throws Exception {
    String collectionName = "freeDiskWithReplicasCollection";
    int NUM_NODES = 3;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    Node smallNode = null;
    for (int i = 0; i < NUM_NODES; i++) {
      Builders.NodeBuilder nodeBuilder = clusterBuilder.getLiveNodeBuilders().get(i);
      // Act as if the two replicas were placed on nodes 1 and 2
      nodeBuilder.setCoreCount(0);
      nodeBuilder.setFreeDiskGB(100.0);
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(
        3,
        1,
        0,
        0,
        clusterBuilder.getLiveNodeBuilders(), // .subList(1, 3),
        List.of(33, 33, 60));
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection solrCollection = cluster.getCollection(collectionName);

    // Test when an additional replicaType makes the projected indexSize go over the limit
    // Add two replicas (different types) to the first shard, the second replica should fail
    PlacementRequestImpl badReplicaPlacementRequest =
        new PlacementRequestImpl(
            solrCollection,
            StreamSupport.stream(solrCollection.shards().spliterator(), false)
                .map(Shard::getShardName)
                .findFirst()
                .map(Set::of)
                .orElseGet(Collections::emptySet),
            cluster.getLiveNodes(),
            0,
            1,
            1);

    assertThrows(
        PlacementException.class,
        () -> plugin.computePlacement(badReplicaPlacementRequest, placementContext));

    // Test when an additional shard makes the projected indexSize go over the limit
    // Add one replica to each shard, the third shard should fail
    PlacementRequest badShardPlacementRequest =
        new PlacementRequestImpl(
            solrCollection,
            StreamSupport.stream(solrCollection.shards().spliterator(), false)
                .map(Shard::getShardName)
                .collect(Collectors.toSet()),
            cluster.getLiveNodes(),
            0,
            1,
            1);

    assertThrows(
        PlacementException.class,
        () -> plugin.computePlacement(badShardPlacementRequest, placementContext));
  }

  @Test(expected = SolrException.class)
  public void testWithCollectionDisjointWithShards() {
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of());
    configurePlugin(config);
  }

  @Test
  public void testWithCollectionPlacement() throws Exception {
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of());
    configurePlugin(config);

    int NUM_NODES = 3;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    Builders.CollectionBuilder collectionBuilder =
        Builders.newCollectionBuilder(secondaryCollectionName);
    collectionBuilder.initializeShardsReplicas(1, 2, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    collectionBuilder = Builders.newCollectionBuilder(primaryCollectionName);
    collectionBuilder.initializeShardsReplicas(0, 0, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection secondaryCollection = cluster.getCollection(secondaryCollectionName);
    SolrCollection primaryCollection = cluster.getCollection(primaryCollectionName);

    Set<Node> secondaryNodes = new HashSet<>();
    secondaryCollection
        .shards()
        .forEach(s -> s.replicas().forEach(r -> secondaryNodes.add(r.getNode())));

    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            primaryCollection, Set.of("shard1", "shard2"), cluster.getLiveNodes(), 1, 0, 0);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    assertEquals(2, pp.getReplicaPlacements().size());
    // verify that all placements are on nodes with the secondary replica
    pp.getReplicaPlacements()
        .forEach(
            placement ->
                assertTrue(
                    "placement node " + placement.getNode() + " not in secondary=" + secondaryNodes,
                    secondaryNodes.contains(placement.getNode())));

    placementRequest =
        new PlacementRequestImpl(
            primaryCollection, Set.of("shard1"), cluster.getLiveNodes(), 3, 0, 0);
    try {
      pp = plugin.computePlacement(placementRequest, placementContext);
      fail("should generate 'Not enough eligible nodes' failure here");
    } catch (PlacementException pe) {
      assertTrue(pe.toString().contains("Not enough eligible nodes"));
    }
  }

  @Test
  public void testWithCollectionShardsPlacement() throws Exception {
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(),
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of());
    configurePlugin(config);

    int NUM_NODES = 3;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    Builders.CollectionBuilder collectionBuilder =
        Builders.newCollectionBuilder(secondaryCollectionName);
    collectionBuilder.initializeShardsReplicas(2, 1, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    collectionBuilder = Builders.newCollectionBuilder(primaryCollectionName);
    collectionBuilder.initializeShardsReplicas(0, 0, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection secondaryCollection = cluster.getCollection(secondaryCollectionName);
    SolrCollection primaryCollection = cluster.getCollection(primaryCollectionName);

    Set<Node> secondaryNodes = new HashSet<>();
    secondaryCollection
        .shards()
        .forEach(s -> s.replicas().forEach(r -> secondaryNodes.add(r.getNode())));

    final List<Node> liveNodes = new ArrayList<>(cluster.getLiveNodes());
    Collections.shuffle(liveNodes, random());
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            primaryCollection,
            shuffle(Arrays.asList("shard2", "shard1")),
            shuffle(cluster.getLiveNodes()),
            1,
            0,
            0);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    assertEquals(2, pp.getReplicaPlacements().size());
    // verify that all placements are on nodes with the secondary replica
    pp.getReplicaPlacements()
        .forEach(
            placement -> {
              assertTrue(
                  "placement node " + placement.getNode() + " not in secondary=" + secondaryNodes,
                  secondaryNodes.contains(placement.getNode()));
              boolean collocated = false;
              final Shard shard = secondaryCollection.getShard(placement.getShardName());
              StringBuilder msg = new StringBuilder();
              for (Iterator<Replica> secReplicas = shard.iterator();
                  secReplicas.hasNext() && !collocated; ) {
                final Replica secReplica = secReplicas.next();
                collocated |= placement.getNode().getName().equals(secReplica.getNode().getName());
                msg.append(secReplica.getReplicaName());
                msg.append("@");
                msg.append(secReplica.getNode().getName());
                msg.append(", ");
              }
              assertTrue(placement + " is expected to be collocated with " + msg, collocated);
            });

    placementRequest =
        new PlacementRequestImpl(
            primaryCollection, Set.of("shard3"), cluster.getLiveNodes(), 1, 0, 0);
    try {
      pp = plugin.computePlacement(placementRequest, placementContext);
      fail("should generate 'has no replicas on eligible nodes' failure here");
    } catch (PlacementException pe) {
      assertTrue(pe.toString(), pe.toString().contains("Not enough eligible nodes"));
    }
  }

  private <T> Set<T> shuffle(Set<T> liveNodes) {
    final List<T> nodes = new ArrayList<>(liveNodes);
    return shuffle(nodes);
  }

  private static <T> Set<T> shuffle(List<T> nodes) {
    Collections.shuffle(nodes, random());
    return new LinkedHashSet<>(nodes);
  }

  @Test
  public void testWithCollectionModificationRejected() throws Exception {
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of());
    configurePlugin(config);

    int NUM_NODES = 2;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    Builders.CollectionBuilder collectionBuilder =
        Builders.newCollectionBuilder(secondaryCollectionName);
    collectionBuilder.initializeShardsReplicas(1, 4, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    collectionBuilder = Builders.newCollectionBuilder(primaryCollectionName);
    collectionBuilder.initializeShardsReplicas(2, 2, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection secondaryCollection = cluster.getCollection(secondaryCollectionName);
    SolrCollection primaryCollection = cluster.getCollection(primaryCollectionName);

    Node node = cluster.getLiveNodes().iterator().next();
    Set<Replica> secondaryReplicas = new HashSet<>();
    secondaryCollection
        .shards()
        .forEach(
            shard ->
                shard
                    .replicas()
                    .forEach(
                        replica -> {
                          if (secondaryReplicas.size() < 1 && replica.getNode().equals(node)) {
                            secondaryReplicas.add(replica);
                          }
                        }));

    DeleteReplicasRequest deleteReplicasRequest =
        ModificationRequestImpl.createDeleteReplicasRequest(secondaryCollection, secondaryReplicas);
    try {
      plugin.verifyAllowedModification(deleteReplicasRequest, placementContext);
    } catch (PlacementException pe) {
      fail("should have succeeded: " + pe);
    }

    secondaryCollection
        .shards()
        .forEach(
            shard ->
                shard
                    .replicas()
                    .forEach(
                        replica -> {
                          if (secondaryReplicas.size() < 2 && replica.getNode().equals(node)) {
                            secondaryReplicas.add(replica);
                          }
                        }));

    deleteReplicasRequest =
        ModificationRequestImpl.createDeleteReplicasRequest(secondaryCollection, secondaryReplicas);
    try {
      plugin.verifyAllowedModification(deleteReplicasRequest, placementContext);
      fail("should have failed: " + deleteReplicasRequest);
    } catch (PlacementException pe) {
    }
  }

  @Test
  public void testWithCollectionShardsModificationRejected() throws Exception {
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(),
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of());
    configurePlugin(config);

    int NUM_NODES = 2;
    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(NUM_NODES);
    Builders.CollectionBuilder collectionBuilder =
        Builders.newCollectionBuilder(secondaryCollectionName);
    collectionBuilder.initializeShardsReplicas(2, 3, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    collectionBuilder = Builders.newCollectionBuilder(primaryCollectionName);
    collectionBuilder.initializeShardsReplicas(
        2, random().nextBoolean() ? 1 : 2, 0, 0, clusterBuilder.getLiveNodeBuilders());
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    SolrCollection secondaryCollection = cluster.getCollection(secondaryCollectionName);
    SolrCollection primaryCollection = cluster.getCollection(primaryCollectionName);

    final ArrayList<Node> nodes = new ArrayList<>(cluster.getLiveNodes());
    Collections.shuffle(nodes, random());
    Set<Replica> toRemove = new HashSet<>();
    DeleteReplicasRequest deleteReplicasRequest;
    for (Node node : nodes) {
      Set<String> seen = new HashSet<>();
      final Set<String> mustHaveShards =
          replicas(primaryCollection)
              .filter(r -> r.getNode().getName().equals(node.getName()))
              .map(r -> r.getShard().getShardName())
              .collect(Collectors.toSet());

      replicas(secondaryCollection)
          .filter(r -> r.getNode().getName().equals(node.getName()))
          .forEach(
              r -> {
                final String secRepShard = r.getShard().getShardName();
                if (mustHaveShards.contains(secRepShard)) {
                  if (seen.contains(secRepShard)) {
                    toRemove.add(r);
                  } else {
                    seen.add(secRepShard);
                  }
                } else {
                  toRemove.add(r);
                }
              });

      assertFalse(toRemove.isEmpty());
      deleteReplicasRequest =
          ModificationRequestImpl.createDeleteReplicasRequest(secondaryCollection, toRemove);
      try {
        plugin.verifyAllowedModification(deleteReplicasRequest, placementContext);
      } catch (PlacementException pe) {
        fail("should have succeeded: " + pe);
      }
    }
    final List<Replica> remainingReplicas =
        replicas(secondaryCollection)
            .filter(r -> !toRemove.contains(r))
            .collect(Collectors.toList());
    Collections.shuffle(remainingReplicas, random());
    toRemove.add(remainingReplicas.iterator().next());

    deleteReplicasRequest =
        ModificationRequestImpl.createDeleteReplicasRequest(secondaryCollection, toRemove);
    try {
      plugin.verifyAllowedModification(deleteReplicasRequest, placementContext);
      fail("should have failed: " + deleteReplicasRequest);
    } catch (PlacementException pe) {
    }
  }

  private Stream<Replica> replicas(SolrCollection primaryCollection) {
    return shards(primaryCollection).flatMap(shard -> replicas(shard));
  }

  private Stream<Replica> replicas(Shard shard) {
    return StreamSupport.stream(shard.replicas().spliterator(), false);
  }

  private static Stream<Shard> shards(SolrCollection primaryCollection) {
    return StreamSupport.stream(primaryCollection.shards().spliterator(), false);
  }

  @Test
  public void testNodeType() throws Exception {
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(9);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    for (int i = 0; i < 9; i++) {
      nodeBuilders.get(i).setSysprop(AffinityPlacementConfig.NODE_TYPE_SYSPROP, "type_" + (i % 3));
    }

    String collectionName = "nodeTypeCollection";
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(1, 0, 0, 0, clusterBuilder.getLiveNodeBuilders());

    // test single node type in collection
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(),
            Map.of(collectionName, "type_0"));
    configurePlugin(config);

    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Map<String, Set<String>> nodeNamesByType = new HashMap<>();
    AttributeValues attributeValues =
        placementContext
            .getAttributeFetcher()
            .requestNodeSystemProperty(AffinityPlacementConfig.NODE_TYPE_SYSPROP)
            .fetchAttributes();
    placementContext
        .getCluster()
        .getLiveNodes()
        .forEach(
            n ->
                nodeNamesByType
                    .computeIfAbsent(
                        attributeValues
                            .getSystemProperty(n, AffinityPlacementConfig.NODE_TYPE_SYSPROP)
                            .get(),
                        type -> new HashSet<>())
                    .add(n.getName()));
    SolrCollection collection = placementContext.getCluster().getCollection(collectionName);
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            collection, Set.of("shard1"), placementContext.getCluster().getLiveNodes(), 3, 0, 0);

    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    assertEquals("expected 3 placements: " + pp, 3, pp.getReplicaPlacements().size());
    Set<String> type0nodes = nodeNamesByType.get("type_0");
    Set<String> type1nodes = nodeNamesByType.get("type_1");
    // Set<String> type2nodes = nodeNamesByType.get("type_2");

    for (ReplicaPlacement p : pp.getReplicaPlacements()) {
      assertTrue(type0nodes.contains(p.getNode().getName()));
    }

    // test 2 node types in collection
    config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(),
            Map.of(collectionName, "type_0,type_1"));
    configurePlugin(config);

    placementContext = clusterBuilder.buildPlacementContext();
    collection = placementContext.getCluster().getCollection(collectionName);
    placementRequest =
        new PlacementRequestImpl(
            collection, Set.of("shard1"), placementContext.getCluster().getLiveNodes(), 6, 0, 0);

    pp = plugin.computePlacement(placementRequest, placementContext);
    assertEquals("expected 6 placements: " + pp, 6, pp.getReplicaPlacements().size());
    for (ReplicaPlacement p : pp.getReplicaPlacements()) {
      assertTrue(
          type0nodes.contains(p.getNode().getName()) || type1nodes.contains(p.getNode().getName()));
    }

    // test 2 node types in nodes
    for (int i = 0; i < 9; i++) {
      if (i < 3) {
        nodeBuilders.get(i).setSysprop(AffinityPlacementConfig.NODE_TYPE_SYSPROP, "type_0,type_1");
      } else if (i < 6) {
        nodeBuilders.get(i).setSysprop(AffinityPlacementConfig.NODE_TYPE_SYSPROP, "type_1,type_2");
      } else {
        nodeBuilders.get(i).setSysprop(AffinityPlacementConfig.NODE_TYPE_SYSPROP, "type_2");
      }
    }

    placementContext = clusterBuilder.buildPlacementContext();
    collection = placementContext.getCluster().getCollection(collectionName);
    placementRequest =
        new PlacementRequestImpl(
            collection, Set.of("shard1"), placementContext.getCluster().getLiveNodes(), 6, 0, 0);
    pp = plugin.computePlacement(placementRequest, placementContext);
    assertEquals("expected 6 placements: " + pp, 6, pp.getReplicaPlacements().size());
    nodeNamesByType.clear();
    AttributeValues attributeValues2 =
        placementContext
            .getAttributeFetcher()
            .requestNodeSystemProperty(AffinityPlacementConfig.NODE_TYPE_SYSPROP)
            .fetchAttributes();
    placementContext
        .getCluster()
        .getLiveNodes()
        .forEach(
            n -> {
              String nodeTypesStr =
                  attributeValues2
                      .getSystemProperty(n, AffinityPlacementConfig.NODE_TYPE_SYSPROP)
                      .get();
              for (String nodeType : StrUtils.splitSmart(nodeTypesStr, ',')) {
                nodeNamesByType.computeIfAbsent(nodeType, type -> new HashSet<>()).add(n.getName());
              }
            });
    type0nodes = nodeNamesByType.get("type_0");
    type1nodes = nodeNamesByType.get("type_1");

    for (ReplicaPlacement p : pp.getReplicaPlacements()) {
      assertTrue(
          type0nodes.contains(p.getNode().getName()) || type1nodes.contains(p.getNode().getName()));
    }
  }

  @Test
  public void testScalability() throws Exception {
    // for non-nightly we scale a bit, but retain test speed - for nightly test speed can be 2+
    // minutes

    int numShards = TEST_NIGHTLY ? 100 : 10;
    int nrtReplicas = TEST_NIGHTLY ? 40 : 4;
    int tlogReplicas = TEST_NIGHTLY ? 40 : 4;
    int pullReplicas = TEST_NIGHTLY ? 20 : 2;

    log.info("==== numNodes ====");
    runTestScalability(1000, numShards, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(2000, numShards, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(5000, numShards, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(10000, numShards, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(20000, numShards, nrtReplicas, tlogReplicas, pullReplicas, false);

    log.info("==== numShards ====");
    int numNodes = TEST_NIGHTLY ? 5000 : 500;
    runTestScalability(numNodes, 100, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(numNodes, 200, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(numNodes, 500, nrtReplicas, tlogReplicas, pullReplicas, false);
    if (TEST_NIGHTLY) {
      runTestScalability(numNodes, 1000, nrtReplicas, tlogReplicas, pullReplicas, false);
      runTestScalability(numNodes, 2000, nrtReplicas, tlogReplicas, pullReplicas, false);
    }

    log.info("==== numReplicas ====");
    runTestScalability(numNodes, numShards, TEST_NIGHTLY ? 100 : 10, 0, 0, false);
    runTestScalability(numNodes, numShards, TEST_NIGHTLY ? 200 : 20, 0, 0, false);
    runTestScalability(numNodes, numShards, TEST_NIGHTLY ? 500 : 50, 0, 0, false);
    runTestScalability(numNodes, numShards, TEST_NIGHTLY ? 1000 : 30, 0, 0, false);
    runTestScalability(numNodes, numShards, TEST_NIGHTLY ? 2000 : 50, 0, 0, false);

    log.info("==== spread domains ====");
    runTestScalability(numNodes, numShards, nrtReplicas, tlogReplicas, pullReplicas, false);
    runTestScalability(numNodes, numShards, nrtReplicas, tlogReplicas, pullReplicas, true);
  }

  private void runTestScalability(
      int numNodes,
      int numShards,
      int nrtReplicas,
      int tlogReplicas,
      int pullReplicas,
      boolean useSpreadDomains)
      throws Exception {
    String collectionName = "scaleCollection";

    if (useSpreadDomains) {
      defaultConfig.spreadAcrossDomains = true;
      configurePlugin(defaultConfig);
    } else {
      defaultConfig.spreadAcrossDomains = false;
      configurePlugin(defaultConfig);
    }

    int numSpreadDomains = 5;

    Builders.ClusterBuilder clusterBuilder =
        Builders.newClusterBuilder().initializeLiveNodes(numNodes);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    for (int i = 0; i < numNodes; i++) {
      nodeBuilders.get(i).setCoreCount(0).setFreeDiskGB((double) numNodes);
      nodeBuilders
          .get(i)
          .setSysprop(
              AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, String.valueOf(i % numSpreadDomains));
    }

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(numShards, 0, 0, 0, List.of());

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();
    Set<Node> setNodes = new HashSet<>(liveNodes);
    assertEquals(setNodes.size(), liveNodes.size());

    // Place replicas for all the shards of the (newly created since it has no replicas yet)
    // collection
    PlacementRequestImpl placementRequest =
        new PlacementRequestImpl(
            solrCollection,
            solrCollection.getShardNames(),
            new HashSet<>(liveNodes),
            nrtReplicas,
            tlogReplicas,
            pullReplicas);

    long start = System.nanoTime();
    PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);
    long end = System.nanoTime();

    final int REPLICAS_PER_SHARD = nrtReplicas + tlogReplicas + pullReplicas;
    final int TOTAL_REPLICAS = numShards * REPLICAS_PER_SHARD;

    log.info(
        "ComputePlacement: {} nodes, {} shards, {} total replicas, spread-domains={}, elapsed time {} ms.",
        numNodes,
        numShards,
        TOTAL_REPLICAS,
        useSpreadDomains,
        TimeUnit.NANOSECONDS.toMillis(end - start)); // nowarn
    assertEquals(
        "incorrect number of calculated placements",
        TOTAL_REPLICAS,
        pp.getReplicaPlacements().size());
    // check that replicas are correctly placed
    Map<Node, AtomicInteger> replicasPerNode = new HashMap<>();
    Map<Node, Set<String>> shardsPerNode = new HashMap<>();
    Map<String, AtomicInteger> replicasPerShard = new HashMap<>();
    Map<Replica.ReplicaType, AtomicInteger> replicasByType = new HashMap<>();
    for (ReplicaPlacement placement : pp.getReplicaPlacements()) {
      replicasPerNode
          .computeIfAbsent(placement.getNode(), n -> new AtomicInteger())
          .incrementAndGet();
      shardsPerNode
          .computeIfAbsent(placement.getNode(), n -> new HashSet<>())
          .add(placement.getShardName());
      replicasByType
          .computeIfAbsent(placement.getReplicaType(), t -> new AtomicInteger())
          .incrementAndGet();
      replicasPerShard
          .computeIfAbsent(placement.getShardName(), s -> new AtomicInteger())
          .incrementAndGet();
    }
    int perNode = TOTAL_REPLICAS > numNodes ? TOTAL_REPLICAS / numNodes : 1;
    replicasPerNode.forEach(
        (node, count) -> {
          assertEquals(
              "Wrong number of replicas for node: " + node.getName(), perNode, count.get());
        });
    shardsPerNode.forEach(
        (node, names) -> {
          assertEquals("Wrong number of shards for node: " + node.getName(), perNode, names.size());
        });

    replicasPerShard.forEach(
        (shard, count) -> {
          assertEquals(REPLICAS_PER_SHARD, count.get());
        });
  }

  @Test
  public void testAntiAffinityIsSoft() throws Exception {
    defaultConfig.spreadAcrossDomains = true;
    configurePlugin(defaultConfig);
    String collectionName = "basicCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(2);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    // Anti-affinity can't be achieved, since all nodes in the cluster have the same value
    nodeBuilders
        .get(0)
        .setCoreCount(1)
        .setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1))
        .setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "A");
    nodeBuilders
        .get(1)
        .setCoreCount(2)
        .setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1))
        .setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "A");

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(1, 0, 0, 0, List.of());

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();

    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    {
      // Place a new replica for the (only) existing shard of the collection
      PlacementRequestImpl placementRequest =
          new PlacementRequestImpl(
              solrCollection,
              Set.of(solrCollection.shards().iterator().next().getShardName()),
              new HashSet<>(liveNodes),
              1,
              0,
              0);

      PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);

      assertEquals(1, pp.getReplicaPlacements().size());
      ReplicaPlacement rp = pp.getReplicaPlacements().iterator().next();
      assertEquals(liveNodes.get(0), rp.getNode());
    }
    {
      // Place a new replica for the (only) existing shard of the collection
      PlacementRequestImpl placementRequest =
          new PlacementRequestImpl(
              solrCollection,
              Set.of(solrCollection.shards().iterator().next().getShardName()),
              new HashSet<>(liveNodes),
              2,
              0,
              0);

      PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);

      assertEquals(2, pp.getReplicaPlacements().size());
      assertEquals(
          Set.of(liveNodes.get(0), liveNodes.get(1)),
          pp.getReplicaPlacements().stream()
              .map(ReplicaPlacement::getNode)
              .collect(Collectors.toSet()));
    }
  }

  @Test
  public void testSpreadDomainsWithExistingCollection() throws Exception {
    testSpreadDomains(true);
  }

  @Test
  public void testSpreadDomainsWithEmptyCluster() throws Exception {
    testSpreadDomains(false);
  }

  private void testSpreadDomains(boolean hasExistingCollection) throws Exception {
    defaultConfig.spreadAcrossDomains = true;
    configurePlugin(defaultConfig);
    String collectionName = "basicCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    nodeBuilders
        .get(0)
        .setCoreCount(1)
        .setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1))
        .setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "A");
    nodeBuilders
        .get(1)
        .setCoreCount(2)
        .setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1))
        .setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "A");
    nodeBuilders
        .get(2)
        .setCoreCount(3)
        .setFreeDiskGB((double) (PRIORITIZED_FREE_DISK_GB + 1))
        .setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "B");

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);

    if (hasExistingCollection) {
      // Existing collection has replicas for its shards and is visible in the cluster state
      collectionBuilder.initializeShardsReplicas(1, 1, 0, 0, nodeBuilders);
      clusterBuilder.addCollection(collectionBuilder);
    } else {
      // New collection to create has the shards defined but no replicas and is not present in
      // cluster state
      collectionBuilder.initializeShardsReplicas(1, 0, 0, 0, List.of());
    }

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();

    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    {
      // Place a new replica for the (only) existing shard of the collection
      PlacementRequestImpl placementRequest =
          new PlacementRequestImpl(
              solrCollection,
              Set.of(solrCollection.shards().iterator().next().getShardName()),
              new HashSet<>(liveNodes),
              1,
              0,
              0);

      PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);

      assertEquals(1, pp.getReplicaPlacements().size());
      ReplicaPlacement rp = pp.getReplicaPlacements().iterator().next();
      assertEquals(hasExistingCollection ? liveNodes.get(2) : liveNodes.get(0), rp.getNode());
    }
    {
      // Place a new replica for the (only) existing shard of the collection
      PlacementRequestImpl placementRequest =
          new PlacementRequestImpl(
              solrCollection,
              Set.of(solrCollection.shards().iterator().next().getShardName()),
              new HashSet<>(liveNodes),
              2,
              0,
              0);

      PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);

      assertEquals(2, pp.getReplicaPlacements().size());
      assertEquals(
          hasExistingCollection
              ? Set.of(liveNodes.get(1), liveNodes.get(2))
              : Set.of(liveNodes.get(0), liveNodes.get(2)),
          pp.getReplicaPlacements().stream()
              .map(ReplicaPlacement::getNode)
              .collect(Collectors.toSet()));
    }
  }

  @Test
  public void testSpreadDomainsWithDownNode() throws Exception {
    defaultConfig.spreadAcrossDomains = true;
    defaultConfig.maxReplicasPerShardInDomain = -1;
    configurePlugin(defaultConfig);
    String collectionName = "basicCollection";

    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(3);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    nodeBuilders.get(0).setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "A");
    nodeBuilders.get(1).setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "B");
    nodeBuilders.get(2).setSysprop(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP, "A");

    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    collectionBuilder.initializeShardsReplicas(1, 2, 0, 0, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);
    clusterBuilder.getLiveNodeBuilders().remove(0);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    SolrCollection solrCollection = collectionBuilder.build();
    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    {
      // Place a new replica for the (only) existing shard of the collection
      PlacementRequestImpl placementRequest =
          new PlacementRequestImpl(
              solrCollection,
              Set.of(solrCollection.shards().iterator().next().getShardName()),
              new HashSet<>(liveNodes),
              1,
              0,
              0);

      PlacementPlan pp = plugin.computePlacement(placementRequest, placementContext);

      assertEquals(1, pp.getReplicaPlacements().size());
      ReplicaPlacement rp = pp.getReplicaPlacements().iterator().next();
      assertEquals(liveNodes.get(1), rp.getNode());
    }
  }

  /** Tests replica balancing across all nodes in a cluster */
  @Test
  public void testBalancingBareMetrics() throws Exception {
    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(5);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("a");
    // Note that the collection as defined below is in a state that would NOT be returned by the
    // placement plugin: shard 1 has two replicas on node 0. The plugin should still be able to
    // place additional replicas as long as they don't break the rules.
    List<List<String>> shardsReplicas =
        List.of(
            List.of("NRT 0", "TLOG 0", "NRT 2"), // shard 1
            List.of("NRT 1", "NRT 4", "TLOG 3")); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    // Add another collection
    collectionBuilder = Builders.newCollectionBuilder("b");
    shardsReplicas =
        List.of(
            List.of("NRT 1", "TLOG 0", "NRT 3"), // shard 1
            List.of("NRT 1", "NRT 3", "TLOG 0")); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    BalanceRequestImpl balanceRequest =
        new BalanceRequestImpl(new HashSet<>(clusterBuilder.buildLiveNodes()));
    BalancePlan balancePlan =
        plugin.computeBalancing(balanceRequest, clusterBuilder.buildPlacementContext());

    // Each expected placement is represented as a string "col shard replica-type fromNode ->
    // toNode"
    Set<String> expectedPlacements = Set.of("b 1 TLOG 0 -> 2", "b 1 NRT 3 -> 4");
    verifyBalancing(
        expectedPlacements,
        balancePlan,
        collectionBuilder.getShardBuilders(),
        clusterBuilder.buildLiveNodes());
  }

  /** Tests replica balancing across all nodes in a cluster */
  @Test
  public void testBalancingExistingMetrics() throws Exception {
    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(5);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    int coresOnNode = 10;
    for (Builders.NodeBuilder nodeBuilder : nodeBuilders) {
      nodeBuilder.setCoreCount(coresOnNode);
      coresOnNode += 10;
    }

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("a");
    // Note that the collection as defined below is in a state that would NOT be returned by the
    // placement plugin: shard 1 has two replicas on node 0. The plugin should still be able to
    // place additional replicas as long as they don't break the rules.
    List<List<String>> shardsReplicas =
        List.of(
            List.of("NRT 0", "TLOG 0", "NRT 3"), // shard 1
            List.of("NRT 1", "NRT 3", "TLOG 2")); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    BalanceRequestImpl balanceRequest =
        new BalanceRequestImpl(new HashSet<>(clusterBuilder.buildLiveNodes()));
    BalancePlan balancePlan =
        plugin.computeBalancing(balanceRequest, clusterBuilder.buildPlacementContext());

    // Each expected placement is represented as a string "col shard replica-type fromNode ->
    // toNode"
    Set<String> expectedPlacements = Set.of("a 1 NRT 3 -> 1", "a 2 NRT 3 -> 0");
    verifyBalancing(
        expectedPlacements,
        balancePlan,
        collectionBuilder.getShardBuilders(),
        clusterBuilder.buildLiveNodes());
  }

  /** Tests that balancing works across a subset of nodes */
  @Test
  public void testBalancingWithNodeSubset() throws Exception {
    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(5);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    int coresOnNode = 10;
    for (Builders.NodeBuilder nodeBuilder : nodeBuilders) {
      nodeBuilder.setCoreCount(coresOnNode);
      coresOnNode += 10;
    }

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder("a");
    // Note that the collection as defined below is in a state that would NOT be returned by the
    // placement plugin: shard 1 has two replicas on node 0. The plugin should still be able to
    // place additional replicas as long as they don't break the rules.
    List<List<String>> shardsReplicas =
        List.of(
            List.of("NRT 0", "TLOG 0", "NRT 3"), // shard 1
            List.of("NRT 1", "NRT 3", "TLOG 2")); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    // Only balance over node 1 and 2
    List<Node> overNodes = clusterBuilder.buildLiveNodes();
    overNodes.remove(0);

    BalanceRequestImpl balanceRequest = new BalanceRequestImpl(new HashSet<>(overNodes));
    BalancePlan balancePlan =
        plugin.computeBalancing(balanceRequest, clusterBuilder.buildPlacementContext());

    // Each expected placement is represented as a string "col shard replica-type fromNode ->
    // toNode"
    Set<String> expectedPlacements = Set.of("a 1 NRT 3 -> 1");
    verifyBalancing(
        expectedPlacements,
        balancePlan,
        collectionBuilder.getShardBuilders(),
        clusterBuilder.buildLiveNodes());
  }

  /**
   * Tests balancing with multiple criteria: Replica type restricted nodes, Availability zones +
   * existing collection
   */
  @Test
  public void testBalancingMultiCriteria() throws Exception {
    String collectionName = "a";

    // Note node numbering is in purpose not following AZ structure
    final int AZ1_NRT_LOWCORES = 0;
    final int AZ1_NRT_HIGHCORES = 3;
    final int AZ1_TLOGPULL_LOWFREEDISK = 5;

    final int AZ2_NRT_MEDCORES = 2;
    final int AZ2_NRT_HIGHCORES = 1;
    final int AZ2_TLOGPULL = 7;

    final int AZ3_NRT_LOWCORES = 4;
    final int AZ3_NRT_HIGHCORES = 6;
    final int AZ3_TLOGPULL = 8;

    final String AZ1 = "AZ1";
    final String AZ2 = "AZ2";
    final String AZ3 = "AZ3";

    final int LOW_CORES = 10;
    final int MED_CORES = 50;
    final int HIGH_CORES = 100;

    final String TLOG_PULL_REPLICA_TYPE = "TLOG, PULL";
    final String NRT_REPLICA_TYPE = "Nrt";

    // Cluster nodes and their attributes.
    // 3 AZ's with three nodes each, 2 of which can only take NRT, one that can take TLOG or PULL
    // One of the NRT has fewer cores than the other
    // The TLOG/PULL replica on AZ1 doesn't have much free disk space
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(9);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();
    for (int i = 0; i < 9; i++) {
      final String az;
      final int numcores;
      final double freedisk;
      final String acceptedReplicaType;

      if (i == AZ1_NRT_LOWCORES || i == AZ1_NRT_HIGHCORES || i == AZ1_TLOGPULL_LOWFREEDISK) {
        az = AZ1;
      } else if (i == AZ2_NRT_HIGHCORES || i == AZ2_NRT_MEDCORES || i == AZ2_TLOGPULL) {
        az = AZ2;
      } else {
        az = AZ3;
      }

      if (i == AZ1_NRT_LOWCORES || i == AZ3_NRT_LOWCORES) {
        numcores = LOW_CORES;
      } else if (i == AZ2_NRT_MEDCORES) {
        numcores = MED_CORES;
      } else {
        numcores = HIGH_CORES;
      }

      if (i == AZ1_TLOGPULL_LOWFREEDISK) {
        freedisk = PRIORITIZED_FREE_DISK_GB - 10;
      } else {
        freedisk = PRIORITIZED_FREE_DISK_GB + 10;
      }

      if (i == AZ1_TLOGPULL_LOWFREEDISK || i == AZ2_TLOGPULL || i == AZ3_TLOGPULL) {
        acceptedReplicaType = TLOG_PULL_REPLICA_TYPE;
      } else {
        acceptedReplicaType = NRT_REPLICA_TYPE;
      }

      nodeBuilders
          .get(i)
          .setSysprop(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP, az)
          .setSysprop(AffinityPlacementConfig.REPLICA_TYPE_SYSPROP, acceptedReplicaType)
          .setCoreCount(numcores)
          .setFreeDiskGB(freedisk);
    }

    // The collection already exists with shards and replicas.
    Builders.CollectionBuilder collectionBuilder = Builders.newCollectionBuilder(collectionName);
    List<List<String>> shardsReplicas =
        List.of(
            List.of(
                "NRT " + AZ1_NRT_HIGHCORES,
                "TLOG " + AZ3_TLOGPULL,
                "NRT " + AZ3_NRT_HIGHCORES,
                "NRT " + AZ3_NRT_LOWCORES,
                "TLOG " + AZ2_TLOGPULL), // shard 1
            List.of(
                "TLOG " + AZ2_TLOGPULL,
                "NRT " + AZ2_NRT_HIGHCORES,
                "NRT " + AZ3_NRT_LOWCORES,
                "TLOG " + AZ3_TLOGPULL)); // shard 2
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    List<Node> liveNodes = clusterBuilder.buildLiveNodes();

    BalanceRequestImpl balanceRequest = new BalanceRequestImpl(new HashSet<>(liveNodes));
    BalancePlan bp =
        plugin.computeBalancing(balanceRequest, clusterBuilder.buildPlacementContext());
    Set<String> expectedMovements =
        Set.of(
            "a 2 NRT " + AZ2_NRT_HIGHCORES + " -> " + AZ1_NRT_LOWCORES,
            "a 1 NRT " + AZ3_NRT_HIGHCORES + " -> " + AZ1_NRT_LOWCORES,
            "a 1 NRT " + AZ1_NRT_HIGHCORES + " -> " + AZ2_NRT_MEDCORES);
    verifyBalancing(expectedMovements, bp, collectionBuilder.getShardBuilders(), liveNodes);
  }

  @Test
  public void testWithCollectionBalancing() throws Exception {
    AffinityPlacementConfig config =
        new AffinityPlacementConfig(
            MINIMAL_FREE_DISK_GB,
            PRIORITIZED_FREE_DISK_GB,
            Map.of(primaryCollectionName, secondaryCollectionName),
            Map.of());
    configurePlugin(config);
    // Cluster nodes and their attributes
    Builders.ClusterBuilder clusterBuilder = Builders.newClusterBuilder().initializeLiveNodes(6);
    List<Builders.NodeBuilder> nodeBuilders = clusterBuilder.getLiveNodeBuilders();

    // The collection already exists with shards and replicas
    Builders.CollectionBuilder collectionBuilder =
        Builders.newCollectionBuilder(primaryCollectionName);
    // Note that the collection as defined below is in a state that would NOT be returned by the
    // placement plugin: shard 1 has two replicas on node 0. The plugin should still be able to
    // place additional replicas as long as they don't break the rules.
    List<List<String>> shardsReplicas = List.of(List.of("NRT 0", "NRT 1", "NRT 3")); // shard 1
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    // Add another collection
    collectionBuilder = Builders.newCollectionBuilder(secondaryCollectionName);
    shardsReplicas = List.of(List.of("NRT 0", "NRT 2", "NRT 4")); // shard 1
    collectionBuilder.customCollectionSetup(shardsReplicas, nodeBuilders);
    clusterBuilder.addCollection(collectionBuilder);

    PlacementContext placementContext = clusterBuilder.buildPlacementContext();
    Cluster cluster = placementContext.getCluster();

    BalanceRequestImpl balanceRequest = new BalanceRequestImpl(cluster.getLiveNodes());

    BalancePlan bp = plugin.computeBalancing(balanceRequest, placementContext);
    assertEquals(
        "No movements expected, single movable replica requires co-placement",
        0,
        bp.getReplicaMovements().size());
  }
}
