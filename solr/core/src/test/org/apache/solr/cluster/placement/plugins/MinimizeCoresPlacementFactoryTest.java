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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.impl.BalanceRequestImpl;
import org.apache.solr.cluster.placement.impl.PlacementRequestImpl;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit test for {@link AffinityPlacementFactory} */
public class MinimizeCoresPlacementFactoryTest extends AbstractPlacementFactoryTest {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PlacementPlugin plugin;

  @Before
  public void setupPlugin() {
    configurePlugin();
  }

  private void configurePlugin() {
    MinimizeCoresPlacementFactory factory = new MinimizeCoresPlacementFactory();
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
      nodeBuilder.setCoreCount(coresOnNode);
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
      nodeBuilder.setCoreCount(coreCount++);
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
}
