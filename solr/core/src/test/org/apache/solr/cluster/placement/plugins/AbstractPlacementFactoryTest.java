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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.Builders;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.ReplicaPlacement;

/** Unit test for {@link AffinityPlacementFactory} */
public abstract class AbstractPlacementFactoryTest extends SolrTestCaseJ4 {

  /**
   * Verifies that a computed set of placements does match the expected placement on nodes.
   *
   * @param expectedPlacements a set of strings of the form {@code "1 NRT 3"} where 1 would be the
   *     shard index, NRT the replica type and 3 the node on which the replica is placed. Shards are
   *     1-based. Nodes 0-based.
   *     <p>Read carefully: <b>shard index</b> and not shard name. Index in the <b>order</b> of
   *     shards as defined for the collection in the call to {@code
   *     Builders.CollectionBuilder#customCollectionSetup(List, List)}
   * @param shardBuilders the shard builders are passed here to get the shard names by index
   *     (1-based) rather than by parsing the shard names (which would break if we change the shard
   *     naming scheme).
   */
  public static void verifyPlacements(
      Set<String> expectedPlacements,
      PlacementPlan placementPlan,
      List<Builders.ShardBuilder> shardBuilders,
      List<Node> liveNodes) {
    Set<ReplicaPlacement> computedPlacements = placementPlan.getReplicaPlacements();

    // Prepare structures for looking up shard name index and node index
    Map<String, Integer> shardNumbering = new HashMap<>();
    int index = 1; // first shard is 1 not 0
    for (Builders.ShardBuilder sb : shardBuilders) {
      shardNumbering.put(sb.getShardName(), index++);
    }
    Map<Node, Integer> nodeNumbering = new HashMap<>();
    index = 0;
    for (Node n : liveNodes) {
      nodeNumbering.put(n, index++);
    }

    if (expectedPlacements.size() != computedPlacements.size()) {
      fail(
          "Wrong number of placements, expected "
              + expectedPlacements.size()
              + " computed "
              + computedPlacements.size()
              + ". "
              + getExpectedVsComputedPlacement(
                  expectedPlacements, computedPlacements, shardNumbering, nodeNumbering));
    }

    Set<String> expected = new HashSet<>(expectedPlacements);
    for (ReplicaPlacement p : computedPlacements) {
      String lookUpPlacementResult =
          shardNumbering.get(p.getShardName())
              + " "
              + p.getReplicaType().name()
              + " "
              + nodeNumbering.get(p.getNode());
      if (!expected.remove(lookUpPlacementResult)) {
        fail(
            "Computed placement ["
                + lookUpPlacementResult
                + "] not expected. "
                + getExpectedVsComputedPlacement(
                    expectedPlacements, computedPlacements, shardNumbering, nodeNumbering));
      }
    }
  }

  private static String getExpectedVsComputedPlacement(
      Set<String> expectedPlacements,
      Set<ReplicaPlacement> computedPlacements,
      Map<String, Integer> shardNumbering,
      Map<Node, Integer> nodeNumbering) {

    StringBuilder sb = new StringBuilder("Expected placement: ");
    for (String placement : expectedPlacements) {
      sb.append("[").append(placement).append("] ");
    }

    sb.append("Computed placement: ");
    for (ReplicaPlacement placement : computedPlacements) {
      String lookUpPlacementResult =
          shardNumbering.get(placement.getShardName())
              + " "
              + placement.getReplicaType().name()
              + " "
              + nodeNumbering.get(placement.getNode());

      sb.append("[").append(lookUpPlacementResult).append("] ");
    }

    return sb.toString();
  }

  /**
   * Verifies that a computed set of placements does match the expected placement on nodes.
   *
   * @param expectedMovements a set of strings of the form {@code "COL 1 NRT 3 -> 4"} where COL is
   *     the name of the collection, 1 would be the shard index, NRT the replica type and {@code 3
   *     -> 4} represents the replica moving from the 3rd node to the 4th node. Shards are 1-based.
   *     Nodes 0-based.
   *     <p>Read carefully: <b>shard index</b> and not shard name. Index in the <b>order</b> of
   *     shards as defined for the collection in the call to {@code
   *     Builders.CollectionBuilder#customCollectionSetup(List, List)}
   * @param shardBuilders the shard builders are passed here to get the shard names by index
   *     (1-based) rather than by parsing the shard names (which would break if we change the shard
   *     naming scheme).
   */
  public static void verifyBalancing(
      Set<String> expectedMovements,
      BalancePlan balancePlan,
      List<Builders.ShardBuilder> shardBuilders,
      List<Node> liveNodes) {
    Map<Replica, Node> computedMovements = balancePlan.getReplicaMovements();

    assertNotNull(
        "Replica movements returned from balancePlan should not be null", computedMovements);

    // Prepare structures for looking up shard name index and node index
    Map<String, Integer> shardNumbering = new HashMap<>();
    int index = 1; // first shard is 1 not 0
    for (Builders.ShardBuilder sb : shardBuilders) {
      shardNumbering.put(sb.getShardName(), index++);
    }
    Map<Node, Integer> nodeNumbering = new HashMap<>();
    index = 0;
    for (Node n : liveNodes) {
      nodeNumbering.put(n, index++);
    }

    if (expectedMovements.size() != computedMovements.size()) {
      fail(
          "Wrong number of replica movements, expected "
              + expectedMovements.size()
              + " computed "
              + computedMovements.size()
              + ". "
              + getExpectedVsComputedMovement(
                  expectedMovements, computedMovements, shardNumbering, nodeNumbering));
    }

    Set<String> expected = new HashSet<>(expectedMovements);
    for (Map.Entry<Replica, Node> movement : computedMovements.entrySet()) {
      Replica replica = movement.getKey();
      String lookUpMovementResult =
          replica.getShard().getCollection().getName()
              + " "
              + shardNumbering.get(replica.getShard().getShardName())
              + " "
              + replica.getType().name()
              + " "
              + nodeNumbering.get(replica.getNode())
              + " -> "
              + nodeNumbering.get(movement.getValue());
      if (!expected.remove(lookUpMovementResult)) {
        fail(
            "Computed placement ["
                + lookUpMovementResult
                + "] not expected. "
                + getExpectedVsComputedMovement(
                    expectedMovements, computedMovements, shardNumbering, nodeNumbering));
      }
    }
  }

  private static String getExpectedVsComputedMovement(
      Set<String> expectedMovements,
      Map<Replica, Node> computedMovements,
      Map<String, Integer> shardNumbering,
      Map<Node, Integer> nodeNumbering) {

    StringBuilder sb = new StringBuilder("Expected movement: ");
    for (String movement : expectedMovements) {
      sb.append("[").append(movement).append("] ");
    }

    sb.append("Computed movement: ");
    for (Map.Entry<Replica, Node> movement : computedMovements.entrySet()) {
      Replica replica = movement.getKey();
      String lookUpMovementResult =
          replica.getShard().getCollection().getName()
              + " "
              + shardNumbering.get(replica.getShard().getShardName())
              + " "
              + replica.getType().name()
              + " "
              + nodeNumbering.get(replica.getNode())
              + " -> "
              + nodeNumbering.get(movement.getValue());

      sb.append("[").append(lookUpMovementResult).append("] ");
    }

    return sb.toString();
  }
}
