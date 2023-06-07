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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;

/**
 * Factory for creating {@link RandomPlacementPlugin}, a placement plugin implementing random
 * placement for new collection creation while preventing two replicas of same shard from being
 * placed on same node..
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.
 */
public class RandomPlacementFactory
    implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new RandomPlacementPlugin();
  }

  public static class RandomPlacementPlugin extends OrderedNodePlacementPlugin {
    private final Random replicaPlacementRandom =
        new Random(); // ok even if random sequence is predictable.

    private RandomPlacementPlugin() {
      // We make things reproducible in tests by using test seed if any
      String seed = System.getProperty("tests.seed");
      if (seed != null) {
        replicaPlacementRandom.setSeed(seed.hashCode());
      }
    }

    @Override
    protected Map<Node, WeightedNode> getBaseWeightedNodes(PlacementContext placementContext, Set<Node> nodes, Iterable<SolrCollection> relevantCollections) {
      HashMap<Node, WeightedNode> nodeMap = new HashMap<>();

      for (Node node : nodes) {
        nodeMap.put(node, new RandomNode(node));
      }

      return nodeMap;
    }

    private class RandomNode extends WeightedNode {
      private int randomTiebreaker;

      public RandomNode(Node node) {
        super(node);
        this.randomTiebreaker = replicaPlacementRandom.nextInt();
      }

      @Override
      public int getWeight() {
        return 0;
      }

      @Override
      @SuppressWarnings({"rawtypes"})
      public Comparable getTiebreaker() {
        return randomTiebreaker;
      }

      @Override
      public int getWeightWithReplica(Replica replica) {
        return getWeight();
      }

      @Override
      public void addProjectedReplicaWeights(Replica replica) {
        randomTiebreaker = replicaPlacementRandom.nextInt();
      }

      @Override
      public void removeProjectedReplicaWeights(Replica replica) {
        randomTiebreaker = replicaPlacementRandom.nextInt();
      }
    }
  }
}
