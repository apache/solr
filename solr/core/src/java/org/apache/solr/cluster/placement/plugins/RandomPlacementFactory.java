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
 * <p>See {@link RandomNode} for information on how this PlacementFactory weights nodes.
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
    protected Map<Node, WeightedNode> getBaseWeightedNodes(
        PlacementContext placementContext,
        Set<Node> nodes,
        Iterable<SolrCollection> relevantCollections,
        boolean skipNodesWithErrors) {
      HashMap<Node, WeightedNode> nodeMap = new HashMap<>();

      for (Node node : nodes) {
        nodeMap.put(node, new RandomNode(node, replicaPlacementRandom));
      }

      return nodeMap;
    }
  }

  /**
   * This implementation weights nodes equally. When trying to determine which nodes should be
   * chosen to host replicas, a random sorting is used.
   *
   * <p>Multiple replicas of the same shard are not permitted to live on the same Node.
   */
  private static class RandomNode extends OrderedNodePlacementPlugin.WeightedNode {
    private final Random random;
    private int randomTiebreaker;

    public RandomNode(Node node, Random random) {
      super(node);
      this.random = random;
      this.randomTiebreaker = random.nextInt();
    }

    @Override
    public int calcWeight() {
      return 0;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public Comparable getTiebreaker() {
      return randomTiebreaker;
    }

    @Override
    public int calcRelevantWeightWithReplica(Replica replica) {
      return calcWeight();
    }

    @Override
    protected boolean addProjectedReplicaWeights(Replica replica) {
      randomTiebreaker = random.nextInt();
      // NO-OP
      return false;
    }

    @Override
    protected void removeProjectedReplicaWeights(Replica replica) {
      randomTiebreaker = random.nextInt();
      // NO-OP
    }
  }
}
