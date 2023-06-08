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
import java.util.Set;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;

/**
 * Factory for creating {@link SimplePlacementPlugin}, a placement plugin implementing the logic
 * from the old <code>LegacyAssignStrategy</code>. This chooses nodes with the fewest cores
 * (especially cores of the same collection).
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.
 */
public class SimplePlacementFactory
    implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new SimplePlacementPlugin();
  }

  public static class SimplePlacementPlugin extends OrderedNodePlacementPlugin {

    @Override
    protected Map<Node, WeightedNode> getBaseWeightedNodes(
        PlacementContext placementContext,
        Set<Node> nodes,
        Iterable<SolrCollection> relevantCollections,
        boolean skipNodesWithErrors) {
      HashMap<Node, WeightedNode> nodeVsShardCount = new HashMap<>();

      for (Node n : nodes) {
        nodeVsShardCount.computeIfAbsent(n, SameCollWeightedNode::new);
      }

      return nodeVsShardCount;
    }
  }

  private static class SameCollWeightedNode extends OrderedNodePlacementPlugin.WeightedNode {
    private static final int SAME_COL_MULT = 5;
    private static final int SAME_SHARD_MULT = 1000;
    public Map<String, Integer> collectionReplicas;
    public int totalWeight = 0;

    SameCollWeightedNode(Node node) {
      super(node);
      this.collectionReplicas = new HashMap<>();
    }

    /**
     * The weight of the SameCollWeightedNode is the sum of:
     * - The number of replicas on the node
     * - 5 * for each collection, the sum of:
     *   - (the number of replicas for that collection - 1)^2
     * - 1000 * for each shard, the sum of:
     *   - (the number of replicas for that shard - 1)^2
     *
     * @return the weight
     */
    @Override
    public int calcWeight() {
      return totalWeight;
    }

    @Override
    public int calcRelevantWeightWithReplica(Replica replica) {
      // Don't add 1 to the individual replica Counts, because 1 is subtracted from each when calculating weights.
      // So since 1 would be added to each for the new replica, we can just use the original number to calculate the weights.
      int colReplicaCount =
          collectionReplicas.getOrDefault(replica.getShard().getCollection().getName(), 0);
      int shardReplicaCount = getReplicasForShardOnNode(replica.getShard()).size();
      return
          getAllReplicasOnNode().size() + 1
          + colReplicaCount * SAME_COL_MULT
          + shardReplicaCount * SAME_SHARD_MULT;
    }

    @Override
    public boolean canAddReplica(Replica replica) {
      return true;
    }

    @Override
    protected boolean addProjectedReplicaWeights(Replica replica) {
      int colReplicaCount =
          collectionReplicas.merge(replica.getShard().getCollection().getName(), 1, Integer::sum);
      totalWeight += 1;
      if (colReplicaCount > 1) {
        totalWeight += SAME_COL_MULT * (Math.pow(colReplicaCount - 1, 2) - Math.pow(colReplicaCount - 2, 2));
      }
      int shardReplicaCount = getReplicasForShardOnNode(replica.getShard()).size();
      if (shardReplicaCount > 1) {
        totalWeight += SAME_SHARD_MULT * (Math.pow(shardReplicaCount - 1, 2) - Math.pow(shardReplicaCount - 2, 2));
      }
      return false;
    }

    @Override
    protected void initReplicaWeights(Replica replica) {
      addProjectedReplicaWeights(replica);
    }

    @Override
    protected void removeProjectedReplicaWeights(Replica replica) {
      Integer colReplicaCount =
          collectionReplicas.computeIfPresent(
              replica.getShard().getCollection().getName(), (k, v) -> v - 1);
      if (colReplicaCount != null) {
        totalWeight -= 1;
        if (colReplicaCount >= 1) {
          totalWeight -= SAME_COL_MULT * (Math.pow(colReplicaCount, 2) - Math.pow(colReplicaCount - 1, 2));
        }
      }
    }
  }
}
