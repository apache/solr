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
        Iterable<SolrCollection> relevantCollections) {
      HashMap<Node, WeightedNode> nodeVsShardCount = new HashMap<>();

      for (Node n : nodes) {
        nodeVsShardCount.computeIfAbsent(n, SameCollWeightedNode::new);
      }

      return nodeVsShardCount;
    }
  }

  private static class SameCollWeightedNode extends OrderedNodePlacementPlugin.WeightedNode {
    private static final int SAME_COL_MULT = 5;
    public Map<String, Integer> collectionReplicas;
    public int totalWeight = 0;

    SameCollWeightedNode(Node node) {
      super(node);
      this.collectionReplicas = new HashMap<>();
    }

    @Override
    public int calcWeight() {
      return totalWeight;
    }

    @Override
    public int calcRelevantWeightWithReplica(Replica replica) {
      int colReplicaCount =
          collectionReplicas.getOrDefault(replica.getShard().getCollection().getName(), 0) + 1;
      return getAllReplicasOnNode().size() + colReplicaCount * SAME_COL_MULT;
    }

    @Override
    protected boolean addProjectedReplicaWeights(Replica replica) {
      int colReplicaCount =
          collectionReplicas.merge(replica.getShard().getCollection().getName(), 1, Integer::sum);
      totalWeight +=
          1
              + Math.pow(SAME_COL_MULT, colReplicaCount)
              - Math.pow(SAME_COL_MULT, colReplicaCount - 1);
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
        totalWeight -=
            1
                + Math.pow(SAME_COL_MULT, colReplicaCount + 1)
                - Math.pow(SAME_COL_MULT, colReplicaCount);
      }
    }
  }
}
