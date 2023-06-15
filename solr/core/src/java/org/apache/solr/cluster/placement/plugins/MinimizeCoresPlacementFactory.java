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
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.impl.NodeMetricImpl;

/**
 * Factory for creating {@link MinimizeCoresPlacementPlugin}, a Placement plugin implementing
 * placing replicas to minimize number of cores per {@link Node}, while not placing two replicas of
 * the same shard on the same node. This code is meant as an educational example of a placement
 * plugin.
 *
 * <p>See {@link NodeWithCoreCount} for information on how this PlacementFactory weights nodes.
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.
 */
public class MinimizeCoresPlacementFactory
    implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new MinimizeCoresPlacementPlugin();
  }

  private static class MinimizeCoresPlacementPlugin extends OrderedNodePlacementPlugin {

    @Override
    protected Map<Node, WeightedNode> getBaseWeightedNodes(
        PlacementContext placementContext,
        Set<Node> nodes,
        Iterable<SolrCollection> relevantCollections,
        boolean skipNodesWithErrors)
        throws PlacementException {
      // Fetch attributes for a superset of all nodes requested amongst the placementRequests
      AttributeFetcher attributeFetcher = placementContext.getAttributeFetcher();
      attributeFetcher.requestNodeMetric(NodeMetricImpl.NUM_CORES);
      attributeFetcher.fetchFrom(nodes);
      AttributeValues attrValues = attributeFetcher.fetchAttributes();
      HashMap<Node, WeightedNode> nodeMap = new HashMap<>();
      for (Node node : nodes) {
        if (skipNodesWithErrors
            && attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES).isEmpty()) {
          throw new PlacementException("Can't get number of cores in " + node);
        }
        nodeMap.put(
            node,
            new NodeWithCoreCount(
                node, attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES).orElse(0)));
      }

      return nodeMap;
    }
  }

  /**
   * This implementation weights nodes according to how many cores they contain. The weight of a
   * node is just the count of cores on that node.
   *
   * <p>Multiple replicas of the same shard are not permitted to live on the same Node.
   */
  private static class NodeWithCoreCount extends OrderedNodePlacementPlugin.WeightedNode {
    private int coreCount;

    public NodeWithCoreCount(Node node, int coreCount) {
      super(node);
      this.coreCount = coreCount;
    }

    @Override
    public int calcWeight() {
      return coreCount;
    }

    @Override
    public int calcRelevantWeightWithReplica(Replica replica) {
      return coreCount + 1;
    }

    @Override
    public boolean addProjectedReplicaWeights(Replica replica) {
      coreCount += 1;
      return false;
    }

    @Override
    public void removeProjectedReplicaWeights(Replica replica) {
      coreCount -= 1;
    }
  }
}
