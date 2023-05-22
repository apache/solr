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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.impl.NodeMetricImpl;
import org.apache.solr.common.util.SuppressForbidden;

/**
 * Factory for creating {@link MinimizeCoresPlacementPlugin}, a Placement plugin implementing
 * placing replicas to minimize number of cores per {@link Node}, while not placing two replicas of
 * the same shard on the same node. This code is meant as an educational example of a placement
 * plugin.
 *
 * <p>See {@link AffinityPlacementFactory} for a more realistic example and documentation.
 */
public class MinimizeCoresPlacementFactory
    implements PlacementPluginFactory<PlacementPluginFactory.NoConfig> {

  @Override
  public PlacementPlugin createPluginInstance() {
    return new MinimizeCoresPlacementPlugin();
  }

  private static class MinimizeCoresPlacementPlugin implements PlacementPlugin {

    @Override
    @SuppressForbidden(
        reason =
            "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
    public List<PlacementPlan> computePlacements(
        Collection<PlacementRequest> requests, PlacementContext placementContext)
        throws PlacementException {
      List<PlacementPlan> placementPlans = new ArrayList<>(requests.size());
      Set<Node> allNodes = new HashSet<>();
      for (PlacementRequest request : requests) {
        allNodes.addAll(request.getTargetNodes());
      }

      // Fetch attributes for a superset of all nodes requested amongst the placementRequests
      AttributeFetcher attributeFetcher = placementContext.getAttributeFetcher();
      attributeFetcher.requestNodeMetric(NodeMetricImpl.NUM_CORES);
      attributeFetcher.fetchFrom(allNodes);
      AttributeValues attrValues = attributeFetcher.fetchAttributes();
      Map<String, Integer> coresPerNodeTotal = new HashMap<>();
      for (Node node : allNodes) {
        if (attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES).isEmpty()) {
          throw new PlacementException("Can't get number of cores in " + node);
        }
        coresPerNodeTotal.put(
            node.getName(), attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES).get());
      }

      for (PlacementRequest request : requests) {
        int totalReplicasPerShard = 0;
        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          totalReplicasPerShard += request.getCountReplicasToCreate(rt);
        }

        if (request.getTargetNodes().size() < totalReplicasPerShard) {
          throw new PlacementException("Cluster size too small for number of replicas per shard");
        }

        // Get number of cores on each Node
        Map<Integer, Set<Node>> nodesByCores = new TreeMap<>(Comparator.naturalOrder());

        Set<Node> nodes = request.getTargetNodes();

        // Get the number of cores on each node and sort the nodes by increasing number of cores
        for (Node node : nodes) {
          nodesByCores
              .computeIfAbsent(coresPerNodeTotal.get(node.getName()), k -> new HashSet<>())
              .add(node);
        }

        Set<ReplicaPlacement> replicaPlacements =
            new HashSet<>(totalReplicasPerShard * request.getShardNames().size());

        // Now place all replicas of all shards on nodes, by placing on nodes with the smallest
        // number of cores and taking into account replicas placed during this computation. Note
        // that for each shard we must place replicas on different nodes, when moving to the next
        // shard we use the nodes sorted by their updated number of cores (due to replica placements
        // for previous shards).
        for (String shardName : request.getShardNames()) {
          // Assign replicas based on the sort order of the nodesByCores tree multimap to put
          // replicas on nodes with fewer cores first. We only need totalReplicasPerShard nodes
          // given that's the number of replicas to place. We assign based on the passed
          // nodeEntriesToAssign list so the right nodes get replicas.
          List<Map.Entry<Integer, Node>> nodeEntriesToAssign =
              nodesByCores.entrySet().stream()
                  .flatMap(e -> e.getValue().stream().map(n -> Map.entry(e.getKey(), n)))
                  .limit(totalReplicasPerShard)
                  .collect(Collectors.toList());

          // Update the number of cores each node will have once the assignments below got
          // executed so the next shard picks the lowest loaded nodes for its replicas.
          for (Map.Entry<Integer, Node> e : nodeEntriesToAssign) {
            int coreCount = e.getKey();
            Node node = e.getValue();
            nodesByCores.getOrDefault(coreCount, new HashSet<>()).remove(node);
            nodesByCores.computeIfAbsent(coreCount + 1, k -> new HashSet<>()).add(node);
            coresPerNodeTotal.put(node.getName(), coreCount + 1);
          }

          for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
            placeReplicas(
                request.getCollection(),
                nodeEntriesToAssign,
                placementContext.getPlacementPlanFactory(),
                replicaPlacements,
                shardName,
                request,
                replicaType);
          }
        }

        placementPlans.add(
            placementContext
                .getPlacementPlanFactory()
                .createPlacementPlan(request, replicaPlacements));
      }
      return placementPlans;
    }

    private void placeReplicas(
        SolrCollection solrCollection,
        List<Map.Entry<Integer, Node>> nodeEntriesToAssign,
        PlacementPlanFactory placementPlanFactory,
        Set<ReplicaPlacement> replicaPlacements,
        String shardName,
        PlacementRequest request,
        Replica.ReplicaType replicaType) {
      for (int replica = 0; replica < request.getCountReplicasToCreate(replicaType); replica++) {
        final Map.Entry<Integer, Node> entry = nodeEntriesToAssign.remove(0);
        final Node node = entry.getValue();

        replicaPlacements.add(
            placementPlanFactory.createReplicaPlacement(
                solrCollection, shardName, node, replicaType));
      }
    }

    @Override
    public BalancePlan computeBalancing(
        BalanceRequest balanceRequest, PlacementContext placementContext)
        throws PlacementException, InterruptedException {
      Map<Replica, Node> replicaMovements = new HashMap<>();

      TreeSet<NodeWithCoreCount> orderedNodes =
          getCoresPerNode(placementContext, balanceRequest.getNodes());
      int totalCores = orderedNodes.stream().mapToInt(NodeWithCoreCount::getCoreCount).sum();
      int optimalCoresPerNode = (int) Math.floor(totalCores / (double) orderedNodes.size());
      Map<String, Map<String, Set<Replica>>> replicasPerNode =
          getReplicasPerNode(placementContext, balanceRequest.getNodes());

      // TODO: think about what to do if this gets stuck
      // While the node with the least cores still has room to take a replica from the node with the
      // most cores, loop
      while (orderedNodes.first().getCoreCount() < optimalCoresPerNode) {
        NodeWithCoreCount leastCores = orderedNodes.pollFirst();
        NodeWithCoreCount mostCores = orderedNodes.pollLast();

        // select a replica from the node with the most cores to move to the node with the least
        // cores
        Map<String, Set<Replica>> toNodeShards =
            replicasPerNode.get(leastCores.getNode().getName());
        Map<String, Set<Replica>> fromNodeShards =
            replicasPerNode.get(mostCores.getNode().getName());
        for (String shard : fromNodeShards.keySet()) {
          if (!toNodeShards.containsKey(shard) || toNodeShards.get(shard).isEmpty()) {
            Set<Replica> fromNodeShard = fromNodeShards.get(shard);
            Optional<Replica> replica = fromNodeShard.stream().findFirst();
            if (replica.isPresent()) {
              mostCores.decrementCoreCount();
              fromNodeShard.remove(replica.get());
              leastCores.incrementCoreCount();
              toNodeShards.put(shard, Collections.singleton(replica.get()));
              replicaMovements.put(replica.get(), leastCores.getNode());
            }
            // Stop if either node has reached the optimal amount of cores
            if (mostCores.getCoreCount() == optimalCoresPerNode
                || leastCores.getCoreCount() == optimalCoresPerNode) {
              break;
            }
          }
        }

        // Add back the nodes into the sorted set
        orderedNodes.add(leastCores);
        orderedNodes.add(mostCores);
      }

      return placementContext
          .getBalancePlanFactory()
          .createBalancePlan(balanceRequest, replicaMovements);
    }

    private TreeSet<NodeWithCoreCount> getCoresPerNode(
        PlacementContext placementContext, Set<Node> nodes) throws PlacementException {
      // Fetch attributes for a superset of all nodes requested amongst the placementRequests
      AttributeFetcher attributeFetcher = placementContext.getAttributeFetcher();
      attributeFetcher.requestNodeMetric(NodeMetricImpl.NUM_CORES);
      attributeFetcher.fetchFrom(nodes);
      AttributeValues attrValues = attributeFetcher.fetchAttributes();
      TreeSet<NodeWithCoreCount> coresPerNodeTotal = new TreeSet<>();
      for (Node node : nodes) {
        if (attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES).isEmpty()) {
          throw new PlacementException("Can't get number of cores in " + node);
        }
        coresPerNodeTotal.add(
            new NodeWithCoreCount(
                node, attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES).get()));
      }

      return coresPerNodeTotal;
    }

    private Map<String, Map<String, Set<Replica>>> getReplicasPerNode(
        PlacementContext placementContext, Set<Node> nodes) throws PlacementException {
      Map<String, Map<String, Set<Replica>>> replicasPerNode = new HashMap<>();
      for (Node node : nodes) {
        replicasPerNode.put(node.getName(), new HashMap<>());
      }
      // Fetch attributes for a superset of all nodes requested amongst the placementRequests
      Cluster cluster = placementContext.getCluster();
      for (SolrCollection collection : cluster.collections()) {
        for (Shard shard : collection.shards()) {
          for (Replica replica : shard.replicas()) {
            if (replicasPerNode.containsKey(replica.getNode().getName())) {
              replicasPerNode
                  .get(replica.getNode().getName())
                  .computeIfAbsent(
                      collection.getName() + "%%%%%" + shard.getShardName(),
                      (s) -> new HashSet<>(1))
                  .add(replica);
            }
          }
        }
      }

      return replicasPerNode;
    }

    private static class NodeWithCoreCount implements Comparable<NodeWithCoreCount> {
      private final Node node;
      private int coreCount;

      public NodeWithCoreCount(Node node, int coreCount) {
        this.node = node;
        this.coreCount = coreCount;
      }

      public Node getNode() {
        return node;
      }

      public void incrementCoreCount() {
        coreCount++;
      }

      public void decrementCoreCount() {
        coreCount--;
      }

      public int getCoreCount() {
        return coreCount;
      }

      @Override
      public boolean equals(Object other) {
        if (other instanceof NodeWithCoreCount) {
          NodeWithCoreCount otherNodeWithCoreCount = (NodeWithCoreCount) other;
          if (this.getNode() == null) {
            return otherNodeWithCoreCount.node == null;
          } else if (this.getNode().equals(otherNodeWithCoreCount.getNode())) {
            return this.getCoreCount() == otherNodeWithCoreCount.getCoreCount();
          }
        }
        return false;
      }

      @Override
      public int hashCode() {
        return node.hashCode();
      }

      @Override
      public int compareTo(NodeWithCoreCount other) {
        if (this.getCoreCount() == other.getCoreCount()) {
          return getNode().getName().compareTo(other.getNode().getName());
        } else {
          return Integer.compare(getCoreCount(), other.getCoreCount());
        }
      }
    }
  }
}
