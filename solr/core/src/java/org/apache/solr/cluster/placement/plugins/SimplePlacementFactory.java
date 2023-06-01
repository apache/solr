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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.impl.WeightedNodeSelection;
import org.apache.solr.common.util.CollectionUtil;

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

  public static class SimplePlacementPlugin implements PlacementPlugin {
    @Override
    public List<PlacementPlan> computePlacements(
        Collection<PlacementRequest> requests, PlacementContext placementContext)
        throws PlacementException {
      List<PlacementPlan> placementPlans = new ArrayList<>(requests.size());
      Map<Node, ReplicaCount> nodeVsShardCount = getNodeVsShardCount(placementContext, placementContext.getCluster().getLiveDataNodes());
      for (PlacementRequest request : requests) {
        int totalReplicasPerShard = 0;
        for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
          totalReplicasPerShard += request.getCountReplicasToCreate(rt);
        }

        Set<ReplicaPlacement> replicaPlacements =
            CollectionUtil.newHashSet(totalReplicasPerShard * request.getShardNames().size());

        Collection<ReplicaCount> replicaCounts = nodeVsShardCount.values();

        if (request.getTargetNodes().size() < replicaCounts.size()) {
          replicaCounts =
              replicaCounts.stream()
                  .filter(rc -> request.getTargetNodes().contains(rc.getNode()))
                  .collect(Collectors.toList());
        }

        for (String shard : request.getShardNames()) {
          // Reset the ordering of the nodes for each shard, using the replicas added in the
          // previous shards and assign requests
          List<Node> nodeList =
              replicaCounts.stream()
                  .sorted(
                      Comparator.comparingInt(ReplicaCount::getWeight))
                  .map(ReplicaCount::getNode)
                  .collect(Collectors.toList());
          int replicaNumOfShard = 0;
          for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
            for (int i = 0; i < request.getCountReplicasToCreate(replicaType); i++) {
              Node assignedNode = nodeList.get(replicaNumOfShard++ % nodeList.size());

              replicaPlacements.add(
                  placementContext
                      .getPlacementPlanFactory()
                      .createReplicaPlacement(
                          request.getCollection(), shard, assignedNode, replicaType));

              ReplicaCount replicaCount =
                  nodeVsShardCount.computeIfAbsent(assignedNode, ReplicaCount::new);
              replicaCount.addReplica(
                  PlacementPlugin.createProjectedReplica(request.getCollection(), shard, replicaType, assignedNode),
                  true
              );
            }
          }
        }

        placementPlans.add(
            placementContext
                .getPlacementPlanFactory()
                .createPlacementPlan(request, replicaPlacements));
      }
      return placementPlans;
    }

    @Override
    public BalancePlan computeBalancing(BalanceRequest balanceRequest, PlacementContext placementContext) {
      TreeSet<ReplicaCount> orderedNodes =
          new TreeSet<>(getNodeVsShardCount(placementContext, balanceRequest.getNodes()).values());
      return placementContext
          .getBalancePlanFactory()
          .createBalancePlan(
              balanceRequest,
              WeightedNodeSelection.computeBalancingMovements(placementContext, orderedNodes)
          );
    }

    private Map<Node, ReplicaCount> getNodeVsShardCount(PlacementContext placementContext, Set<Node> nodes) {
      HashMap<Node, ReplicaCount> nodeVsShardCount = new HashMap<>();

      for (Node s : nodes) {
        nodeVsShardCount.computeIfAbsent(s, ReplicaCount::new);
      }

      // if we get here we were not given a createNodeList, build a map with real counts.
      for (SolrCollection collection : placementContext.getCluster().collections()) {
        // identify suitable nodes  by checking the no:of cores in each of them
        for (Shard shard : collection.shards()) {
          for (Replica replica : shard.replicas()) {
            ReplicaCount count = nodeVsShardCount.get(replica.getNode());
            if (count != null) {
              count.addReplica(replica, true);
            }
          }
        }
      }
      return nodeVsShardCount;
    }
  }

  static class ReplicaCount extends WeightedNodeSelection.WeightedNode {
    private static final int SAME_COL_MULT = 5;
    public Map<String, Integer> collectionReplicas;
    public int totalWeight = 0;

    ReplicaCount(Node node) {
      super(node);
      this.collectionReplicas = new HashMap<>();
    }

    @Override
    public int getWeight() {
      return totalWeight;
    }

    @Override
    public int getWeightWithReplica(Replica replica) {
      int replicaCount = collectionReplicas.getOrDefault(replica.getShard().getCollection().getName(), 0);
      return totalWeight + (replicaCount > 0 ? SAME_COL_MULT : 1);
    }

    @Override
    protected void addProjectedReplicaWeights(Replica replica) {
      int replicaCount = collectionReplicas.merge(replica.getShard().getCollection().getName(), 1, Integer::sum);
      totalWeight += replicaCount > 1 ? SAME_COL_MULT : 1;
    }

    @Override
    public int getWeightWithoutReplica(Replica replica) {
      int replicaCount = collectionReplicas.getOrDefault(replica.getShard().getCollection().getName(), 0);
      return totalWeight - (replicaCount > 1 ? SAME_COL_MULT : 1);
    }

    @Override
    protected void removeProjectedReplicaWeights(Replica replica) {
      int replicaCount = collectionReplicas.computeIfPresent(replica.getShard().getCollection().getName(), (k, v) -> v - 1);
      totalWeight -= replicaCount > 0 ? SAME_COL_MULT : 1;
    }
  }
}
