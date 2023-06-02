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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.BalancePlan;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.DeleteCollectionRequest;
import org.apache.solr.cluster.placement.DeleteReplicasRequest;
import org.apache.solr.cluster.placement.DeleteShardsRequest;
import org.apache.solr.cluster.placement.ModificationRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementModificationException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.common.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class OrderedNodePlacementPlugin<T extends OrderedNodePlacementPlugin.WeightedNode> implements PlacementPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  public List<PlacementPlan> computePlacements(
      Collection<PlacementRequest> requests, PlacementContext placementContext)
      throws PlacementException {
    List<PlacementPlan> placementPlans = new ArrayList<>(requests.size());
    Set<Node> allNodes = new HashSet<>();
    Set<SolrCollection> allCollections = new HashSet<>();
    for (PlacementRequest request : requests) {
      allNodes.addAll(request.getTargetNodes());
      allCollections.add(request.getCollection());
    }
    Collection<T> weightedNodes = getWeightedNodes(placementContext, allNodes, allCollections).values();
    for (PlacementRequest request : requests) {
      int totalReplicasPerShard = 0;
      for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
        totalReplicasPerShard += request.getCountReplicasToCreate(rt);
      }

//      if (request.getTargetNodes().size() < totalReplicasPerShard) {
//        throw new PlacementException("Cluster size too small for number of replicas per shard");
//      }

      List<T> nodesForRequest = weightedNodes.stream()
          .filter(wn -> request.getTargetNodes().contains(wn.getNode()))
          .collect(Collectors.toList());

      Set<ReplicaPlacement> replicaPlacements =
          CollectionUtil.newHashSet(totalReplicasPerShard * request.getShardNames().size());

      SolrCollection solrCollection = request.getCollection();
      // Now place randomly all replicas of all shards on available nodes
      for (String shardName : request.getShardNames()) {
        final Replica exampleReplica =
            PlacementPlugin.createProjectedReplica(solrCollection, shardName, Replica.ReplicaType.NRT, null);
        nodesForRequest.sort(Comparator.comparingInt(wn -> wn.getWeightWithReplica(exampleReplica)));
        LinkedList<T> nodesForShard = new LinkedList<>(nodesForRequest);

        for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
          ListIterator<T> listIterator = nodesForShard.listIterator();
          Replica pr = PlacementPlugin.createProjectedReplica(solrCollection, shardName, replicaType, null);
          int replicaCount = request.getCountReplicasToCreate(replicaType);
          ArrayList<T> nodesUsed = new ArrayList<>();
          for (int replica = 0; replica < replicaCount; replica++) {
            T node;
            do {
              if (!listIterator.hasNext()) {
                throw new PlacementException();
              }
              node = listIterator.next();
            } while (!node.canAddReplica(pr));

            listIterator.remove();
            node.addReplica(pr, false);
            nodesUsed.add(node);
            replicaPlacements.add(
                placementContext.getPlacementPlanFactory().createReplicaPlacement(
                    solrCollection, shardName, node.getNode(), replicaType));
          }
          nodesForShard.addAll(nodesUsed);
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
  public BalancePlan computeBalancing(BalanceRequest balanceRequest, PlacementContext placementContext) throws PlacementException {
    Map<Replica, Node> replicaMovements = new HashMap<>();
    TreeSet<T> orderedNodes = new TreeSet<>(getWeightedNodes(placementContext, balanceRequest.getNodes(), placementContext.getCluster().collections()).values());

    // While the node with the least cores still has room to take a replica from the node with the
    // most cores, loop
    Map<Replica, Node> newReplicaMovements = new HashMap<>();
    ArrayList<T> traversedHighNodes = new ArrayList<>(orderedNodes.size() - 1);
    while (orderedNodes.size() > 1 && orderedNodes.first().getWeight() < orderedNodes.last().getWeight()) {
      T lowestWeight = orderedNodes.pollFirst();
      if (lowestWeight == null) {
        break;
      }
      log.debug("Lowest node: {}, weight: {}", lowestWeight.getNode().getName(), lowestWeight.getWeight());

      newReplicaMovements.clear();
      // If a compatible node was found to move replicas, break and find the lowest weighted node again
      while (newReplicaMovements.isEmpty() && !orderedNodes.isEmpty() && orderedNodes.last().getWeight() > lowestWeight.getWeight() + 1) {
        T highestWeight = orderedNodes.pollLast();
        if (highestWeight == null) {
          break;
        }
        log.debug("Highest node: {}, weight: {}", highestWeight.getNode().getName(), highestWeight.getWeight());

        traversedHighNodes.add(highestWeight);
        // select a replica from the node with the most cores to move to the node with the least
        // cores
        Set<Replica> availableReplicasToMove = highestWeight.getAllReplicas();
        int combinedNodeWeights = highestWeight.getWeight() + lowestWeight.getWeight();
        for (Replica r : availableReplicasToMove) {
          log.debug("Replica: {}, lowestWith: {} ({}), highestWithout: {} ({})", r.getReplicaName(), lowestWeight.getWeightWithReplica(r), lowestWeight.canAddReplica(r), highestWeight.getWeightWithoutReplica(r), highestWeight.canRemoveReplica(r));
          // Only continue if the replica can be removed from the old node and moved to the new node
          if (!highestWeight.canRemoveReplica(r) || !lowestWeight.canAddReplica(r)) {
            continue;
          }
          int lowestWeightWithReplica = lowestWeight.getWeightWithReplica(r);
          int highestWeightWithoutReplica = highestWeight.getWeightWithoutReplica(r);
          // If the combined weight of both nodes is lower after the move, make the move.
          // Otherwise, make the move if it doesn't cause the weight of the higher node to
          // go below the weight of the lower node, because that is over-correction.
          if (highestWeightWithoutReplica + lowestWeightWithReplica >= combinedNodeWeights && highestWeightWithoutReplica < lowestWeightWithReplica) {
            continue;
          }
          log.debug("Replica Movement Chosen!");
          highestWeight.removeReplica(r);
          lowestWeight.addReplica(r, false);
          newReplicaMovements.put(r, lowestWeight.getNode());

          // Do not go beyond here, do another loop and see if other nodes can move replicas.
          // It might end up being the same nodes in the next loop that end up moving another replica, but that's ok.
          break;
        }
      }
      orderedNodes.addAll(traversedHighNodes);
      traversedHighNodes.clear();
      if (newReplicaMovements.size() > 0) {
        replicaMovements.putAll(newReplicaMovements);
        // There are no replicas to move to the lowestWeight, remove it from our loop
        orderedNodes.add(lowestWeight);
      }
    }

    return placementContext.getBalancePlanFactory().createBalancePlan(balanceRequest, replicaMovements);
  }

  protected Map<Node, T> getWeightedNodes(PlacementContext placementContext, Set<Node> nodes, Iterable<SolrCollection> relevantCollections) throws PlacementException {
    Map<Node, T> weightedNodes =  getBaseWeightedNodes(placementContext, nodes, relevantCollections);

    for (SolrCollection collection : placementContext.getCluster().collections()) {
      for (Shard shard : collection.shards()) {
        for (Replica replica : shard.replicas()) {
          T weightedNode = weightedNodes.get(replica.getNode());
          if (weightedNode != null) {
            weightedNode.addReplica(replica, true);
          }
        }
      }
    }

    return weightedNodes;
  }

  protected abstract Map<Node, T> getBaseWeightedNodes(PlacementContext placementContext, Set<Node> nodes, Iterable<SolrCollection> relevantCollections) throws PlacementException;

  @Override
  public void verifyAllowedModification(
      ModificationRequest modificationRequest, PlacementContext placementContext)
      throws PlacementException {
    if (modificationRequest instanceof DeleteShardsRequest) {
      log.warn("DeleteShardsRequest not implemented yet, skipping: {}", modificationRequest);
    } else if (modificationRequest instanceof DeleteCollectionRequest) {
      verifyDeleteCollection((DeleteCollectionRequest) modificationRequest, placementContext);
    } else if (modificationRequest instanceof DeleteReplicasRequest) {
      verifyDeleteReplicas((DeleteReplicasRequest) modificationRequest, placementContext);
    } else {
      log.warn("unsupported request type, skipping: {}", modificationRequest);
    }
  }

  protected void verifyDeleteCollection(
      DeleteCollectionRequest deleteCollectionRequest, PlacementContext placementContext)
      throws PlacementException {
    // NO-OP
  }

  protected void verifyDeleteReplicas(
      DeleteReplicasRequest deleteReplicasRequest, PlacementContext placementContext)
      throws PlacementException {
    Set<Node> relevantNodes = deleteReplicasRequest.getReplicas().stream().map(Replica::getNode).collect(Collectors.toSet());
    Map<Node, T> weightedNodes = getWeightedNodes(placementContext, relevantNodes, placementContext.getCluster().collections());
    for (Replica replica : deleteReplicasRequest.getReplicas()) {
      T node = weightedNodes.get(replica.getNode());
      if (node == null) {
        throw new PlacementModificationException("Could not get information for node: " + replica.getNode());
      }
      if (!node.canRemoveReplica(replica)) {
        throw new PlacementModificationException("Can not remove replica: " + replica);
      }
    }
  }

  public static abstract class WeightedNode implements Comparable<WeightedNode> {
    private final Node node;
    private final Map<String, Map<String, Set<Replica>>> replicas;
    private final boolean includeReplicaWeightsWhenInitializing;

    public WeightedNode(Node node) {
      this(node, false);
    }

    public WeightedNode(Node node, boolean includeReplicaWeightsWhenInitializing) {
      this.node = node;
      this.replicas = new HashMap<>();
      this.includeReplicaWeightsWhenInitializing = includeReplicaWeightsWhenInitializing;
    }

    public Node getNode() {
      return node;
    }

    public Set<Replica> getAllReplicas() {
      return
          replicas.values()
              .stream()
              .flatMap(shard -> shard.values().stream())
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());
    }

    public Set<String> getCollections() {
      return replicas.keySet();
    }

    public Set<String> getShards(String collection) {
      return replicas.getOrDefault(collection, Collections.emptyMap()).keySet();
    }

    public Set<Replica> getReplicasForShard(Shard shard) {
      return
          Optional.ofNullable(replicas.get(shard.getCollection().getName()))
              .map(m -> m.get(shard.getShardName()))
              .orElseGet(Collections::emptySet);
    }

    public abstract int getWeight();

    public abstract int getWeightWithReplica(Replica replica);

    public boolean canAddReplica(Replica replica) {
      // By default, do not allow two replicas of the same shard on a node
      return getReplicasForShard(replica.getShard()).isEmpty();
    }

    final public void addReplica(Replica replica, boolean initializingNode) {
      boolean didAddReplica =
          replicas
              .computeIfAbsent(replica.getShard().getCollection().getName(), k -> new HashMap<>())
              .computeIfAbsent(replica.getShard().getShardName(), k -> new HashSet<>(1))
              .add(replica);
      if (didAddReplica && (!initializingNode || includeReplicaWeightsWhenInitializing)) {
        addProjectedReplicaWeights(replica);
      }
    }

    protected abstract void addProjectedReplicaWeights(Replica replica);

    public abstract int getWeightWithoutReplica(Replica replica);

    public boolean canRemoveReplica(Replica replica) {
      return getReplicasForShard(replica.getShard()).contains(replica);
    }

    final public void removeReplica(Replica replica) {
      // Only remove the projected replicaWeight if the node has this replica
      AtomicBoolean hasReplica = new AtomicBoolean(false);
      replicas.computeIfPresent(
          replica.getShard().getCollection().getName(),
          (col, shardReps) -> {
            shardReps.computeIfPresent(
                replica.getShard().getShardName(),
                (shard, reps) -> {
                  if (reps.remove(replica)) {
                    hasReplica.set(true);
                  }
                  return reps.isEmpty() ? null : reps;
                }
            );
            return shardReps.isEmpty() ? null : shardReps;
          }
      );
      if (hasReplica.get()) {
        removeProjectedReplicaWeights(replica);
      }
    }

    protected abstract void removeProjectedReplicaWeights(Replica replica);

    @SuppressWarnings({"rawtypes"})
    protected Comparable getTiebreaker() {
      return node.getName();
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public int compareTo(WeightedNode o) {
      int comp = Integer.compare(this.getWeight(), o.getWeight());
      if (comp == 0 && !equals(o)) {
        // TreeSets do not like a 0 comp for non-equal members.
        comp = getTiebreaker().compareTo(o.getTiebreaker());
      }
      return comp;
    }

    @Override
    public int hashCode() {
      return node.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof WeightedNode)) {
        return false;
      } else {
        WeightedNode on = (WeightedNode) o;
        if (this.node == null) {
          return on.node == null;
        } else {
          return this.node.equals(on.node);
        }
      }
    }
  }
}