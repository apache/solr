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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;
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

public abstract class OrderedNodePlacementPlugin implements PlacementPlugin {
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
    Collection<WeightedNode> weightedNodes = getWeightedNodes(placementContext, allNodes, allCollections).values();
    for (PlacementRequest request : requests) {
      int totalReplicasPerShard = 0;
      for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
        totalReplicasPerShard += request.getCountReplicasToCreate(rt);
      }

      List<WeightedNode> nodesForRequest = weightedNodes.stream()
          .filter(wn -> request.getTargetNodes().contains(wn.getNode()))
          .collect(Collectors.toList());

      Set<ReplicaPlacement> replicaPlacements =
          CollectionUtil.newHashSet(totalReplicasPerShard * request.getShardNames().size());

      SolrCollection solrCollection = request.getCollection();
      // Now place randomly all replicas of all shards on available nodes
      for (String shardName : request.getShardNames()) {
        log.info("Collection: {}, shard: {}", solrCollection.getName(), shardName);

        for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
          log.info("ReplicaType: {}", replicaType);
          int replicaCount = request.getCountReplicasToCreate(replicaType);
          if (replicaCount == 0) {
            continue;
          }
          Replica pr = PlacementPlugin.createProjectedReplica(solrCollection, shardName, replicaType, null);
          PriorityQueue<WeightedNode> nodesForReplicaType = new PriorityQueue<>();
          nodesForRequest.stream()
              .filter(n -> n.canAddReplica(pr))
              .forEach(n -> {
                n.sortByRelevantWeightWithReplica(pr);
                n.addToSortedCollection(nodesForReplicaType);
              });

          if (nodesForReplicaType.size() < replicaCount) {
            throw new PlacementException(
                "Not enough eligible nodes to place "
                    + replicaCount
                    + " replica(s) of type "
                    + replicaType
                    + " for shard "
                    + shardName
                    + " of collection "
                    + solrCollection.getName());
          }

          int nodesChosen = 0;
          while (nodesChosen < replicaCount) {
            if (nodesForReplicaType.isEmpty()) {
              throw new PlacementException("There are not enough nodes to handle request to place replica");
            }
            WeightedNode node = nodesForReplicaType.poll();
            while (node.hasWeightChangedSinceSort()) {
              log.info("Out of date Node: {}", node.getNode());
              node.addToSortedCollection(nodesForReplicaType);
              node = nodesForReplicaType.poll();
            }
            log.info("Node: {}", node.getNode());

            boolean needsToResort = node.addReplica(
                PlacementPlugin.createProjectedReplica(solrCollection, shardName, replicaType, node.getNode())
            );
            nodesChosen += 1;
            replicaPlacements.add(
                placementContext.getPlacementPlanFactory().createReplicaPlacement(
                    solrCollection, shardName, node.getNode(), replicaType));
            if (needsToResort) {
              List<WeightedNode> nodeList = new ArrayList<>(nodesForReplicaType);
              nodesForReplicaType.clear();
              nodeList.forEach(n -> n.addToSortedCollection(nodesForReplicaType));
            }
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
  public BalancePlan computeBalancing(BalanceRequest balanceRequest, PlacementContext placementContext) throws PlacementException {
    Map<Replica, Node> replicaMovements = new HashMap<>();
    TreeSet<WeightedNode> orderedNodes = new TreeSet<>();
    Collection<WeightedNode> weightedNodes = getWeightedNodes(placementContext, balanceRequest.getNodes(), placementContext.getCluster().collections()).values();
    // This is critical to store the last sort weight for this node
    weightedNodes.forEach(node -> node.addToSortedCollection(orderedNodes));

    // While the node with the least cores still has room to take a replica from the node with the
    // most cores, loop
    Map<Replica, Node> newReplicaMovements = new HashMap<>();
    ArrayList<WeightedNode> traversedHighNodes = new ArrayList<>(orderedNodes.size() - 1);
    while (orderedNodes.size() > 1 && orderedNodes.first().calcWeight() < orderedNodes.last().calcWeight()) {
      WeightedNode lowestWeight = orderedNodes.pollFirst();
      if (lowestWeight == null) {
        break;
      }
      if (lowestWeight.hasWeightChangedSinceSort()) {
        // Re-sort this node and go back to find the lowest
        lowestWeight.addToSortedCollection(orderedNodes);
        continue;
      }
      log.debug("Lowest node: {}, weight: {}", lowestWeight.getNode().getName(), lowestWeight.calcWeight());

      newReplicaMovements.clear();
      // If a compatible node was found to move replicas, break and find the lowest weighted node again
      while (newReplicaMovements.isEmpty() && !orderedNodes.isEmpty() && orderedNodes.last().calcWeight() > lowestWeight.calcWeight() + 1) {
        WeightedNode highestWeight = orderedNodes.pollLast();
        if (highestWeight == null) {
          break;
        }
        if (highestWeight.hasWeightChangedSinceSort()) {
          // Re-sort this node and go back to find the lowest
          highestWeight.addToSortedCollection(orderedNodes);
          continue;
        }
        log.debug("Highest node: {}, weight: {}", highestWeight.getNode().getName(), highestWeight.calcWeight());

        traversedHighNodes.add(highestWeight);
        // select a replica from the node with the most cores to move to the node with the least
        // cores
        Set<Replica> availableReplicasToMove = highestWeight.getAllReplicasOnNode();
        int combinedNodeWeights = highestWeight.calcWeight() + lowestWeight.calcWeight();
        for (Replica r : availableReplicasToMove) {
          // Only continue if the replica can be removed from the old node and moved to the new node
          if (highestWeight.canRemoveReplicas(Set.of(r)).isEmpty() || !lowestWeight.canAddReplica(r)) {
            continue;
          }
          lowestWeight.addReplica(r);
          highestWeight.removeReplica(r);
          int lowestWeightWithReplica = lowestWeight.calcWeight();
          int highestWeightWithoutReplica = highestWeight.calcWeight();
          log.debug("Replica: {}, lowestWith: {} ({}), highestWithout: {} ({})", r.getReplicaName(), lowestWeightWithReplica, lowestWeight.canAddReplica(r), highestWeightWithoutReplica, highestWeight.canRemoveReplicas(Set.of(r)));

          // If the combined weight of both nodes is lower after the move, make the move.
          // Otherwise, make the move if it doesn't cause the weight of the higher node to
          // go below the weight of the lower node, because that is over-correction.
          if (highestWeightWithoutReplica + lowestWeightWithReplica >= combinedNodeWeights && highestWeightWithoutReplica < lowestWeightWithReplica) {
            // Undo the move
            lowestWeight.removeReplica(r);
            highestWeight.addReplica(r);
            continue;
          }
          log.debug("Replica Movement Chosen!");
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

  protected Map<Node, WeightedNode> getWeightedNodes(PlacementContext placementContext, Set<Node> nodes, Iterable<SolrCollection> relevantCollections) throws PlacementException {
    Map<Node, WeightedNode> weightedNodes =  getBaseWeightedNodes(placementContext, nodes, relevantCollections);

    for (SolrCollection collection : placementContext.getCluster().collections()) {
      for (Shard shard : collection.shards()) {
        for (Replica replica : shard.replicas()) {
          WeightedNode weightedNode = weightedNodes.get(replica.getNode());
          if (weightedNode != null) {
            weightedNode.initReplica(replica);
          }
        }
      }
    }

    return weightedNodes;
  }

  protected abstract Map<Node, WeightedNode> getBaseWeightedNodes(PlacementContext placementContext, Set<Node> nodes, Iterable<SolrCollection> relevantCollections) throws PlacementException;

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
    Map<Node, List<Replica>> nodesRepresented =
        deleteReplicasRequest.getReplicas()
            .stream()
            .collect(Collectors.groupingBy(Replica::getNode));

    Map<Node, WeightedNode> weightedNodes =
        getWeightedNodes(placementContext, nodesRepresented.keySet(), placementContext.getCluster().collections());

    PlacementModificationException placementModificationException = new PlacementModificationException("delete replica(s) rejected");
    for (Map.Entry<Node, List<Replica>> entry : nodesRepresented.entrySet()) {
      WeightedNode node = weightedNodes.get(entry.getKey());
      if (node == null) {
        entry.getValue().forEach(replica ->
            placementModificationException.addRejectedModification(
                replica.toString(),
                "could not load information for node: " + entry.getKey().getName())
        );
      }
      node.canRemoveReplicas(entry.getValue())
          .forEach((replica, reason) ->
              placementModificationException.addRejectedModification(
                  replica.toString(),
                  reason)
          );
    }
    if (!placementModificationException.getRejectedModifications().isEmpty()) {
      throw placementModificationException;
    }
  }

  public static abstract class WeightedNode implements Comparable<WeightedNode> {
    private final Node node;
    private final Map<String, Map<String, Set<Replica>>> replicas;
    private IntSupplier sortWeightCalculator;
    private int lastSortedWeight;

    public WeightedNode(Node node) {
      this.node = node;
      this.replicas = new HashMap<>();
      this.lastSortedWeight = 0;
      this.sortWeightCalculator = this::calcWeight;
    }

    public void sortByRelevantWeightWithReplica(Replica replica) {
      sortWeightCalculator = () -> calcRelevantWeightWithReplica(replica);
    }

    public void sortWithoutChanges() {
      sortWeightCalculator = this::calcWeight;
    }

    public Node getNode() {
      return node;
    }

    public Set<Replica> getAllReplicasOnNode() {
      return
          replicas.values()
              .stream()
              .flatMap(shard -> shard.values().stream())
              .flatMap(Collection::stream)
              .collect(Collectors.toSet());
    }

    public Set<String> getCollectionsOnNode() {
      return replicas.keySet();
    }

    public boolean hasCollectionOnNode(String collection) {
      return replicas.containsKey(collection);
    }

    public Set<String> getShardsOnNode(String collection) {
      return replicas.getOrDefault(collection, Collections.emptyMap()).keySet();
    }

    public boolean hasShardOnNode(Shard shard) {
      return
          replicas
              .getOrDefault(shard.getCollection().getName(), Collections.emptyMap())
              .containsKey(shard.getShardName());
    }

    public Set<Replica> getReplicasForShardOnNode(Shard shard) {
      return
          Optional.ofNullable(replicas.get(shard.getCollection().getName()))
              .map(m -> m.get(shard.getShardName()))
              .orElseGet(Collections::emptySet);
    }

    public void addToSortedCollection(Collection<WeightedNode> collection) {
      stashSortedWeight();
      collection.add(this);
    }

    public abstract int calcWeight();

    public abstract int calcRelevantWeightWithReplica(Replica replica);

    public boolean canAddReplica(Replica replica) {
      // By default, do not allow two replicas of the same shard on a node
      return getReplicasForShardOnNode(replica.getShard()).isEmpty();
    }

    private boolean addReplicaToInternalState(Replica replica) {
      return replicas
          .computeIfAbsent(replica.getShard().getCollection().getName(), k -> new HashMap<>())
          .computeIfAbsent(replica.getShard().getShardName(), k -> new HashSet<>(1))
          .add(replica);
    }

    final public void initReplica(Replica replica) {
      if (addReplicaToInternalState(replica)) {
        initReplicaWeights(replica);
      }
    }

    protected void initReplicaWeights(Replica replica) {
      // Defaults to a NO-OP
    }

    final public boolean addReplica(Replica replica) {
      if (addReplicaToInternalState(replica)) {
        return addProjectedReplicaWeights(replica);
      } else {
        return false;
      }
    }

    /**
     * Add the weights for the given replica to this node
     *
     * @param replica the replica to add weights for
     * @return a whether the ordered list of nodes needs a resort afterwords.
     */
    protected abstract boolean addProjectedReplicaWeights(Replica replica);

    /**
     * Determine if the given replicas can be removed from the node.
     *
     * @param replicas the replicas to remove
     * @return a mapping from replicas that cannot be removed to the reason why they can't be removed.
     */
    public Map<Replica, String> canRemoveReplicas(Collection<Replica> replicas) {
      Map<Replica, String> replicaRemovalExceptions = new HashMap<>();
      for (Replica replica : replicas) {
        if (!getReplicasForShardOnNode(replica.getShard()).contains(replica)) {
          replicaRemovalExceptions.put(replica, "The replica does not exist on the node");
        }
      }
      return replicaRemovalExceptions;
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

    private void stashSortedWeight() {
      lastSortedWeight = sortWeightCalculator.getAsInt();
    }

    protected boolean hasWeightChangedSinceSort() {
      return lastSortedWeight != sortWeightCalculator.getAsInt();
    }

    @SuppressWarnings({"rawtypes"})
    protected Comparable getTiebreaker() {
      return node.getName();
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public int compareTo(WeightedNode o) {
      int comp = Integer.compare(this.lastSortedWeight, o.lastSortedWeight);
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