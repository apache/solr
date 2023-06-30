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
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
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
    Collection<WeightedNode> weightedNodes =
        getWeightedNodes(placementContext, allNodes, allCollections, true).values();
    for (PlacementRequest request : requests) {
      int totalReplicasPerShard = 0;
      for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
        totalReplicasPerShard += request.getCountReplicasToCreate(rt);
      }

      List<WeightedNode> nodesForRequest =
          weightedNodes.stream()
              .filter(wn -> request.getTargetNodes().contains(wn.getNode()))
              .collect(Collectors.toList());

      Set<ReplicaPlacement> replicaPlacements =
          CollectionUtil.newHashSet(totalReplicasPerShard * request.getShardNames().size());

      SolrCollection solrCollection = request.getCollection();
      // Now place randomly all replicas of all shards on available nodes
      for (String shardName : request.getShardNames()) {
        for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
          int replicaCount = request.getCountReplicasToCreate(replicaType);
          if (replicaCount == 0) {
            continue;
          }
          if (log.isDebugEnabled()) {
            log.debug(
                "Placing {} replicas for Collection: {}, Shard: {}, ReplicaType: {}",
                replicaCount,
                solrCollection.getName(),
                shardName,
                replicaType);
          }
          Replica pr = createProjectedReplica(solrCollection, shardName, replicaType, null);
          PriorityQueue<WeightedNode> nodesForReplicaType = new PriorityQueue<>();
          nodesForRequest.stream()
              .filter(n -> n.canAddReplica(pr))
              .forEach(
                  n -> {
                    n.sortByRelevantWeightWithReplica(pr);
                    n.addToSortedCollection(nodesForReplicaType);
                  });

          int replicasPlaced = 0;
          while (!nodesForReplicaType.isEmpty() && replicasPlaced < replicaCount) {
            WeightedNode node = nodesForReplicaType.poll();
            if (!node.canAddReplica(pr)) {
              if (log.isDebugEnabled()) {
                log.debug(
                    "Node can no longer accept replica, removing from selection list: {}",
                    node.getNode());
              }
              continue;
            }
            if (node.hasWeightChangedSinceSort()) {
              if (log.isDebugEnabled()) {
                log.debug(
                    "Node's sort is out-of-date, adding back to selection list: {}",
                    node.getNode());
              }
              node.addToSortedCollection(nodesForReplicaType);
              // The node will be re-sorted,
              // so go back to the top of the loop to get the new lowest-sorted node
              continue;
            }
            if (log.isDebugEnabled()) {
              log.debug("Node chosen to host replica: {}", node.getNode());
            }

            boolean needsToResortAll =
                node.addReplica(
                    createProjectedReplica(solrCollection, shardName, replicaType, node.getNode()));
            replicasPlaced += 1;
            replicaPlacements.add(
                placementContext
                    .getPlacementPlanFactory()
                    .createReplicaPlacement(
                        solrCollection, shardName, node.getNode(), replicaType));
            // Only update the priorityQueue if there are still replicas to be placed
            if (replicasPlaced < replicaCount) {
              if (needsToResortAll) {
                if (log.isDebugEnabled()) {
                  log.debug("Replica addition requires re-sorting of entire selection list");
                }
                List<WeightedNode> nodeList = new ArrayList<>(nodesForReplicaType);
                nodesForReplicaType.clear();
                nodeList.forEach(n -> n.addToSortedCollection(nodesForReplicaType));
              }
              // Add the chosen node back to the list if it can accept another replica of the
              // shard/replicaType.
              // The default implementation of "canAddReplica()" returns false for replicas
              // of shards that the node already contains, so this will usually be false.
              if (node.canAddReplica(pr)) {
                nodesForReplicaType.add(node);
              }
            }
          }

          if (replicasPlaced < replicaCount) {
            throw new PlacementException(
                String.format(
                    Locale.ROOT,
                    "Not enough eligible nodes to place %d replica(s) of type %s for shard %s of collection %s. Only able to place %d replicas.",
                    replicaCount,
                    replicaType,
                    shardName,
                    solrCollection.getName(),
                    replicasPlaced));
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
  public BalancePlan computeBalancing(
      BalanceRequest balanceRequest, PlacementContext placementContext) throws PlacementException {
    Map<Replica, Node> replicaMovements = new HashMap<>();
    TreeSet<WeightedNode> orderedNodes = new TreeSet<>();
    Collection<WeightedNode> weightedNodes =
        getWeightedNodes(
                placementContext,
                balanceRequest.getNodes(),
                placementContext.getCluster().collections(),
                true)
            .values();
    // This is critical to store the last sort weight for this node
    weightedNodes.forEach(
        node -> {
          node.sortWithoutChanges();
          node.addToSortedCollection(orderedNodes);
        });

    // While the node with the lowest weight still has room to take a replica from the node with the
    // highest weight, loop
    Map<Replica, Node> newReplicaMovements = new HashMap<>();
    ArrayList<WeightedNode> traversedHighNodes = new ArrayList<>(orderedNodes.size() - 1);
    while (orderedNodes.size() > 1
        && orderedNodes.first().calcWeight() < orderedNodes.last().calcWeight()) {
      WeightedNode lowestWeight = orderedNodes.pollFirst();
      if (lowestWeight == null) {
        break;
      }
      if (lowestWeight.hasWeightChangedSinceSort()) {
        if (log.isDebugEnabled()) {
          log.debug(
              "Re-sorting lowest weighted node: {}, sorting weight is out-of-date.",
              lowestWeight.getNode().getName());
        }
        // Re-sort this node and go back to find the lowest weight
        lowestWeight.addToSortedCollection(orderedNodes);
        continue;
      }
      if (log.isDebugEnabled()) {
        log.debug(
            "Lowest weighted node: {}, weight: {}",
            lowestWeight.getNode().getName(),
            lowestWeight.calcWeight());
      }

      newReplicaMovements.clear();
      // If a compatible node was found to move replicas, break and find the lowest weighted node
      // again
      while (newReplicaMovements.isEmpty()
          && !orderedNodes.isEmpty()
          && orderedNodes.last().calcWeight() > lowestWeight.calcWeight() + 1) {
        WeightedNode highestWeight = orderedNodes.pollLast();
        if (highestWeight == null) {
          break;
        }
        if (highestWeight.hasWeightChangedSinceSort()) {
          if (log.isDebugEnabled()) {
            log.debug(
                "Re-sorting highest weighted node: {}, sorting weight is out-of-date.",
                highestWeight.getNode().getName());
          }
          // Re-sort this node and go back to find the highest weight
          highestWeight.addToSortedCollection(orderedNodes);
          continue;
        }
        if (log.isDebugEnabled()) {
          log.debug(
              "Highest weighted node: {}, weight: {}",
              highestWeight.getNode().getName(),
              highestWeight.calcWeight());
        }

        traversedHighNodes.add(highestWeight);
        // select a replica from the node with the most cores to move to the node with the least
        // cores
        List<Replica> availableReplicasToMove =
            highestWeight.getAllReplicasOnNode().stream()
                .sorted(Comparator.comparing(Replica::getReplicaName))
                .collect(Collectors.toList());
        int combinedNodeWeights = highestWeight.calcWeight() + lowestWeight.calcWeight();
        for (Replica r : availableReplicasToMove) {
          // Only continue if the replica can be removed from the old node and moved to the new node
          if (!highestWeight.canRemoveReplicas(Set.of(r)).isEmpty()
              || !lowestWeight.canAddReplica(r)) {
            continue;
          }
          lowestWeight.addReplica(r);
          highestWeight.removeReplica(r);
          int lowestWeightWithReplica = lowestWeight.calcWeight();
          int highestWeightWithoutReplica = highestWeight.calcWeight();
          if (log.isDebugEnabled()) {
            log.debug(
                "Replica: {}, toNode weight with replica: {}, fromNode weight without replica: {}",
                r.getReplicaName(),
                lowestWeightWithReplica,
                highestWeightWithoutReplica);
          }

          // If the combined weight of both nodes is lower after the move, make the move.
          // Otherwise, make the move if it doesn't cause the weight of the higher node to
          // go below the weight of the lower node, because that is over-correction.
          if (highestWeightWithoutReplica + lowestWeightWithReplica >= combinedNodeWeights
              && highestWeightWithoutReplica < lowestWeightWithReplica) {
            // Undo the move
            lowestWeight.removeReplica(r);
            highestWeight.addReplica(r);
            continue;
          }
          if (log.isDebugEnabled()) {
            log.debug(
                "Replica Movement chosen. From: {}, To: {}, Replica: {}",
                highestWeight.getNode().getName(),
                lowestWeight.getNode().getName(),
                r);
          }
          newReplicaMovements.put(r, lowestWeight.getNode());

          // Do not go beyond here, do another loop and see if other nodes can move replicas.
          // It might end up being the same nodes in the next loop that end up moving another
          // replica, but that's ok.
          break;
        }
      }
      // For now we do not have any way to see if there are out-of-date notes in the middle of the
      // TreeSet. Therefore, we need to re-sort this list after every selection. In the future, we
      // should find a way to re-sort the out-of-date nodes without having to sort all nodes.
      traversedHighNodes.addAll(orderedNodes);
      orderedNodes.clear();

      // Add back in the traversed highNodes that we did not select replicas from,
      // they might have replicas to move to the next lowestWeighted node
      traversedHighNodes.forEach(n -> n.addToSortedCollection(orderedNodes));
      traversedHighNodes.clear();
      if (newReplicaMovements.size() > 0) {
        replicaMovements.putAll(newReplicaMovements);
        // There are no replicas to move to the lowestWeight, remove it from our loop
        lowestWeight.addToSortedCollection(orderedNodes);
      }
    }

    return placementContext
        .getBalancePlanFactory()
        .createBalancePlan(balanceRequest, replicaMovements);
  }

  protected Map<Node, WeightedNode> getWeightedNodes(
      PlacementContext placementContext,
      Set<Node> nodes,
      Iterable<SolrCollection> relevantCollections,
      boolean skipNodesWithErrors)
      throws PlacementException {
    Map<Node, WeightedNode> weightedNodes =
        getBaseWeightedNodes(placementContext, nodes, relevantCollections, skipNodesWithErrors);

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

  protected abstract Map<Node, WeightedNode> getBaseWeightedNodes(
      PlacementContext placementContext,
      Set<Node> nodes,
      Iterable<SolrCollection> relevantCollections,
      boolean skipNodesWithErrors)
      throws PlacementException;

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
        deleteReplicasRequest.getReplicas().stream()
            .collect(Collectors.groupingBy(Replica::getNode));

    Map<Node, WeightedNode> weightedNodes =
        getWeightedNodes(
            placementContext,
            nodesRepresented.keySet(),
            placementContext.getCluster().collections(),
            false);

    PlacementModificationException placementModificationException =
        new PlacementModificationException("delete replica(s) rejected");
    for (Map.Entry<Node, List<Replica>> entry : nodesRepresented.entrySet()) {
      WeightedNode node = weightedNodes.get(entry.getKey());
      if (node == null) {
        entry
            .getValue()
            .forEach(
                replica ->
                    placementModificationException.addRejectedModification(
                        replica.toString(),
                        "could not load information for node: " + entry.getKey().getName()));
      } else {
        node.canRemoveReplicas(entry.getValue())
            .forEach(
                (replica, reason) ->
                    placementModificationException.addRejectedModification(
                        replica.toString(), reason));
      }
    }
    if (!placementModificationException.getRejectedModifications().isEmpty()) {
      throw placementModificationException;
    }
  }

  /**
   * A class that determines the weight of a given node and the replicas that reside on it.
   *
   * <p>The {@link OrderedNodePlacementPlugin} uses the weights determined here to place and balance
   * replicas across the cluster. Replicas will be placed onto WeightedNodes with lower weights, and
   * be taken off of WeightedNodes with higher weights.
   *
   * @lucene.experimental
   */
  public abstract static class WeightedNode implements Comparable<WeightedNode> {
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
      return replicas.values().stream()
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
      return replicas
          .getOrDefault(shard.getCollection().getName(), Collections.emptyMap())
          .containsKey(shard.getShardName());
    }

    public Set<Replica> getReplicasForShardOnNode(Shard shard) {
      return Optional.ofNullable(replicas.get(shard.getCollection().getName()))
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
          .computeIfAbsent(replica.getShard().getShardName(), k -> CollectionUtil.newHashSet(1))
          .add(replica);
    }

    public final void initReplica(Replica replica) {
      if (addReplicaToInternalState(replica)) {
        initReplicaWeights(replica);
      }
    }

    protected void initReplicaWeights(Replica replica) {
      // Defaults to a NO-OP
    }

    public final boolean addReplica(Replica replica) {
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
     * @return a mapping from replicas that cannot be removed to the reason why they can't be
     *     removed.
     */
    public Map<Replica, String> canRemoveReplicas(Collection<Replica> replicas) {
      return Collections.emptyMap();
    }

    public final void removeReplica(Replica replica) {
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
                });
            return shardReps.isEmpty() ? null : shardReps;
          });
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

    @Override
    public String toString() {
      return "WeightedNode{" + "node=" + node + ", lastSortedWeight=" + lastSortedWeight + '}';
    }
  }

  /**
   * Create a fake replica to be used when computing placements for new Replicas. The new replicas
   * need to be added to the projected state, even though they don't exist.
   *
   * @param collection the existing collection that the replica will be created for
   * @param shardName the name of the new replica's shard
   * @param type the ReplicaType for the new replica
   * @param node the Solr node on which the replica will be placed
   * @return a fake replica to use until the new replica is created
   */
  static Replica createProjectedReplica(
      final SolrCollection collection,
      final String shardName,
      final Replica.ReplicaType type,
      final Node node) {
    final Shard shard =
        new Shard() {
          @Override
          public String getShardName() {
            return shardName;
          }

          @Override
          public SolrCollection getCollection() {
            return collection;
          }

          @Override
          public Replica getReplica(String name) {
            return null;
          }

          @Override
          public Iterator<Replica> iterator() {
            return null;
          }

          @Override
          public Iterable<Replica> replicas() {
            return null;
          }

          @Override
          public Replica getLeader() {
            return null;
          }

          @Override
          public ShardState getState() {
            return null;
          }

          @Override
          public String toString() {
            return Optional.ofNullable(collection)
                    .map(SolrCollection::getName)
                    .orElse("<no collection>")
                + "/"
                + shardName;
          }
        };
    return new Replica() {
      @Override
      public Shard getShard() {
        return shard;
      }

      @Override
      public ReplicaType getType() {
        return type;
      }

      @Override
      public ReplicaState getState() {
        return ReplicaState.DOWN;
      }

      @Override
      public String getReplicaName() {
        return "";
      }

      @Override
      public String getCoreName() {
        return "";
      }

      @Override
      public Node getNode() {
        return node;
      }

      @Override
      public String toString() {
        return Optional.ofNullable(shard).map(Shard::getShardName).orElse("<no shard>")
            + "@"
            + Optional.ofNullable(node).map(Node::getName).orElse("<no node>")
            + " of "
            + type;
      }
    };
  }
}
