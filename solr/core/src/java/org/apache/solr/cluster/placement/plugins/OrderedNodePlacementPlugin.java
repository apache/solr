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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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

    Deque<PendingPlacementRequest> pendingRequests = new ArrayDeque<>(requests.size());
    for (PlacementRequest request : requests) {
      PendingPlacementRequest pending = new PendingPlacementRequest(request);
      pendingRequests.add(pending);
      placementPlans.add(
          placementContext
              .getPlacementPlanFactory()
              .createPlacementPlan(request, pending.getComputedPlacementSet()));
      allNodes.addAll(request.getTargetNodes());
      allCollections.add(request.getCollection());
    }

    Collection<WeightedNode> weightedNodes =
        getWeightedNodes(placementContext, allNodes, allCollections, true).values();
    while (!pendingRequests.isEmpty()) {
      PendingPlacementRequest request = pendingRequests.poll();
      if (!request.isPending()) {
        continue;
      }

      List<WeightedNode> nodesForRequest =
          weightedNodes.stream().filter(request::isTargetingNode).collect(Collectors.toList());

      SolrCollection solrCollection = request.getCollection();
      // Now place all replicas of all shards on available nodes
      for (String shardName : request.getPendingShards()) {
        for (Replica.ReplicaType replicaType : request.getPendingReplicaTypes(shardName)) {
          int replicaCount = request.getPendingReplicas(shardName, replicaType);
          if (log.isDebugEnabled()) {
            log.debug(
                "Placing {} replicas for Collection: {}, Shard: {}, ReplicaType: {}",
                replicaCount,
                solrCollection.getName(),
                shardName,
                replicaType);
          }
          Replica pr = createProjectedReplica(solrCollection, shardName, replicaType, null);

          // Create a NodeHeap so that we have access to the number of ties for the lowestWeighted
          // node.
          // Sort this heap by the relevant weight of the node given that the replica has been
          // added.
          NodeHeap nodesForReplicaType = new NodeHeap(n -> n.calcRelevantWeightWithReplica(pr));
          nodesForRequest.stream()
              .filter(n -> n.canAddReplica(pr))
              .forEach(nodesForReplicaType::add);

          int replicasPlaced = 0;
          boolean retryRequestLater = false;
          while (!nodesForReplicaType.isEmpty() && replicasPlaced < replicaCount) {
            WeightedNode node = nodesForReplicaType.poll();

            if (!node.canAddReplica(pr)) {
              log.debug("Node can no-longer add the given replica, move on to next node: {}", node);
              continue;
            }

            // If there is a tie, and there are more node options than we have replicas to place,
            // then we want to come back later and try again. If there are ties, but less tie
            // options than we have replicas to place, that's ok, because the replicas will likely
            // be put on all the tie options.
            //
            // Only skip the request if it can be requeued, and there are other pending requests to
            // compute.
            int numWeightTies = nodesForReplicaType.peekTies();
            if (!pendingRequests.isEmpty()
                && request.canBeRequeued()
                && numWeightTies > (replicaCount - replicasPlaced)) {
              log.debug(
                  "There is a tie for best weight. There are more options ({}) than replicas to place ({}), so try this placement request later: {}",
                  numWeightTies,
                  replicaCount - replicasPlaced,
                  node);
              retryRequestLater = true;
              break;
            }
            log.debug("Node chosen to host replica: {}", node);

            boolean needsToResortAll =
                node.addReplica(
                    createProjectedReplica(solrCollection, shardName, replicaType, node.getNode()));
            replicasPlaced += 1;
            request.addPlacement(
                placementContext
                    .getPlacementPlanFactory()
                    .createReplicaPlacement(
                        solrCollection, shardName, node.getNode(), replicaType));
            // Only update the priorityQueue if there are still replicas to be placed
            if (replicasPlaced < replicaCount) {
              if (needsToResortAll) {
                log.debug("Replica addition requires re-sorting of entire selection list");
                nodesForReplicaType.resortAll();
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

          if (!retryRequestLater && replicasPlaced < replicaCount) {
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
      if (request.isPending()) {
        request.requeue();
        pendingRequests.add(request);
      }
    }
    return placementPlans;
  }

  @Override
  public BalancePlan computeBalancing(
      BalanceRequest balanceRequest, PlacementContext placementContext) throws PlacementException {
    Map<Replica, Node> replicaMovements = new HashMap<>();
    TreeSet<WeightedNode> orderedNodes = new TreeSet<>();
    orderedNodes.addAll(
        getWeightedNodes(
                placementContext,
                balanceRequest.getNodes(),
                placementContext.getCluster().collections(),
                true)
            .values());

    // While the node with the lowest weight still has room to take a replica from the node with the
    // highest weight, loop
    Map<Replica, Node> newReplicaMovements = CollectionUtil.newHashMap(1);
    ArrayList<WeightedNode> traversedHighNodes = new ArrayList<>(orderedNodes.size() - 1);
    while (orderedNodes.size() > 1
        && orderedNodes.first().calcWeight() < orderedNodes.last().calcWeight()) {
      WeightedNode lowestWeight = orderedNodes.pollFirst();
      if (lowestWeight == null) {
        break;
      }
      log.debug("Highest weighted node: {}", lowestWeight);

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
        log.debug("Highest weighted node: {}", highestWeight);

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
          log.debug(
              "Replica Movement chosen. From: {}, To: {}, Replica: {}",
              highestWeight,
              lowestWeight,
              r);
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
      orderedNodes.addAll(traversedHighNodes);
      traversedHighNodes.clear();
      if (newReplicaMovements.size() > 0) {
        replicaMovements.putAll(newReplicaMovements);
        // There are no replicas to move to the lowestWeight, remove it from our loop
        orderedNodes.add(lowestWeight);
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

    public WeightedNode(Node node) {
      this.node = node;
      this.replicas = new HashMap<>();
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

    @SuppressWarnings({"rawtypes"})
    protected Comparable getTiebreaker() {
      return node.getName();
    }

    @Override
    @SuppressWarnings({"unchecked"})
    public int compareTo(WeightedNode o) {
      int comp = Integer.compare(this.calcWeight(), o.calcWeight());
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
      return "WeightedNode{" + "node=" + node.getName() + ", weight=" + calcWeight() + '}';
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

  /**
   * A heap that stores Nodes, sorting them by a given function.
   *
   * <p>A normal Java heap class cannot be used, because the {@link #peekTies()} method is required.
   */
  private static class NodeHeap {
    final Function<WeightedNode, Integer> weightFunc;

    final TreeMap<Integer, Deque<WeightedNode>> nodesByWeight;

    Deque<WeightedNode> currentLowestList;
    int currentLowestWeight;

    int size = 0;

    protected NodeHeap(Function<WeightedNode, Integer> weightFunc) {
      this.weightFunc = weightFunc;
      nodesByWeight = new TreeMap<>();
      currentLowestList = null;
      currentLowestWeight = -1;
    }

    /**
     * Remove and return the node with the lowest weight. There is no guarantee to the sorting of
     * nodes that have equal weights.
     *
     * @return the node with the lowest weight
     */
    protected WeightedNode poll() {
      updateLowestWeightedList();
      if (currentLowestList == null || currentLowestList.isEmpty()) {
        return null;
      } else {
        size--;
        return currentLowestList.pollFirst();
      }
    }

    /**
     * Return the number of Nodes that are tied for the current lowest weight (using the given
     * sorting function).
     *
     * <p>PeekTies should only be called after poll().
     *
     * @return the number of nodes that are tied for the lowest weight
     */
    protected int peekTies() {
      return currentLowestList == null ? 1 : currentLowestList.size() + 1;
    }

    /** Make sure that the list that contains the nodes with the lowest weights is correct. */
    private void updateLowestWeightedList() {
      recheckLowestWeights();
      while (currentLowestList == null || currentLowestList.isEmpty()) {
        Map.Entry<Integer, Deque<WeightedNode>> lowestEntry = nodesByWeight.pollFirstEntry();
        if (lowestEntry == null) {
          currentLowestList = null;
          currentLowestWeight = -1;
          break;
        } else {
          currentLowestList = lowestEntry.getValue();
          currentLowestWeight = lowestEntry.getKey();
          recheckLowestWeights();
        }
      }
    }

    /**
     * Go through the list of Nodes with the lowest weight, and make sure that they are still the
     * same weight. If their weight has increased, re-add the node to the heap.
     */
    private void recheckLowestWeights() {
      if (currentLowestList != null) {
        currentLowestList.removeIf(
            node -> {
              if (weightFunc.apply(node) != currentLowestWeight) {
                log.debug("Node's sort is out-of-date, re-sorting: {}", node);
                add(node);
                return true;
              }
              return false;
            });
      }
    }

    /**
     * Add a node to the heap.
     *
     * @param node the node to add
     */
    public void add(WeightedNode node) {
      size++;
      int nodeWeight = weightFunc.apply(node);
      if (currentLowestWeight == nodeWeight) {
        currentLowestList.add(node);
      } else {
        nodesByWeight.computeIfAbsent(nodeWeight, w -> new ArrayDeque<>()).addLast(node);
      }
    }

    /**
     * Get the number of nodes in the heap.
     *
     * @return number of nodes
     */
    public int size() {
      return size;
    }

    /**
     * Check if the heap is empty.
     *
     * @return if the heap has no nodes
     */
    public boolean isEmpty() {
      return size == 0;
    }

    /**
     * Re-sort all nodes in the heap, because their weights can no-longer be trusted. This is only
     * necessary if nodes in the heap may have had their weights decrease. If the nodes just had
     * their weights increase, then calling this is not required.
     */
    public void resortAll() {
      ArrayList<WeightedNode> temp = new ArrayList<>(size);
      if (currentLowestList != null) {
        temp.addAll(currentLowestList);
        currentLowestList.clear();
      }
      nodesByWeight.values().forEach(temp::addAll);
      currentLowestWeight = -1;
      nodesByWeight.clear();
      temp.forEach(this::add);
    }
  }

  /** Context for a placement request still has replicas that need to be placed. */
  static class PendingPlacementRequest {
    boolean hasBeenRequeued;

    final SolrCollection collection;

    final Set<Node> targetNodes;

    // A running list of placements already computed
    final Set<ReplicaPlacement> computedPlacements;

    // A live view on how many replicas still need to be placed for each shard & replica type
    final Map<String, Map<Replica.ReplicaType, Integer>> replicasToPlaceForShards;

    public PendingPlacementRequest(PlacementRequest request) {
      hasBeenRequeued = false;
      collection = request.getCollection();
      targetNodes = request.getTargetNodes();
      Set<String> shards = request.getShardNames();
      replicasToPlaceForShards = CollectionUtil.newHashMap(shards.size());
      int totalShardReplicas = 0;
      for (Replica.ReplicaType type : Replica.ReplicaType.values()) {
        int count = request.getCountReplicasToCreate(type);
        if (count > 0) {
          totalShardReplicas += count;
          shards.forEach(
              s ->
                  replicasToPlaceForShards
                      .computeIfAbsent(s, sh -> CollectionUtil.newHashMap(3))
                      .put(type, count));
        }
      }
      computedPlacements = CollectionUtil.newHashSet(totalShardReplicas * shards.size());
    }

    /**
     * Determine if this request is not yet complete, and there are requested replicas that have not
     * had placements computed.
     *
     * @return if there are still replica placements that need to be computed
     */
    public boolean isPending() {
      return !replicasToPlaceForShards.isEmpty();
    }

    public SolrCollection getCollection() {
      return collection;
    }

    public boolean isTargetingNode(WeightedNode node) {
      return targetNodes.contains(node.getNode());
    }

    /**
     * The set of ReplicaPlacements computed for this request.
     *
     * <p>The list that is returned is the same list used internally, so it will be augmented until
     * {@link #isPending()} returns false.
     *
     * @return The live set of replicaPlacements for this request.
     */
    public Set<ReplicaPlacement> getComputedPlacementSet() {
      return computedPlacements;
    }

    /**
     * Fetch the list of shards that still have replicas that need placements computed. If all the
     * requested replicas for a shard are represented in {@link #getComputedPlacementSet()}, then
     * that shard will not be returned by this method.
     *
     * @return list of unfinished shards
     */
    public Collection<String> getPendingShards() {
      return new ArrayList<>(replicasToPlaceForShards.keySet());
    }

    /**
     * For the given shard, return the replica types that still have placements that need to be
     * computed.
     *
     * @param shard name of the shard to check for uncomputed placements
     * @return the set of unfinished replica types
     */
    public Collection<Replica.ReplicaType> getPendingReplicaTypes(String shard) {
      return Optional.ofNullable(replicasToPlaceForShards.get(shard))
          .map(Map::keySet)
          // Use a sorted TreeSet to make sure that tests are repeatable
          .<Collection<Replica.ReplicaType>>map(TreeSet::new)
          .orElseGet(Collections::emptyList);
    }

    /**
     * Fetch the number of replicas that still need to be placed for the given shard and replica
     * type.
     *
     * @param shard name of shard to be place
     * @param type type of replica to be placed
     * @return the number of replicas that have not yet had placements computed
     */
    public int getPendingReplicas(String shard, Replica.ReplicaType type) {
      return Optional.ofNullable(replicasToPlaceForShards.get(shard))
          .map(m -> m.get(type))
          .orElse(0);
    }

    /**
     * Currently, only of requeue is allowed per pending request.
     *
     * @return true if the request has not been requeued already
     */
    public boolean canBeRequeued() {
      return !hasBeenRequeued;
    }

    /** Let the pending request know that it has been requeued */
    public void requeue() {
      hasBeenRequeued = true;
    }

    /**
     * Track the given replica placement for this pending request.
     *
     * @param replica placement that has been made for the pending request
     */
    public void addPlacement(ReplicaPlacement replica) {
      computedPlacements.add(replica);
      replicasToPlaceForShards.computeIfPresent(
          replica.getShardName(),
          (shard, replicaTypes) -> {
            replicaTypes.computeIfPresent(
                replica.getReplicaType(), (type, count) -> (count == 1) ? null : count - 1);
            if (replicaTypes.size() > 0) {
              return replicaTypes;
            } else {
              return null;
            }
          });
    }
  }
}
