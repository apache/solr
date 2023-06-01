package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.PlacementContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class WeightedNodeSelection {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static <T extends WeightedNode> Map<Replica, Node> computeBalancingMovements(
      PlacementContext placementContext, TreeSet<T> orderedNodes) {
    Map<Replica, Node> replicaMovements = new HashMap<>();

    addReplicasToNodes(placementContext, orderedNodes);
    int totalWeight = orderedNodes.stream().mapToInt(WeightedNode::getWeight).sum();
    double optimalWeightPerNode = totalWeight / (double) orderedNodes.size();
    int optimalWeightFloor = (int)Math.floor(optimalWeightPerNode);
    int optimalWeightCeil = (int)Math.ceil(optimalWeightPerNode);

    // While the node with the least cores still has room to take a replica from the node with the
    // most cores, loop
    Map<Replica, Node> newReplicaMovements = new HashMap<>();
    ArrayList<T> traversedHighNodes = new ArrayList<>(orderedNodes.size() - 1);
    while (orderedNodes.size() > 1 && orderedNodes.first().getWeight() < orderedNodes.last().getWeight()) {
      T lowestWeight = orderedNodes.pollFirst();
      if (lowestWeight == null) {
        break;
      }
      log.info("Lowest node: {}, weight: {}", lowestWeight.getNode().getName(), lowestWeight.getWeight());

      newReplicaMovements.clear();
      // If a compatible node was found to move replicas, break and find the lowest weighted node again
      while (newReplicaMovements.isEmpty() && !orderedNodes.isEmpty() && orderedNodes.last().getWeight() > lowestWeight.getWeight() + 1) {
        T highestWeight = orderedNodes.pollLast();
        if (highestWeight == null) {
          break;
        }
        log.info("Highest node: {}, weight: {}", highestWeight.getNode().getName(), highestWeight.getWeight());

        traversedHighNodes.add(highestWeight);
        // select a replica from the node with the most cores to move to the node with the least
        // cores
        Set<Replica> availableReplicasToMove = highestWeight.getAllReplicas();
        int combinedNodeWeights = highestWeight.getWeight() + lowestWeight.getWeight();
        for (Replica r : availableReplicasToMove) {
          log.info("Replica: {}, lowestWith: {} ({}), highestWithout: {} ({})", r.getReplicaName(), lowestWeight.getWeightWithReplica(r), lowestWeight.canAddReplica(r), highestWeight.getWeightWithoutReplica(r), highestWeight.canRemoveReplica(r));
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
          log.info("Replica Movement Chosen!");
          highestWeight.removeReplica(r, true);
          lowestWeight.addReplica(r, true);
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

    return replicaMovements;
  }

  private static void addReplicasToNodes(PlacementContext placementContext, Set<? extends WeightedNode> nodes) {
    Map<String, WeightedNode> weightedNodeMap = new HashMap<>();
    for (WeightedNode node : nodes) {
      weightedNodeMap.put(node.getNode().getName(), node);
    }
    // Fetch attributes for a superset of all nodes requested amongst the placementRequests
    Cluster cluster = placementContext.getCluster();
    for (SolrCollection collection : cluster.collections()) {
      for (Shard shard : collection.shards()) {
        for (Replica replica : shard.replicas()) {
          if (weightedNodeMap.containsKey(replica.getNode().getName())) {
            weightedNodeMap.get(replica.getNode().getName())
                .addReplica(replica, false);
          }
        }
      }
    }
  }

  public static abstract class WeightedNode implements Comparable<WeightedNode> {
    private final Node node;
    private final Map<String, Map<String, Set<Replica>>> replicas;

    public WeightedNode(Node node) {
      this.node = node;
      this.replicas = new HashMap<>();
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

    final public void addReplica(Replica replica, boolean includeProjectedWeights) {
      boolean didAddReplica =
          replicas
              .computeIfAbsent(replica.getShard().getCollection().getName(), k -> new HashMap<>())
              .computeIfAbsent(replica.getShard().getShardName(), k -> new HashSet<>(1))
              .add(replica);
      if (didAddReplica && includeProjectedWeights) {
        addProjectedReplicaWeights(replica);
      }
    }

    protected abstract void addProjectedReplicaWeights(Replica replica);

    public abstract int getWeightWithoutReplica(Replica replica);

    public boolean canRemoveReplica(Replica replica) {
      return getReplicasForShard(replica.getShard()).contains(replica);
    }

    final public void removeReplica(Replica replica, boolean includeProjectedWeights) {
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
      if (hasReplica.get() && includeProjectedWeights) {
        removeProjectedReplicaWeights(replica);
      }
    }

    protected abstract void removeProjectedReplicaWeights(Replica replica);

    @Override
    public int compareTo(WeightedNode o) {
      int comp = Integer.compare(this.getWeight(), o.getWeight());
      if (comp == 0 && !equals(o)) {
        // TreeSets do not like a 0 comp for non-equal members.
        comp = node.getName().compareTo(o.node.getName());
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

  public static class UnweightedNode extends WeightedNode {

    protected UnweightedNode(Node node) {
      super(node);
    }

    @Override
    public int getWeight() {
      return 0;
    }

    @Override
    protected void addProjectedReplicaWeights(Replica replica) { /* NO-OP */ }

    @Override
    public int getWeightWithReplica(Replica replica) {
      return 0;
    }

    @Override
    public void removeProjectedReplicaWeights(Replica replica) { /* NO-OP */ }

    @Override
    public int getWeightWithoutReplica(Replica replica) {
      return 0;
    }
  }
}
