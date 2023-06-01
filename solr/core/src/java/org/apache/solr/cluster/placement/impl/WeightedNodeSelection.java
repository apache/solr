package org.apache.solr.cluster.placement.impl;

import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.Shard;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.PlacementContext;

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

  public static <T extends WeightedNode> Map<Replica, Node> computeBalancingMovements(
      PlacementContext placementContext, TreeSet<T> orderedNodes) {
    Map<Replica, Node> replicaMovements = new HashMap<>();

    addReplicasToNodes(placementContext, orderedNodes);
    int totalWeight = orderedNodes.stream().mapToInt(WeightedNode::getWeight).sum();
    int optimalWeightPerNode = (int) Math.floor(totalWeight / (double) orderedNodes.size());

    // TODO: think about what to do if this gets stuck
    // While the node with the least cores still has room to take a replica from the node with the
    // most cores, loop
    while (orderedNodes.size() > 0 && orderedNodes.first().getWeight() < optimalWeightPerNode) {
      T lowestWeight = orderedNodes.pollFirst();
      T highestWeight = orderedNodes.pollLast();
      if (lowestWeight == null || highestWeight == null) {
        break;
      }

      // select a replica from the node with the most cores to move to the node with the least
      // cores
      Set<Replica> availableReplicasToMove = highestWeight.getAllReplicas();
      int combinedNodeWeights = highestWeight.getWeight() + lowestWeight.getWeight();
      for (Replica r : availableReplicasToMove) {
        // Only continue if the replica can be removed from the old node and moved to the new node
        if (!highestWeight.canRemoveReplica(r) || !lowestWeight.canAddReplica(r)) {
          continue;
        }
        int lowestWeightWithReplica = lowestWeight.getWeightWithReplica(r);
        int highestWeightWithoutReplica = highestWeight.getWeightWithoutReplica(r);
        // If the combined weight of both nodes is lower after the move, make the move
        if (highestWeightWithoutReplica + lowestWeightWithReplica >= combinedNodeWeights) {
          // Do not take the replica off of the highest weight node if that will make the weight of the node go below the optimal weight
          if (highestWeight.getWeightWithoutReplica(r) < optimalWeightPerNode) {
            continue;
          }
        }
        highestWeight.removeReplica(r, true);
        lowestWeight.addReplica(r, true);
        replicaMovements.put(r, lowestWeight.getNode());
        // Stop if either node has reached the optimal weight
        if (highestWeight.getWeight() <= optimalWeightPerNode
            || lowestWeight.getWeight() >= optimalWeightPerNode) {
          break;
        }
      }

      // Add back the nodes into the sorted set
      orderedNodes.add(lowestWeight);
      orderedNodes.add(highestWeight);
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

  public static class NodeFilter {
    public boolean nodeCanAcceptReplica(WeightedNode node, Replica replica) {
      return true;
    }

    public boolean nodeCanRemoveReplica(WeightedNode node, Replica replica) {
      return true;
    }

    public void addReplicaToNode(WeightedNode node, Replica replica) {
      // NO-OP by default
    }

    public void removeReplicaFromNode(WeightedNode node, Replica replica) {
      // NO-OP by default
    }
  }

  public static class NodeWeightContext {

    public int getWeight(WeightedNode node) {
      return 0;
    }

    public int getWeightWithReplica(WeightedNode node, Replica replica) {
      return getWeight(node);
    }

    public int getWeightWithoutReplica(WeightedNode node, Replica replica) {
      return getWeight(node);
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
      return Integer.compare(this.getWeight(), o.getWeight());
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

  public static abstract class WeightedNodeWithoutReplica implements Comparable<WeightedNode> {
    private final Node node;
    private Map<String, Map<Replica.ReplicaType, Integer>> replicasByShard;

    public WeightedNodeWithoutReplica(Node node) {
      this.node = node;
    }

    public Node getNode() {
      return node;
    }

    public Map<String, Map<Replica.ReplicaType, Integer>> getReplicasByShard() {
      return replicasByShard;
    }

    public Map<Replica.ReplicaType, Integer> getReplicasForShard(Shard shard) {
      return getReplicasForShard(shard.getCollection().getName(), shard.getShardName());
    }

    public Map<Replica.ReplicaType, Integer> getReplicasForShard(String collection, String shard) {
      return replicasByShard.getOrDefault(shardKey(collection, shard), Collections.emptyMap());
    }

    public abstract int getWeight();

    final public int getWeightWithReplica(Replica replica) {
      return getWeightWithReplica(replica.getShard().getCollection().getName(), replica.getShard().getShardName(), replica.getType());
    }

    public abstract int getWeightWithReplica(String collection, String shard, Replica.ReplicaType type);

    final public boolean canAddReplica(Replica replica) {
      // By default, do not allow two replicas of the same shard on a node
      return getReplicasForShard(replica.getShard()).isEmpty();
    }

    public boolean canAddReplica(String collection, String shard, Replica.ReplicaType type) {
      // By default, do not allow two replicas of the same shard on a node
      return getReplicasForShard(collection, shard).isEmpty();
    }

    final public void addReplica(Replica replica, boolean includeProjectedWeights) {
      addReplica(replica.getShard().getCollection().getName(), replica.getShard().getShardName(), replica.getType(), includeProjectedWeights);
    }

    final public void addReplica(String collection, String shard, Replica.ReplicaType type, boolean includeProjectedWeights) {
      replicasByShard.computeIfAbsent(shardKey(collection, shard), k -> new HashMap<>(1)).merge(type, 1, Integer::sum);
      if (includeProjectedWeights) {
        addProjectedReplicaWeights(collection, shard, type);
      }
    }

    final public void addProjectedReplicaWeights(Replica replica) {
      addProjectedReplicaWeights(replica.getShard().getCollection().getName(), replica.getShard().getShardName(), replica.getType());
    }

    protected abstract void addProjectedReplicaWeights(String collection, String shard, Replica.ReplicaType type);

    public abstract int getWeightWithoutReplica(Replica replica);

    public boolean canRemoveReplica(Replica replica) {
      return getReplicasForShard(replica.getShard()).containsKey(replica.getType());
    }

    final public void removeReplica(Replica replica, boolean includeProjectedWeights) {
      Map<Replica.ReplicaType, Integer> typesOnNode = replicasByShard.get(shardKey(replica.getShard()));
      if (typesOnNode != null) {
        // remove the type if the number goes to zero
        typesOnNode.computeIfPresent(replica.getType(), (t,v) -> v > 1 ? v - 1 : null);
        if (includeProjectedWeights) {
          removeProjectedReplicaWeights(replica);
        }
      }
    }

    protected abstract void removeProjectedReplicaWeights(Replica replica);

    @Override
    public int compareTo(WeightedNode o) {
      return Integer.compare(this.getWeight(), o.getWeight());
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

    private static String shardKey(Shard s) {
      return s.getCollection().getName() + "%%%%%" + s.getShardName();
    }

    private static String shardKey(String collection, String shard) {
      return collection + "%%%%%" + shard;
    }
  }
}
