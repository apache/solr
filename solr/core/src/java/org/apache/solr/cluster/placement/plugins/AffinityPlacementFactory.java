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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.cluster.placement.ShardMetrics;
import org.apache.solr.cluster.placement.impl.BuiltInMetrics;
import org.apache.solr.common.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This factory is instantiated by config from its class name. Using it is the only way to create
 * instances of {@link AffinityPlacementPlugin}.
 *
 * <p>In order to configure this plugin to be used for placement decisions, the following {@code
 * curl} command (or something equivalent) has to be executed once the cluster is already running in
 * order to set the appropriate Zookeeper stored configuration. Replace {@code localhost:8983} by
 * one of your servers' IP address and port.
 *
 * <pre>
 *
 * curl -X POST -H 'Content-type:application/json' -d '{
 * "add": {
 *   "name": ".placement-plugin",
 *   "class": "org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory",
 *   "config": {
 *     "minimalFreeDiskGB": 10,
 *     "prioritizedFreeDiskGB": 50
 *   }
 * }
 * }' http://localhost:8983/api/cluster/plugin
 * </pre>
 *
 * <p>In order to delete the placement-plugin section (and to fallback to either Legacy or rule
 * based placement if configured for a collection), execute:
 *
 * <pre>
 *
 * curl -X POST -H 'Content-type:application/json' -d '{
 * "remove" : ".placement-plugin"
 * }' http://localhost:8983/api/cluster/plugin
 * </pre>
 *
 * <p>{@link AffinityPlacementPlugin} implements placing replicas in a way that replicate past
 * Autoscaling config defined <a
 * href="https://github.com/lucidworks/fusion-cloud-native/blob/master/policy.json#L16">here</a>.
 *
 * <p>This specification is doing the following:
 *
 * <p><i>Spread replicas per shard as evenly as possible across multiple availability zones (given
 * by a sys prop), assign replicas based on replica type to specific kinds of nodes (another sys
 * prop), and avoid having more than one replica per shard on the same node.<br>
 * Only after these constraints are satisfied do minimize cores per node or disk usage.</i>
 *
 * <p>Overall strategy of this plugin:
 *
 * <ul>
 *   <li>The set of nodes in the cluster is obtained and transformed into 3 independent sets (that
 *       can overlap) of nodes accepting each of the three replica types.
 *   <li>For each shard on which placing replicas is required and then for each replica type to
 *       place (starting with NRT, then TLOG then PULL):
 *       <ul>
 *         <li>The set of candidates nodes corresponding to the replica type is used and from that
 *             set are removed nodes that already have a replica (of any type) for that shard
 *         <li>If there are not enough nodes, an error is thrown (this is checked further down
 *             during processing).
 *         <li>The number of (already existing) replicas of the current type on each Availability
 *             Zone is collected.
 *         <li>Separate the set of available nodes to as many subsets (possibly some are empty) as
 *             there are Availability Zones defined for the candidate nodes
 *         <li>In each AZ nodes subset, sort the nodes by increasing total number of cores count,
 *             with possibly a condition that pushes nodes with low disk space to the end of the
 *             list? Or a weighted combination of the relative importance of these two factors? Some
 *             randomization? Marking as non available nodes with not enough disk space? These and
 *             other are likely aspects to be played with once the plugin is tested or observed to
 *             be running in prod, don't expect the initial code drop(s) to do all of that.
 *         <li>Iterate over the number of replicas to place (for the current replica type for the
 *             current shard):
 *             <ul>
 *               <li>Based on the number of replicas per AZ collected previously, pick the non empty
 *                   set of nodes having the lowest number of replicas. Then pick the first node in
 *                   that set. That's the node the replica is placed one. Remove the node from the
 *                   set of available nodes for the given AZ and increase the number of replicas
 *                   placed on that AZ.
 *             </ul>
 *         <li>During this process, the number of cores on the nodes in general is tracked to take
 *             into account placement decisions so that not all shards decide to put their replicas
 *             on the same nodes (they might though if these are the less loaded nodes).
 *       </ul>
 * </ul>
 *
 * <p>This code is a realistic placement computation, based on a few assumptions. The code is
 * written in such a way to make it relatively easy to adapt it to (somewhat) different assumptions.
 * Additional configuration options could be introduced to allow configuration base option selection
 * as well...
 */
public class AffinityPlacementFactory implements PlacementPluginFactory<AffinityPlacementConfig> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private AffinityPlacementConfig config = AffinityPlacementConfig.DEFAULT;

  /**
   * Empty public constructor is used to instantiate this factory. Using a factory pattern to allow
   * the factory to do one time costly operations if needed, and to only have to instantiate a
   * default constructor class by name, rather than having to call a constructor with more
   * parameters (if we were to instantiate the plugin class directly without going through a
   * factory).
   */
  public AffinityPlacementFactory() {}

  @Override
  public PlacementPlugin createPluginInstance() {
    return new AffinityPlacementPlugin(
        config.minimalFreeDiskGB,
        config.prioritizedFreeDiskGB,
        config.withCollection,
        config.collectionNodeType,
        config.spreadAcrossDomains);
  }

  @Override
  public void configure(AffinityPlacementConfig cfg) {
    Objects.requireNonNull(cfg, "configuration must never be null");
    this.config = cfg;
  }

  @Override
  public AffinityPlacementConfig getConfig() {
    return config;
  }

  /**
   * See {@link AffinityPlacementFactory} for instructions on how to configure a cluster to use this
   * plugin and details on what the plugin does.
   */
  static class AffinityPlacementPlugin extends OrderedNodePlacementPlugin {

    private final long minimalFreeDiskGB;

    private final long prioritizedFreeDiskGB;

    // primary to secondary (1:1)
    private final Map<String, String> withCollections;
    // secondary to primary (1:N)
    private final Map<String, Set<String>> collocatedWith;

    private final Map<String, Set<String>> nodeTypes;

    private final boolean spreadAcrossDomains;

    /**
     * The factory has decoded the configuration for the plugin instance and passes it the
     * parameters it needs.
     */
    private AffinityPlacementPlugin(
        long minimalFreeDiskGB,
        long prioritizedFreeDiskGB,
        Map<String, String> withCollections,
        Map<String, String> collectionNodeTypes,
        boolean spreadAcrossDomains) {
      this.minimalFreeDiskGB = minimalFreeDiskGB;
      this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
      Objects.requireNonNull(withCollections, "withCollections must not be null");
      Objects.requireNonNull(collectionNodeTypes, "collectionNodeTypes must not be null");
      this.spreadAcrossDomains = spreadAcrossDomains;
      this.withCollections = withCollections;
      if (withCollections.isEmpty()) {
        collocatedWith = Map.of();
      } else {
        collocatedWith = new HashMap<>();
        withCollections.forEach(
            (primary, secondary) ->
                collocatedWith.computeIfAbsent(secondary, s -> new HashSet<>()).add(primary));
      }

      if (collectionNodeTypes.isEmpty()) {
        nodeTypes = Map.of();
      } else {
        nodeTypes = new HashMap<>();
        collectionNodeTypes.forEach(
            (coll, typesString) -> {
              List<String> types = StrUtils.splitSmart(typesString, ',', true);
              if (!types.isEmpty()) {
                nodeTypes.put(coll, new HashSet<>(types));
              }
            });
      }

      // We make things reproducible in tests by using test seed if any
      String seed = System.getProperty("tests.seed");
    }

    private static final class AffinityPlacementContext {
      private final Set<String> allSpreadDomains = new HashSet<>();
      private final Map<String, Map<String, ReplicaSpread>> spreadDomainUsage = new HashMap<>();
      private final Set<String> allAvailabilityZones = new HashSet<>();
      private final Map<String, Map<String, Map<Replica.ReplicaType, ReplicaSpread>>>
          availabilityZoneUsage = new HashMap<>();
      private boolean doSpreadAcrossDomains;
    }

    @Override
    protected Map<Node, WeightedNode> getBaseWeightedNodes(
        PlacementContext placementContext,
        Set<Node> nodes,
        Iterable<SolrCollection> relevantCollections)
        throws PlacementException {
      // Fetch attributes for a superset of all nodes requested amongst the placementRequests
      AttributeFetcher attributeFetcher = placementContext.getAttributeFetcher();
      attributeFetcher
          .requestNodeSystemProperty(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP)
          .requestNodeSystemProperty(AffinityPlacementConfig.NODE_TYPE_SYSPROP)
          .requestNodeSystemProperty(AffinityPlacementConfig.REPLICA_TYPE_SYSPROP)
          .requestNodeSystemProperty(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP);
      attributeFetcher
          .requestNodeMetric(BuiltInMetrics.NODE_NUM_CORES)
          .requestNodeMetric(BuiltInMetrics.NODE_FREE_DISK_GB);
      Set<ReplicaMetric<?>> replicaMetrics = Set.of(BuiltInMetrics.REPLICA_INDEX_SIZE_GB);
      Set<String> requestedCollections = new HashSet<>();
      for (SolrCollection collection : relevantCollections) {
        if (requestedCollections.add(collection.getName())) {
          attributeFetcher.requestCollectionMetrics(collection, replicaMetrics);
        }
      }
      attributeFetcher.fetchFrom(nodes);
      final AttributeValues attrValues = attributeFetcher.fetchAttributes();

      AffinityPlacementContext affinityPlacementContext = new AffinityPlacementContext();
      affinityPlacementContext.doSpreadAcrossDomains = spreadAcrossDomains;

      Map<Node, WeightedNode> affinityNodeMap = new HashMap<>(nodes.size());
      for (Node node : nodes) {
        AffinityNode affinityNode = newNodeFromMetrics(node, attrValues, affinityPlacementContext);
        if (affinityNode != null) {
          affinityNodeMap.put(node, affinityNode);
        }
      }

      return affinityNodeMap;
    }

    AffinityNode newNodeFromMetrics(
        Node node, AttributeValues attrValues, AffinityPlacementContext affinityPlacementContext)
        throws PlacementException {
      Set<Replica.ReplicaType> supportedReplicaTypes =
          attrValues.getSystemProperty(node, AffinityPlacementConfig.REPLICA_TYPE_SYSPROP).stream()
              .flatMap(s -> Arrays.stream(s.split(",")))
              .map(String::trim)
              .map(s -> s.toUpperCase(Locale.ROOT))
              .map(
                  s -> {
                    try {
                      return Replica.ReplicaType.valueOf(s);
                    } catch (IllegalArgumentException e) {
                      log.warn(
                          "Node {} has an invalid value for the {} systemProperty: {}",
                          node.getName(),
                          AffinityPlacementConfig.REPLICA_TYPE_SYSPROP,
                          s);
                      return null;
                    }
                  })
              .collect(Collectors.toSet());
      if (supportedReplicaTypes.isEmpty()) {
        // If property not defined or is only whitespace on a node, assuming node can take any
        // replica type
        supportedReplicaTypes = Set.of(Replica.ReplicaType.values());
      }

      Set<String> nodeType;
      Optional<String> nodePropOpt =
          attrValues.getSystemProperty(node, AffinityPlacementConfig.NODE_TYPE_SYSPROP);
      if (nodePropOpt.isEmpty()) {
        nodeType = Collections.emptySet();
      } else {
        nodeType = new HashSet<>(StrUtils.splitSmart(nodePropOpt.get(), ','));
      }

      Optional<Double> nodeFreeDiskGB =
          attrValues.getNodeMetric(node, BuiltInMetrics.NODE_FREE_DISK_GB);
      Optional<Integer> nodeNumCores =
          attrValues.getNodeMetric(node, BuiltInMetrics.NODE_NUM_CORES);
      String az =
          attrValues
              .getSystemProperty(node, AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP)
              .orElse(AffinityPlacementConfig.UNDEFINED_AVAILABILITY_ZONE);
      affinityPlacementContext.allAvailabilityZones.add(az);
      String spreadDomain;
      if (affinityPlacementContext.doSpreadAcrossDomains) {
        spreadDomain =
            attrValues
                .getSystemProperty(node, AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP)
                .orElse(null);
        if (spreadDomain == null) {
          if (log.isWarnEnabled()) {
            log.warn(
                "AffinityPlacementPlugin configured to spread across domains, but node {} does not have the {} system property. Ignoring spreadAcrossDomains.",
                node.getName(),
                AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP);
          }
          affinityPlacementContext.doSpreadAcrossDomains = false;
          affinityPlacementContext.allSpreadDomains.clear();
        } else {
          affinityPlacementContext.allSpreadDomains.add(spreadDomain);
        }
      } else {
        spreadDomain = null;
      }
      if (nodeFreeDiskGB.isEmpty()) {
        if (log.isWarnEnabled()) {
          log.warn(
              "Unknown free disk on node {}, excluding it from placement decisions.",
              node.getName());
        }
        return null;
      } else if (nodeNumCores.isEmpty()) {
        if (log.isWarnEnabled()) {
          log.warn(
              "Unknown number of cores on node {}, excluding it from placement decisions.",
              node.getName());
        }
        return null;
      } else {
        return new AffinityNode(
            node,
            attrValues,
            affinityPlacementContext,
            supportedReplicaTypes,
            nodeType,
            nodeNumCores.get(),
            nodeFreeDiskGB.get(),
            az,
            spreadDomain);
      }
    }

    private class AffinityNode extends WeightedNode {

      private final AttributeValues attrValues;

      private final AffinityPlacementContext affinityPlacementContext;

      private final Set<Replica.ReplicaType> supportedReplicaTypes;
      private final Set<String> nodeType;

      private int coresOnNode;
      private double nodeFreeDiskGB;

      private final String availabilityZone;
      private final String spreadDomain;

      AffinityNode(
          Node node,
          AttributeValues attrValues,
          AffinityPlacementContext affinityPlacementContext,
          Set<Replica.ReplicaType> supportedReplicaTypes,
          Set<String> nodeType,
          int coresOnNode,
          double nodeFreeDiskGB,
          String az,
          String spreadDomain) {
        super(node);
        this.attrValues = attrValues;
        this.affinityPlacementContext = affinityPlacementContext;
        this.supportedReplicaTypes = supportedReplicaTypes;
        this.nodeType = nodeType;
        this.coresOnNode = coresOnNode;
        this.nodeFreeDiskGB = nodeFreeDiskGB;
        this.availabilityZone = az;
        this.spreadDomain = spreadDomain;
      }

      @Override
      public int calcWeight() {
        return coresOnNode
            + 100 * (prioritizedFreeDiskGB > 0 && nodeFreeDiskGB < prioritizedFreeDiskGB ? 1 : 0)
            + 10000 * getSpreadDomainWeight()
            + 1000000 * getAZWeight();
      }

      @Override
      public int calcRelevantWeightWithReplica(Replica replica) {
        return coresOnNode
            + 100
                * (prioritizedFreeDiskGB > 0
                        && nodeFreeDiskGB - getProjectedSizeOfReplica(replica)
                            < prioritizedFreeDiskGB
                    ? 1
                    : 0)
            + 10000 * projectReplicaSpreadWeight(replica)
            + 1000000 * projectAZWeight(replica);
      }

      @Override
      public boolean canAddReplica(Replica replica) {
        String collection = replica.getShard().getCollection().getName();
        return
        // By default, do not allow two replicas of the same shard on a node
        super.canAddReplica(replica)
            && supportedReplicaTypes.contains(replica.getType())
            && Optional.ofNullable(nodeTypes.get(collection))
                .map(s -> s.stream().anyMatch(nodeType::contains))
                .orElse(true)
            && Optional.ofNullable(withCollections.get(collection))
                .map(this::hasCollectionOnNode)
                .orElse(true)
            && (minimalFreeDiskGB <= 0
                || nodeFreeDiskGB - getProjectedSizeOfReplica(replica) > minimalFreeDiskGB);
      }

      @Override
      public Map<Replica, String> canRemoveReplicas(Collection<Replica> replicas) {
        Map<Replica, String> replicaRemovalExceptions = new HashMap<>();
        Map<String, Map<String, Set<Replica>>> removals = new HashMap<>();
        for (Replica replica : replicas) {
          Set<Replica> replicasForShardOnNode = getReplicasForShardOnNode(replica.getShard());

          if (!replicasForShardOnNode.contains(replica)) {
            replicaRemovalExceptions.put(replica, "The replica does not exist on the node");
            continue;
          }

          SolrCollection collection = replica.getShard().getCollection();
          Set<String> collocatedCollections = new HashSet<>();
          Optional.ofNullable(collocatedWith.get(collection.getName()))
              .ifPresent(collocatedCollections::addAll);
          collocatedCollections.retainAll(getCollectionsOnNode());
          if (collocatedCollections.isEmpty()) {
            continue;
          }

          // There are collocatedCollections for this shard, so make sure there is a replica of this
          // shard left on the node after it is removed
          Set<Replica> replicasRemovedForShard =
              removals
                  .computeIfAbsent(
                      replica.getShard().getCollection().getName(), k -> new HashMap<>())
                  .computeIfAbsent(replica.getShard().getShardName(), k -> new HashSet<>());
          replicasRemovedForShard.add(replica);

          if (replicasRemovedForShard.size() >= replicasForShardOnNode.size()) {
            replicaRemovalExceptions.put(
                replica, "co-located with replicas of " + collocatedCollections);
          }
        }
        return replicaRemovalExceptions;
      }

      @Override
      protected boolean addProjectedReplicaWeights(Replica replica) {
        nodeFreeDiskGB -= getProjectedSizeOfReplica(replica);
        coresOnNode += 1;
        return addReplicaToAzAndSpread(replica);
      }

      @Override
      protected void initReplicaWeights(Replica replica) {
        addReplicaToAzAndSpread(replica);
      }

      private boolean addReplicaToAzAndSpread(Replica replica) {
        boolean needsResort = false;
        needsResort |=
            affinityPlacementContext
                .availabilityZoneUsage
                .computeIfAbsent(replica.getShard().getCollection().getName(), k -> new HashMap<>())
                .computeIfAbsent(replica.getShard().getShardName(), k -> new HashMap<>())
                .computeIfAbsent(
                    replica.getType(),
                    k -> new ReplicaSpread(affinityPlacementContext.allAvailabilityZones))
                .addReplica(availabilityZone);
        if (affinityPlacementContext.doSpreadAcrossDomains) {
          needsResort |=
              affinityPlacementContext
                  .spreadDomainUsage
                  .computeIfAbsent(
                      replica.getShard().getCollection().getName(), k -> new HashMap<>())
                  .computeIfAbsent(
                      replica.getShard().getShardName(),
                      k -> new ReplicaSpread(affinityPlacementContext.allSpreadDomains))
                  .addReplica(spreadDomain);
        }
        return needsResort;
      }

      @Override
      protected void removeProjectedReplicaWeights(Replica replica) {
        nodeFreeDiskGB += getProjectedSizeOfReplica(replica);
        coresOnNode -= 1;
        Optional.ofNullable(
                affinityPlacementContext.availabilityZoneUsage.get(
                    replica.getShard().getCollection().getName()))
            .map(m -> m.get(replica.getShard().getShardName()))
            .map(m -> m.get(replica.getType()))
            .ifPresent(m -> m.removeReplica(availabilityZone));
        if (affinityPlacementContext.doSpreadAcrossDomains) {
          Optional.ofNullable(
                  affinityPlacementContext.spreadDomainUsage.get(
                      replica.getShard().getCollection().getName()))
              .map(m -> m.get(replica.getShard().getShardName()))
              .ifPresent(m -> m.removeReplica(spreadDomain));
        }
      }

      private double getProjectedSizeOfReplica(Replica replica) {
        return attrValues
            .getCollectionMetrics(replica.getShard().getCollection().getName())
            .flatMap(colMetrics -> colMetrics.getShardMetrics(replica.getShard().getShardName()))
            .flatMap(ShardMetrics::getLeaderMetrics)
            .flatMap(lrm -> lrm.getReplicaMetric(BuiltInMetrics.REPLICA_INDEX_SIZE_GB))
            .orElse(0D);
      }

      private int getSpreadDomainWeight() {
        if (affinityPlacementContext.doSpreadAcrossDomains) {
          return affinityPlacementContext.spreadDomainUsage.values().stream()
              .flatMap(m -> m.values().stream())
              .mapToInt(rs -> rs.overMinimum(spreadDomain))
              .map(i -> i * i)
              .sum();
        } else {
          return 0;
        }
      }

      private int projectReplicaSpreadWeight(Replica replica) {
        if (replica != null && affinityPlacementContext.doSpreadAcrossDomains) {
          return Optional.ofNullable(
                  affinityPlacementContext.spreadDomainUsage.get(
                      replica.getShard().getCollection().getName()))
              .map(m -> m.get(replica.getShard().getShardName()))
              .map(rs -> rs.projectOverMinimum(spreadDomain, 1))
              .map(i -> i * i)
              .orElse(0);
        } else {
          return 0;
        }
      }

      private int getAZWeight() {
        if (affinityPlacementContext.allAvailabilityZones.size() < 2) {
          return 0;
        } else {
          return affinityPlacementContext.availabilityZoneUsage.values().stream()
              .flatMap(m -> m.values().stream())
              .flatMap(m -> m.values().stream())
              .mapToInt(rs -> rs.overMinimum(availabilityZone))
              .map(i -> i * i)
              .sum();
        }
      }

      private int projectAZWeight(Replica replica) {
        if (affinityPlacementContext.allAvailabilityZones.size() < 2) {
          return 0;
        } else {
          return Optional.ofNullable(
                  affinityPlacementContext.availabilityZoneUsage.get(
                      replica.getShard().getCollection().getName()))
              .map(m -> m.get(replica.getShard().getShardName()))
              .map(m -> m.get(replica.getType()))
              .map(rs -> rs.projectOverMinimum(availabilityZone, 1))
              .map(i -> i * i)
              .orElse(0);
        }
      }
    }

    private static class ReplicaSpread {
      private final Set<String> allKeys;
      private final Map<String, Integer> spread;
      private int minReplicasLocated;

      private ReplicaSpread(Set<String> allKeys) {
        this.allKeys = allKeys;
        this.spread = new HashMap<>();
        this.minReplicasLocated = 0;
      }

      int overMinimum(String key) {
        return spread.getOrDefault(key, 0) - minReplicasLocated;
      }

      int projectOverMinimum(String key, int replicaDelta) {
        int overMinimum = overMinimum(key);
        if (overMinimum == 0 && replicaDelta > 0) {
          addReplica(key);
          int projected = overMinimum(key);
          removeReplica(key);
          return projected;
        } else {
          return Integer.max(0, overMinimum + replicaDelta);
        }
      }

      /**
       * Add a replica for the given spread key, returning whether a full resorting is needed for
       * AffinityNodes. Resorting is only needed if other nodes could possibly have a lower weight
       * than before.
       *
       * @param key the spread key for the replica that should be added
       * @return whether a re-sort is required
       */
      boolean addReplica(String key) {
        int previous = spread.getOrDefault(key, 0);
        spread.put(key, previous + 1);
        if (allKeys.size() > 0
            && spread.size() == allKeys.size()
            && previous == minReplicasLocated) {
          minReplicasLocated = spread.values().stream().mapToInt(Integer::intValue).min().orElse(0);
          return true;
        }
        return false;
      }

      void removeReplica(String key) {
        Integer replicasLocated = spread.computeIfPresent(key, (k, v) -> v - 1 == 0 ? null : v - 1);
        if (replicasLocated == null) {
          replicasLocated = 0;
        }
        if (replicasLocated < minReplicasLocated) {
          minReplicasLocated = replicasLocated;
        }
      }
    }
  }
}
