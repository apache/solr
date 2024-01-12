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

import java.io.IOException;
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
import java.util.stream.Stream;
import org.apache.solr.cluster.Cluster;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.Replica;
import org.apache.solr.cluster.SolrCollection;
import org.apache.solr.cluster.placement.AttributeFetcher;
import org.apache.solr.cluster.placement.AttributeValues;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.DeleteCollectionRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementModificationException;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.cluster.placement.ShardMetrics;
import org.apache.solr.cluster.placement.impl.NodeMetricImpl;
import org.apache.solr.cluster.placement.impl.ReplicaMetricImpl;
import org.apache.solr.common.util.CollectionUtil;
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
 * <p>This plugin achieves this by creating a {@link AffinityPlacementPlugin.AffinityNode} that
 * weights nodes very high if they are unbalanced with respect to AvailabilityZone and SpreadDomain.
 * See {@link AffinityPlacementPlugin.AffinityNode} for more information on how this weighting helps
 * the plugin correctly place and balance replicas.
 *
 * <p>This code is a realistic placement computation, based on a few assumptions. The code is
 * written in such a way to make it relatively easy to adapt it to (somewhat) different assumptions.
 * Additional configuration options could be introduced to allow configuration base option selection
 * as well...
 */
public class AffinityPlacementFactory implements PlacementPluginFactory<AffinityPlacementConfig> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  AffinityPlacementConfig config = AffinityPlacementConfig.DEFAULT;

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
    config.validate();
    return new AffinityPlacementPlugin(
        config.minimalFreeDiskGB,
        config.prioritizedFreeDiskGB,
        config.withCollection,
        config.withCollectionShards,
        config.collectionNodeType,
        config.spreadAcrossDomains);
  }

  @Override
  public void configure(AffinityPlacementConfig cfg) {
    Objects.requireNonNull(cfg, "configuration must never be null");
    cfg.validate();
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
  public static class AffinityPlacementPlugin extends OrderedNodePlacementPlugin {

    private final long minimalFreeDiskGB;

    private final long prioritizedFreeDiskGB;

    // primary to secondary (1:1)
    private final Map<String, String> withCollections;
    // same but shardwise
    private final Map<String, String> withCollectionShards;
    // secondary to primary (1:N) + shard-wise_primary (1:N)
    private final Map<String, Set<String>> collocatedWith;

    private final Map<String, Set<String>> nodeTypes;

    private final boolean spreadAcrossDomains;

    /**
     * The factory has decoded the configuration for the plugin instance and passes it the
     * parameters it needs.
     */
    AffinityPlacementPlugin(
        long minimalFreeDiskGB,
        long prioritizedFreeDiskGB,
        Map<String, String> withCollections,
        Map<String, String> withCollectionShards,
        Map<String, String> collectionNodeTypes,
        boolean spreadAcrossDomains) {
      this.minimalFreeDiskGB = minimalFreeDiskGB;
      this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
      Objects.requireNonNull(withCollections, "withCollections must not be null");
      Objects.requireNonNull(collectionNodeTypes, "collectionNodeTypes must not be null");
      Objects.requireNonNull(withCollectionShards, "withCollectionShards must not be null");
      this.spreadAcrossDomains = spreadAcrossDomains;
      this.withCollections = withCollections;
      this.withCollectionShards = withCollectionShards;
      Map<String, Set<String>> collocated = new HashMap<>();
      // reverse both relations: shard-agnostic and shard-wise
      List.of(this.withCollections, this.withCollectionShards)
          .forEach(
              direct ->
                  direct.forEach(
                      (primary, secondary) ->
                          collocated
                              .computeIfAbsent(secondary, s -> new HashSet<>())
                              .add(primary)));
      this.collocatedWith = Collections.unmodifiableMap(collocated);

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
    }

    @Override
    protected void verifyDeleteCollection(
        DeleteCollectionRequest deleteCollectionRequest, PlacementContext placementContext)
        throws PlacementModificationException {
      Cluster cluster = placementContext.getCluster();
      Set<String> collocatedCollections =
          collocatedWith.getOrDefault(deleteCollectionRequest.getCollection().getName(), Set.of());
      for (String primaryName : collocatedCollections) {
        try {
          if (cluster.getCollection(primaryName) != null) {
            // still exists
            throw new PlacementModificationException(
                "collocated collection "
                    + primaryName
                    + " of "
                    + deleteCollectionRequest.getCollection().getName()
                    + " still present");
          }
        } catch (IOException e) {
          throw new PlacementModificationException(
              "failed to retrieve collocated collection information", e);
        }
      }
    }

    /**
     * AffinityPlacementContext is used to share information across {@link AffinityNode} instances.
     *
     * <p>For instance, with SpreadDomains and AvailabilityZones, the weighting of a Node requires
     * information on the contents of other Nodes. This class is how that information is shared.
     *
     * <p>One AffinityPlacementContext is used for each call to {@link
     * #computePlacements(Collection, PlacementContext)} or {@link #computeBalancing(BalanceRequest,
     * PlacementContext)}. The state of the context will be altered throughout the computation.
     */
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
        Iterable<SolrCollection> relevantCollections,
        boolean skipNodesWithErrors)
        throws PlacementException {
      // Fetch attributes for a superset of all nodes requested amongst the placementRequests
      AttributeFetcher attributeFetcher = placementContext.getAttributeFetcher();
      attributeFetcher
          .requestNodeSystemProperty(AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP)
          .requestNodeSystemProperty(AffinityPlacementConfig.NODE_TYPE_SYSPROP)
          .requestNodeSystemProperty(AffinityPlacementConfig.REPLICA_TYPE_SYSPROP)
          .requestNodeSystemProperty(AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP);
      attributeFetcher
          .requestNodeMetric(NodeMetricImpl.NUM_CORES)
          .requestNodeMetric(NodeMetricImpl.FREE_DISK_GB);
      Set<ReplicaMetric<?>> replicaMetrics = Set.of(ReplicaMetricImpl.INDEX_SIZE_GB);
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

      Map<Node, WeightedNode> affinityNodeMap = CollectionUtil.newHashMap(nodes.size());
      for (Node node : nodes) {
        AffinityNode affinityNode =
            newNodeFromMetrics(node, attrValues, affinityPlacementContext, skipNodesWithErrors);
        if (affinityNode != null) {
          affinityNodeMap.put(node, affinityNode);
        }
      }

      // If there are not multiple spreadDomains, then there is nothing to spread across
      if (affinityPlacementContext.allSpreadDomains.size() < 2) {
        affinityPlacementContext.doSpreadAcrossDomains = false;
      }

      return affinityNodeMap;
    }

    AffinityNode newNodeFromMetrics(
        Node node,
        AttributeValues attrValues,
        AffinityPlacementContext affinityPlacementContext,
        boolean skipNodesWithErrors)
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

      Optional<Double> nodeFreeDiskGB = attrValues.getNodeMetric(node, NodeMetricImpl.FREE_DISK_GB);
      Optional<Integer> nodeNumCores = attrValues.getNodeMetric(node, NodeMetricImpl.NUM_CORES);
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
          // In the context stop using spreadDomains, because we have a node without a spread
          // domain.
          affinityPlacementContext.doSpreadAcrossDomains = false;
          affinityPlacementContext.allSpreadDomains.clear();
        } else {
          affinityPlacementContext.allSpreadDomains.add(spreadDomain);
        }
      } else {
        spreadDomain = null;
      }
      if (nodeFreeDiskGB.isEmpty() && skipNodesWithErrors) {
        if (log.isWarnEnabled()) {
          log.warn(
              "Unknown free disk on node {}, excluding it from placement decisions.",
              node.getName());
        }
        return null;
      } else if (nodeNumCores.isEmpty() && skipNodesWithErrors) {
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
            nodeNumCores.orElse(0),
            nodeFreeDiskGB.orElse(0D),
            az,
            spreadDomain);
      }
    }

    /**
     * This implementation weights nodes in order to achieve balancing across AvailabilityZones and
     * SpreadDomains, while trying to minimize the amount of replicas on a node and ensure a given
     * disk space per node. This implementation also supports limiting the placement of certain
     * replica types per node and co-locating collections.
     *
     * <p>The total weight of the AffinityNode is the sum of:
     *
     * <ul>
     *   <li>The number of replicas on the node
     *   <li>100 if the free disk space on the node < prioritizedFreeDiskGB, otherwise 0
     *   <li>If SpreadDomains are used:<br>
     *       10,000 * the sum over each collection/shard:
     *       <ul>
     *         <li>(# of replicas in this node's spread domain - the minimum spreadDomain's
     *             replicaCount)^2 <br>
     *             <i>These are individually squared to penalize higher values when summing up all
     *             values</i>
     *       </ul>
     *   <li>If AvailabilityZones are used:<br>
     *       1,000,000 * the sum over each collection/shard/replicaType:
     *       <ul>
     *         <li>(# of replicas in this node's AZ - the minimum AZ's replicaCount)^2 <br>
     *             <i>These are individually squared to penalize higher values when summing up all
     *             values</i>
     *       </ul>
     * </ul>
     *
     * The weighting here ensures that the order of importance for nodes is:
     *
     * <ol>
     *   <li>Spread replicas of the same shard/replicaType across availabilityZones
     *   <li>Spread replicas of the same shard across spreadDomains
     *   <li>Make sure that replicas are not placed on nodes that have < prioritizedFreeDiskGB disk
     *       space available
     *   <li>Minimize the amount of replicas on the node
     * </ol>
     *
     * <p>The "relevant" weight with a replica is the sum of:
     *
     * <ul>
     *   <li>The number of replicas on the node
     *   <li>100 if the projected free disk space on the node < prioritizedFreeDiskGB, otherwise 0
     *   <li>If SpreadDomains are used:<br>
     *       10,000 * ( # of replicas for the replica's shard in this node's spread domain - the
     *       minimum spreadDomain's replicaCount )
     *   <li>If AvailabilityZones are used:<br>
     *       1,000,000 * ( # of replicas for the replica's shard & replicaType in this node's AZ -
     *       the minimum AZ's replicaCount )
     * </ul>
     *
     * <p>Multiple replicas of the same shard are not permitted to live on the same Node.
     *
     * <p>Users can specify withCollection, to ensure that co-placement of replicas is ensured when
     * computing new replica placements or replica balancing.
     */
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
            // Only add 100 if prioritizedFreeDiskGB was provided and the node's freeDisk is lower
            // than it
            + 100 * (prioritizedFreeDiskGB > 0 && nodeFreeDiskGB < prioritizedFreeDiskGB ? 1 : 0)
            + 10000 * getSpreadDomainWeight()
            + 1000000 * getAZWeight();
      }

      @Override
      public int calcRelevantWeightWithReplica(Replica replica) {
        return coresOnNode
            // Only add 100 if prioritizedFreeDiskGB was provided and the node's projected freeDisk
            // is lower than it
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
        // By default, do not allow two replicas of the same shard on a node
        return super.canAddReplica(replica)
            // Filter out unsupported replica types
            && supportedReplicaTypes.contains(replica.getType())
            // Filter out unsupported node types
            && Optional.ofNullable(nodeTypes.get(collection))
                .map(s -> s.stream().anyMatch(nodeType::contains))
                .orElse(true)
            // Ensure any co-located collections already exist on the Node
            && Optional.ofNullable(withCollections.get(collection))
                .map(this::hasCollectionOnNode)
                .orElse(true)
            // Ensure same shard is collocated if required
            && Optional.ofNullable(withCollectionShards.get(collection))
                .map(
                    shardWiseOf ->
                        getShardsOnNode(shardWiseOf).contains(replica.getShard().getShardName()))
                .orElse(true)
            // Ensure the disk space will not go below the minimum if the replica is added
            && (minimalFreeDiskGB <= 0
                || nodeFreeDiskGB - getProjectedSizeOfReplica(replica) > minimalFreeDiskGB);
      }

      /**
       * Return any replicas that cannot be removed because there are collocated collections that
       * require the replica to exist.
       *
       * @param replicas the replicas to remove
       * @return any errors for replicas that cannot be removed
       */
      @Override
      public Map<Replica, String> canRemoveReplicas(Collection<Replica> replicas) {
        Map<Replica, String> replicaRemovalExceptions = new HashMap<>();
        Map<String, Map<String, Set<Replica>>> removals = new HashMap<>();
        for (Replica replica : replicas) {
          SolrCollection collection = replica.getShard().getCollection();
          Set<String> collocatedCollections = new HashSet<>();
          Optional.ofNullable(collocatedWith.get(collection.getName()))
              .ifPresent(collocatedCollections::addAll);
          collocatedCollections.retainAll(getCollectionsOnNode());
          if (collocatedCollections.isEmpty()) {
            continue;
          }

          Stream<String> shardWiseCollocations =
              collocatedCollections.stream()
                  .filter(
                      priColl -> collection.getName().equals(withCollectionShards.get(priColl)));
          final Set<String> mandatoryShardsOrAll =
              shardWiseCollocations
                  .flatMap(priColl -> getShardsOnNode(priColl).stream())
                  .collect(Collectors.toSet());
          // There are collocatedCollections for this shard, so make sure there is a replica of this
          // shard left on the node after it is removed
          Set<Replica> replicasRemovedForShard =
              removals
                  .computeIfAbsent(
                      replica.getShard().getCollection().getName(), k -> new HashMap<>())
                  .computeIfAbsent(replica.getShard().getShardName(), k -> new HashSet<>());
          replicasRemovedForShard.add(replica);
          // either if all shards are mandatory, or the current one is mandatory
          boolean shardWise = false;
          if (mandatoryShardsOrAll.isEmpty()
              || (shardWise = mandatoryShardsOrAll.contains(replica.getShard().getShardName()))) {
            if (replicasRemovedForShard.size()
                >= getReplicasForShardOnNode(replica.getShard()).size()) {
              replicaRemovalExceptions.put(
                  replica,
                  "co-located with replicas of "
                      + (shardWise ? replica.getShard().getShardName() + " of " : "")
                      + collocatedCollections);
            }
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
        // Only use AvailabilityZones if there are more than 1
        if (affinityPlacementContext.allAvailabilityZones.size() > 1) {
          needsResort |=
              affinityPlacementContext
                  .availabilityZoneUsage
                  .computeIfAbsent(
                      replica.getShard().getCollection().getName(), k -> new HashMap<>())
                  .computeIfAbsent(replica.getShard().getShardName(), k -> new HashMap<>())
                  .computeIfAbsent(
                      replica.getType(),
                      k -> new ReplicaSpread(affinityPlacementContext.allAvailabilityZones))
                  .addReplica(availabilityZone);
        }
        // Only use SpreadDomains if they have been provided to all nodes and there are more than 1
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
        // Only use AvailabilityZones if there are more than 1
        if (affinityPlacementContext.allAvailabilityZones.size() > 1) {
          Optional.ofNullable(
                  affinityPlacementContext.availabilityZoneUsage.get(
                      replica.getShard().getCollection().getName()))
              .map(m -> m.get(replica.getShard().getShardName()))
              .map(m -> m.get(replica.getType()))
              .ifPresent(m -> m.removeReplica(availabilityZone));
        }
        // Only use SpreadDomains if they have been provided to all nodes and there are more than 1
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
            .flatMap(lrm -> lrm.getReplicaMetric(ReplicaMetricImpl.INDEX_SIZE_GB))
            .orElse(0D);
      }

      /**
       * If there are more than one spreadDomains given in the cluster, then return a weight for
       * this node, given the number of replicas in its spreadDomain.
       *
       * <p>For each Collection & Shard, sum up the number of replicas this node's SpreadDomain has
       * over the minimum SpreadDomain. Square each value before summing, to ensure that smaller
       * number of higher values are penalized more than a larger number of smaller values.
       *
       * @return the weight
       */
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

      /**
       * If there are more than one SpreadDomains given in the cluster, then return a projected
       * SpreadDomain weight for this node and this replica.
       *
       * <p>For the new replica's Collection & Shard, project the number of replicas this node's
       * SpreadDomain has over the minimum SpreadDomain.
       *
       * @return the weight
       */
      private int projectReplicaSpreadWeight(Replica replica) {
        if (replica != null && affinityPlacementContext.doSpreadAcrossDomains) {
          return Optional.ofNullable(
                  affinityPlacementContext.spreadDomainUsage.get(
                      replica.getShard().getCollection().getName()))
              .map(m -> m.get(replica.getShard().getShardName()))
              .map(rs -> rs.projectOverMinimum(spreadDomain, 1))
              .orElse(0);
        } else {
          return 0;
        }
      }

      /**
       * If there are more than one AvailabilityZones given in the cluster, then return a weight for
       * this node, given the number of replicas in its availabilityZone.
       *
       * <p>For each Collection, Shard & ReplicaType, sum up the number of replicas this node's
       * AvailabilityZone has over the minimum AvailabilityZone. Square each value before summing,
       * to ensure that smaller number of higher values are penalized more than a larger number of
       * smaller values.
       *
       * @return the weight
       */
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

      /**
       * If there are more than one AvailabilityZones given in the cluster, then return a projected
       * AvailabilityZone weight for this node and this replica.
       *
       * <p>For the new replica's Collection, Shard & ReplicaType, project the number of replicas
       * this node's AvailabilityZone has over the minimum AvailabilityZone.
       *
       * @return the weight
       */
      private int projectAZWeight(Replica replica) {
        if (replica == null || affinityPlacementContext.allAvailabilityZones.size() < 2) {
          return 0;
        } else {
          return Optional.ofNullable(
                  affinityPlacementContext.availabilityZoneUsage.get(
                      replica.getShard().getCollection().getName()))
              .map(m -> m.get(replica.getShard().getShardName()))
              .map(m -> m.get(replica.getType()))
              .map(rs -> rs.projectOverMinimum(availabilityZone, 1))
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

      /**
       * Trying adding a replica for the given spread key, and return the {@link
       * #overMinimum(String)} with it added. Remove the replica, so that the state is unchanged
       * from when the method was called.
       */
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
