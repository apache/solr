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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.solr.cluster.placement.DeleteCollectionRequest;
import org.apache.solr.cluster.placement.DeleteReplicasRequest;
import org.apache.solr.cluster.placement.DeleteShardsRequest;
import org.apache.solr.cluster.placement.ModificationRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementModificationException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlanFactory;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.cluster.placement.ReplicaMetric;
import org.apache.solr.cluster.placement.ReplicaMetrics;
import org.apache.solr.cluster.placement.ReplicaPlacement;
import org.apache.solr.cluster.placement.ShardMetrics;
import org.apache.solr.cluster.placement.impl.BuiltInMetrics;
import org.apache.solr.cluster.placement.impl.WeightedNodeSelection;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
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
  static class AffinityPlacementPlugin implements PlacementPlugin {

    private final long minimalFreeDiskGB;

    private final long prioritizedFreeDiskGB;

    // primary to secondary (1:1)
    private final Map<String, String> withCollections;
    // secondary to primary (1:N)
    private final Map<String, Set<String>> colocatedWith;

    private final Map<String, Set<String>> nodeTypes;

    private final Random replicaPlacementRandom =
        new Random(); // ok even if random sequence is predictable.

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
        colocatedWith = Map.of();
      } else {
        colocatedWith = new HashMap<>();
        withCollections.forEach(
            (primary, secondary) ->
                colocatedWith.computeIfAbsent(secondary, s -> new HashSet<>()).add(primary));
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
      if (seed != null) {
        replicaPlacementRandom.setSeed(seed.hashCode());
      }
    }

    @Override
    @SuppressForbidden(
        reason =
            "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
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
      for (SolrCollection collection : allCollections) {
        attributeFetcher.requestCollectionMetrics(collection, replicaMetrics);
      }
      attributeFetcher.fetchFrom(allNodes);
      final AttributeValues attrValues = attributeFetcher.fetchAttributes();
      // Get the number of currently existing cores per node, so we can update as we place new cores
      // to not end up always selecting the same node(s). This is used across placement requests
      Map<Node, Integer> allCoresOnNodes = getCoreCountPerNode(allNodes, attrValues);

      boolean doSpreadAcrossDomains = shouldSpreadAcrossDomains(allNodes, attrValues);

      // Keep track with nodesWithReplicas across requests
      Map<String, Map<String, Set<Node>>> allNodesWithReplicas = new HashMap<>();
      for (PlacementRequest request : requests) {
        Set<Node> nodes = request.getTargetNodes();
        SolrCollection solrCollection = request.getCollection();

        // filter out nodes that don't meet the `withCollection` constraint
        nodes =
            filterNodesWithCollection(placementContext.getCluster(), request, attrValues, nodes);
        // filter out nodes that don't match the "node types" specified in the collection props
        nodes = filterNodesByNodeType(placementContext.getCluster(), request, attrValues, nodes);

        // All available zones of live nodes. Due to some nodes not being candidates for placement,
        // and some existing replicas being one availability zones that might be offline (i.e. their
        // nodes are not live), this set might contain zones on which it is impossible to place
        // replicas. That's ok.
        Set<String> availabilityZones = getZonesFromNodes(nodes, attrValues);

        // Build the replica placement decisions here
        Set<ReplicaPlacement> replicaPlacements = new HashSet<>();

        // Let's now iterate on all shards to create replicas for and start finding home sweet homes
        // for the replicas
        for (String shardName : request.getShardNames()) {
          ReplicaMetrics leaderMetrics =
              attrValues
                  .getCollectionMetrics(solrCollection.getName())
                  .flatMap(colMetrics -> colMetrics.getShardMetrics(shardName))
                  .flatMap(ShardMetrics::getLeaderMetrics)
                  .orElse(null);

          // Split the set of nodes into 3 sets of nodes accepting each replica type (sets can
          // overlap
          // if nodes accept multiple replica types). These subsets sets are actually maps, because
          // we
          // capture the number of cores (of any replica type) present on each node.
          //
          // This also filters out nodes that will not satisfy the rules if the replica is placed
          // there
          EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes =
              getAvailableNodesForReplicaTypes(nodes, attrValues, leaderMetrics);

          // Inventory nodes (if any) that already have a replica of any type for the shard, because
          // we can't be placing additional replicas on these. This data structure is updated after
          // each replica to node assign and is used to make sure different replica types are not
          // allocated to the same nodes (protecting same node assignments within a given replica
          // type is done "by construction" in makePlacementDecisions()).
          Set<Node> nodesWithReplicas =
              allNodesWithReplicas
                  .computeIfAbsent(solrCollection.getName(), col -> new HashMap<>())
                  .computeIfAbsent(
                      shardName,
                      s -> {
                        Set<Node> newNodeSet = new HashSet<>();
                        Shard shard = solrCollection.getShard(s);
                        if (shard != null) {
                          // Prefill the set with the existing replicas
                          for (Replica r : shard.replicas()) {
                            newNodeSet.add(r.getNode());
                          }
                        }
                        return newNodeSet;
                      });

          // Iterate on the replica types in the enum order. We place more strategic replicas first
          // (NRT is more strategic than TLOG more strategic than PULL). This is in case we
          // eventually decide that less strategic replica placement impossibility is not a problem
          // that should lead to replica placement computation failure. Current code does fail if
          // placement is impossible (constraint is at most one replica of a shard on any node).
          for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
            int numReplicasToCreate = request.getCountReplicasToCreate(replicaType);
            if (numReplicasToCreate > 0) {
              makePlacementDecisions(
                  solrCollection,
                  shardName,
                  availabilityZones,
                  replicaType,
                  numReplicasToCreate,
                  attrValues,
                  leaderMetrics,
                  replicaTypeToNodes,
                  nodesWithReplicas,
                  allCoresOnNodes,
                  placementContext.getPlacementPlanFactory(),
                  replicaPlacements,
                  doSpreadAcrossDomains);
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
        BalanceRequest balanceRequest, PlacementContext placementContext)
        throws PlacementException, InterruptedException {
      // This is a NO-OP
      return placementContext
          .getBalancePlanFactory()
          .createBalancePlan(balanceRequest, new HashMap<>());
    }

    private boolean shouldSpreadAcrossDomains(Set<Node> allNodes, AttributeValues attrValues) {
      boolean doSpreadAcrossDomains =
          spreadAcrossDomains && spreadDomainPropPresent(allNodes, attrValues);
      if (spreadAcrossDomains && !doSpreadAcrossDomains) {
        log.warn(
            "AffinityPlacementPlugin configured to spread across domains, but there are nodes in the cluster without the {} system property. Ignoring spreadAcrossDomains.",
            AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP);
      }
      return doSpreadAcrossDomains;
    }

    private boolean spreadDomainPropPresent(Set<Node> allNodes, AttributeValues attrValues) {
      // We can only use spread domains if all nodes have the system property
      return allNodes.stream()
          .noneMatch(
              n ->
                  attrValues
                      .getSystemProperty(n, AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP)
                      .isEmpty());
    }

    @Override
    public void verifyAllowedModification(
        ModificationRequest modificationRequest, PlacementContext placementContext)
        throws PlacementModificationException {
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

    private void verifyDeleteCollection(
        DeleteCollectionRequest deleteCollectionRequest, PlacementContext placementContext)
        throws PlacementModificationException {
      Cluster cluster = placementContext.getCluster();
      Set<String> colocatedCollections =
          colocatedWith.getOrDefault(deleteCollectionRequest.getCollection().getName(), Set.of());
      for (String primaryName : colocatedCollections) {
        try {
          if (cluster.getCollection(primaryName) != null) {
            // still exists
            throw new PlacementModificationException(
                "colocated collection "
                    + primaryName
                    + " of "
                    + deleteCollectionRequest.getCollection().getName()
                    + " still present");
          }
        } catch (IOException e) {
          throw new PlacementModificationException(
              "failed to retrieve colocated collection information", e);
        }
      }
    }

    private void verifyDeleteReplicas(
        DeleteReplicasRequest deleteReplicasRequest, PlacementContext placementContext)
        throws PlacementModificationException {
      Cluster cluster = placementContext.getCluster();
      SolrCollection secondaryCollection = deleteReplicasRequest.getCollection();
      Set<String> colocatedCollections = colocatedWith.get(secondaryCollection.getName());
      if (colocatedCollections == null) {
        return;
      }
      Map<Node, Map<String, AtomicInteger>> secondaryNodeShardReplicas = new HashMap<>();
      secondaryCollection
          .shards()
          .forEach(
              shard ->
                  shard
                      .replicas()
                      .forEach(
                          replica ->
                              secondaryNodeShardReplicas
                                  .computeIfAbsent(replica.getNode(), n -> new HashMap<>())
                                  .computeIfAbsent(
                                      replica.getShard().getShardName(), s -> new AtomicInteger())
                                  .incrementAndGet()));

      // find the colocated-with collections
      Map<Node, Set<String>> colocatingNodes = new HashMap<>();
      try {
        for (String colocatedCollection : colocatedCollections) {
          SolrCollection coll = cluster.getCollection(colocatedCollection);
          coll.shards()
              .forEach(
                  shard ->
                      shard
                          .replicas()
                          .forEach(
                              replica ->
                                  colocatingNodes
                                      .computeIfAbsent(replica.getNode(), n -> new HashSet<>())
                                      .add(coll.getName())));
        }
      } catch (IOException ioe) {
        throw new PlacementModificationException(
            "failed to retrieve colocated collection information", ioe);
      }
      PlacementModificationException exception = null;
      for (Replica replica : deleteReplicasRequest.getReplicas()) {
        if (!colocatingNodes.containsKey(replica.getNode())) {
          continue;
        }
        // check that there will be at least one replica remaining
        AtomicInteger secondaryCount =
            secondaryNodeShardReplicas
                .getOrDefault(replica.getNode(), Map.of())
                .getOrDefault(replica.getShard().getShardName(), new AtomicInteger());
        if (secondaryCount.get() > 1) {
          // we can delete it - record the deletion
          secondaryCount.decrementAndGet();
          continue;
        }
        // fail - this replica cannot be removed
        if (exception == null) {
          exception = new PlacementModificationException("delete replica(s) rejected");
        }
        exception.addRejectedModification(
            replica.toString(),
            "co-located with replicas of " + colocatingNodes.get(replica.getNode()));
      }
      if (exception != null) {
        throw exception;
      }
    }

    private Set<String> getZonesFromNodes(Set<Node> nodes, final AttributeValues attrValues) {
      Set<String> azs = new HashSet<>();

      for (Node n : nodes) {
        azs.add(getNodeAZ(n, attrValues));
      }

      return Collections.unmodifiableSet(azs);
    }

    /**
     * Resolves the AZ of a node and takes care of nodes that have no defined AZ in system property
     * {@link AffinityPlacementConfig#AVAILABILITY_ZONE_SYSPROP} to then return {@link
     * AffinityPlacementConfig#UNDEFINED_AVAILABILITY_ZONE} as the AZ name.
     */
    private String getNodeAZ(Node n, final AttributeValues attrValues) {
      Optional<String> nodeAz =
          attrValues.getSystemProperty(n, AffinityPlacementConfig.AVAILABILITY_ZONE_SYSPROP);
      // All nodes with undefined AZ will be considered part of the same AZ. This also works for
      // deployments that do not care about AZ's
      return nodeAz.orElse(AffinityPlacementConfig.UNDEFINED_AVAILABILITY_ZONE);
    }

    /**
     * This class captures an availability zone and the nodes that are legitimate targets for
     * replica placement in that Availability Zone. Instances are used as values in a {@link
     * java.util.TreeMap} in which the total number of already existing replicas in the AZ is the
     * key. This allows easily picking the set of nodes from which to select a node for placement in
     * order to balance the number of replicas per AZ. Picking one of the nodes from the set is done
     * using different criteria unrelated to the Availability Zone (picking the node is based on the
     * {@link CoresAndDiskComparator} ordering).
     */
    private static class AzWithNodes {
      final String azName;
      private final boolean useSpreadDomains;
      private boolean listIsSorted = false;
      private final Comparator<Node> nodeComparator;
      private final Random random;
      private final List<Node> availableNodesForPlacement;
      private final AttributeValues attributeValues;
      private final ReplicaMetrics leaderMetrics;
      private TreeSet<SpreadDomainWithNodes> sortedSpreadDomains;
      private final Map<String, Integer> currentSpreadDomainUsageUsage;
      private int numNodesForPlacement;

      AzWithNodes(
          String azName,
          List<Node> availableNodesForPlacement,
          boolean useSpreadDomains,
          Comparator<Node> nodeComparator,
          Random random,
          AttributeValues attributeValues,
          ReplicaMetrics leaderMetrics,
          Map<String, Integer> currentSpreadDomainUsageUsage) {
        this.azName = azName;
        this.availableNodesForPlacement = availableNodesForPlacement;
        this.useSpreadDomains = useSpreadDomains;
        this.nodeComparator = nodeComparator;
        this.random = random;
        this.attributeValues = attributeValues;
        this.leaderMetrics = leaderMetrics;
        this.currentSpreadDomainUsageUsage = currentSpreadDomainUsageUsage;
        this.numNodesForPlacement = availableNodesForPlacement.size();
      }

      private boolean hasBeenSorted() {
        return (useSpreadDomains && sortedSpreadDomains != null)
            || (!useSpreadDomains && listIsSorted);
      }

      void ensureSorted() {
        if (!hasBeenSorted()) {
          sort();
        }
      }

      private void sort() {
        assert !listIsSorted && sortedSpreadDomains == null
            : "We shouldn't be sorting this list again";

        // Make sure we do not tend to use always the same nodes (within an AZ) if all
        // conditions are identical (well, this likely is not the case since after having added
        // a replica to a node its number of cores increases for the next placement decision,
        // but let's be defensive here, given that multiple concurrent placement decisions might
        // see the same initial cluster state, and we want placement to be reasonable even in
        // that case without creating an unnecessary imbalance). For example, if all nodes have
        // 0 cores and same amount of free disk space, ideally we want to pick a random node for
        // placement, not always the same one due to some internal ordering.
        Collections.shuffle(availableNodesForPlacement, random);

        if (useSpreadDomains) {
          // When we use spread domains, we don't just sort the list of nodes, instead we generate a
          // TreeSet of SpreadDomainWithNodes,
          // sorted by the number of times the domain has been used. Each
          // SpreadDomainWithNodes internally contains the list of nodes that belong to that
          // particular domain,
          // and it's sorted internally by the comparator passed to this
          // class (which is the same that's used when not using spread domains).
          // Whenever a node from a particular SpreadDomainWithNodes is selected as the best
          // candidate, the call to "removeBestNode" will:
          // 1. Remove the SpreadDomainWithNodes instance from the TreeSet
          // 2. Remove the best node from the list within the SpreadDomainWithNodes
          // 3. Increment the count of times the domain has been used
          // 4. Re-add the SpreadDomainWithNodes instance to the TreeSet if there are still nodes
          // available
          HashMap<String, List<Node>> spreadDomainToListOfNodesMap = new HashMap<>();
          for (Node node : availableNodesForPlacement) {
            spreadDomainToListOfNodesMap
                .computeIfAbsent(
                    attributeValues
                        .getSystemProperty(node, AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP)
                        .get(),
                    k -> new ArrayList<>())
                .add(node);
          }
          sortedSpreadDomains =
              new TreeSet<>(new SpreadDomainComparator(currentSpreadDomainUsageUsage));

          int i = 0;
          for (Map.Entry<String, List<Node>> entry : spreadDomainToListOfNodesMap.entrySet()) {
            // Sort the nodes within the spread domain by the provided comparator
            entry.getValue().sort(nodeComparator);
            sortedSpreadDomains.add(
                new SpreadDomainWithNodes(entry.getKey(), entry.getValue(), i++, nodeComparator));
          }
        } else {
          availableNodesForPlacement.sort(nodeComparator);
          listIsSorted = true;
        }
      }

      Node getBestNode() {
        assert hasBeenSorted();
        if (useSpreadDomains) {
          return sortedSpreadDomains.first().sortedNodesForPlacement.get(0);
        } else {
          return availableNodesForPlacement.get(0);
        }
      }

      public Node removeBestNode() {
        assert hasBeenSorted();
        this.numNodesForPlacement--;
        Node n;
        if (useSpreadDomains) {
          // Since this SpreadDomainWithNodes needs to be re-sorted in the sortedSpreadDomains, we
          // remove it and then re-add it, once the best node has been removed.
          SpreadDomainWithNodes group = sortedSpreadDomains.pollFirst();
          n = group.sortedNodesForPlacement.remove(0);
          this.currentSpreadDomainUsageUsage.merge(group.spreadDomainName, 1, Integer::sum);
          if (!group.sortedNodesForPlacement.isEmpty()) {
            sortedSpreadDomains.add(group);
          }
        } else {
          n = availableNodesForPlacement.remove(0);
        }
        Optional.ofNullable(leaderMetrics)
            .flatMap(lrm -> lrm.getReplicaMetric(BuiltInMetrics.REPLICA_INDEX_SIZE_GB))
            .ifPresent(
                indexSize ->
                    attributeValues.decreaseNodeMetric(
                        n, BuiltInMetrics.NODE_FREE_DISK_GB, indexSize));
        attributeValues.increaseNodeMetric(n, BuiltInMetrics.NODE_NUM_CORES, 1);
        return n;
      }

      public int numNodes() {
        return this.numNodesForPlacement;
      }
    }

    /**
     * This class represents group of nodes with the same {@link
     * AffinityPlacementConfig#SPREAD_DOMAIN_SYSPROP} label.
     */
    static class SpreadDomainWithNodes implements Comparable<SpreadDomainWithNodes> {

      /**
       * This is the label that all nodes in this group have in {@link
       * AffinityPlacementConfig#SPREAD_DOMAIN_SYSPROP} label.
       */
      final String spreadDomainName;

      /**
       * The list of all nodes that contain the same {@link
       * AffinityPlacementConfig#SPREAD_DOMAIN_SYSPROP} label. They must be sorted before creating
       * this class.
       */
      private final List<Node> sortedNodesForPlacement;

      /**
       * This is used for tie breaking the sort of {@link SpreadDomainWithNodes}, when the
       * nodeComparator between the top nodes of each group return 0.
       */
      private final int tieBreaker;

      /**
       * This is the comparator that is used to compare the top nodes in the {@link
       * #sortedNodesForPlacement} lists. Must be the same that was used to sort {@link
       * #sortedNodesForPlacement}.
       */
      private final Comparator<Node> nodeComparator;

      public SpreadDomainWithNodes(
          String spreadDomainName,
          List<Node> sortedNodesForPlacement,
          int tieBreaker,
          Comparator<Node> nodeComparator) {
        this.spreadDomainName = spreadDomainName;
        this.sortedNodesForPlacement = sortedNodesForPlacement;
        this.tieBreaker = tieBreaker;
        this.nodeComparator = nodeComparator;
      }

      @Override
      public int compareTo(SpreadDomainWithNodes o) {
        if (o == this) {
          return 0;
        }
        int result =
            nodeComparator.compare(
                this.sortedNodesForPlacement.get(0), o.sortedNodesForPlacement.get(0));
        if (result == 0) {
          return Integer.compare(this.tieBreaker, o.tieBreaker);
        }
        return result;
      }
    }

    /**
     * Builds the number of existing cores on each node returned in the attrValues. Nodes for which
     * the number of cores is not available for whatever reason are excluded from acceptable
     * candidate nodes as it would not be possible to make any meaningful placement decisions.
     *
     * @param nodes all nodes on which this plugin should compute placement
     * @param attrValues attributes fetched for the nodes. This method uses system property {@link
     *     AffinityPlacementConfig#REPLICA_TYPE_SYSPROP} as well as the number of cores on each
     *     node.
     */
    private Map<Node, Integer> getCoreCountPerNode(
        Set<Node> nodes, final AttributeValues attrValues) {
      Map<Node, Integer> coresOnNodes = new HashMap<>();

      for (Node node : nodes) {
        attrValues
            .getNodeMetric(node, BuiltInMetrics.NODE_NUM_CORES)
            .ifPresent(count -> coresOnNodes.put(node, count));
      }

      return coresOnNodes;
    }

    /**
     * Given the set of all nodes on which to do placement and fetched attributes, builds the sets
     * representing candidate nodes for placement of replicas of each replica type. These sets are
     * packaged and returned in an EnumMap keyed by replica type. Nodes for which the number of
     * cores is not available for whatever reason are excluded from acceptable candidate nodes as it
     * would not be possible to make any meaningful placement decisions.
     *
     * @param nodes all nodes on which this plugin should compute placement
     * @param attrValues attributes fetched for the nodes. This method uses system property {@link
     *     AffinityPlacementConfig#REPLICA_TYPE_SYSPROP} as well as the number of cores on each
     *     node.
     */
    private EnumMap<Replica.ReplicaType, Set<Node>> getAvailableNodesForReplicaTypes(
        Set<Node> nodes, final AttributeValues attrValues, final ReplicaMetrics leaderMetrics) {
      EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes =
          new EnumMap<>(Replica.ReplicaType.class);

      for (Replica.ReplicaType replicaType : Replica.ReplicaType.values()) {
        replicaTypeToNodes.put(replicaType, new HashSet<>());
      }

      for (Node node : nodes) {
        // Exclude nodes with unknown or too small disk free space
        Optional<Double> nodeFreeDiskGB =
            attrValues.getNodeMetric(node, BuiltInMetrics.NODE_FREE_DISK_GB);
        if (nodeFreeDiskGB.isEmpty()) {
          if (log.isWarnEnabled()) {
            log.warn(
                "Unknown free disk on node {}, excluding it from placement decisions.",
                node.getName());
          }
          // We rely later on the fact that the free disk optional is present (see
          // CoresAndDiskComparator), be careful it you change anything here.
          continue;
        }
        double replicaIndexSize =
            Optional.ofNullable(leaderMetrics)
                .flatMap(lm -> lm.getReplicaMetric(BuiltInMetrics.REPLICA_INDEX_SIZE_GB))
                .orElse(0D);
        double projectedFreeDiskIfPlaced =
            BuiltInMetrics.NODE_FREE_DISK_GB.decrease(nodeFreeDiskGB.get(), replicaIndexSize);
        if (projectedFreeDiskIfPlaced < minimalFreeDiskGB) {
          if (log.isWarnEnabled()) {
            log.warn(
                "Node {} free disk ({}GB) minus the projected replica size ({}GB) is lower than configured"
                    + " minimum {}GB, excluding it from placement decisions.",
                node.getName(),
                nodeFreeDiskGB.get(),
                replicaIndexSize,
                minimalFreeDiskGB);
          }
          continue;
        }

        if (attrValues.getNodeMetric(node, BuiltInMetrics.NODE_NUM_CORES).isEmpty()) {
          if (log.isWarnEnabled()) {
            log.warn(
                "Unknown number of cores on node {}, excluding it from placement decisions.",
                node.getName());
          }
          // We rely later on the fact that the number of cores optional is present (see
          // CoresAndDiskComparator), be careful it you change anything here.
          continue;
        }

        String supportedReplicaTypes =
            attrValues
                    .getSystemProperty(node, AffinityPlacementConfig.REPLICA_TYPE_SYSPROP)
                    .isPresent()
                ? attrValues
                    .getSystemProperty(node, AffinityPlacementConfig.REPLICA_TYPE_SYSPROP)
                    .get()
                : null;
        // If property not defined or is only whitespace on a node, assuming node can take any
        // replica type
        if (supportedReplicaTypes == null || supportedReplicaTypes.isBlank()) {
          for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
            replicaTypeToNodes.get(rt).add(node);
          }
        } else {
          Set<String> acceptedTypes =
              Arrays.stream(supportedReplicaTypes.split(","))
                  .map(String::trim)
                  .map(s -> s.toLowerCase(Locale.ROOT))
                  .collect(Collectors.toSet());
          for (Replica.ReplicaType rt : Replica.ReplicaType.values()) {
            if (acceptedTypes.contains(rt.name().toLowerCase(Locale.ROOT))) {
              replicaTypeToNodes.get(rt).add(node);
            }
          }
        }
      }
      return replicaTypeToNodes;
    }

    /**
     * Picks nodes from {@code targetNodes} for placing {@code numReplicas} replicas.
     *
     * <p>The criteria used in this method are, in this order:
     *
     * <ol>
     *   <li>No more than one replica of a given shard on a given node (strictly enforced)
     *   <li>Balance as much as possible replicas of a given {@link
     *       org.apache.solr.cluster.Replica.ReplicaType} over available AZ's. This balancing takes
     *       into account existing replicas <b>of the corresponding replica type</b>, if any.
     *   <li>Place replicas if possible on nodes having more than a certain amount of free disk
     *       space (note that nodes with a too small amount of free disk space were eliminated as
     *       placement targets earlier, in {@link #getAvailableNodesForReplicaTypes(Set,
     *       AttributeValues, ReplicaMetrics)}). There's a threshold here rather than sorting on the
     *       amount of free disk space, because sorting on that value would in practice lead to
     *       never considering the number of cores on a node.
     *   <li>Place replicas on nodes having a smaller number of cores (the number of cores
     *       considered for this decision includes previous placement decisions made during the
     *       processing of the placement request)
     * </ol>
     */
    @SuppressForbidden(
        reason =
            "Ordering.arbitrary() has no equivalent in Comparator class. Rather reuse than copy.")
    private void makePlacementDecisions(
        SolrCollection solrCollection,
        String shardName,
        Set<String> availabilityZones,
        Replica.ReplicaType replicaType,
        int numReplicas,
        final AttributeValues attrValues,
        final ReplicaMetrics leaderMetrics,
        EnumMap<Replica.ReplicaType, Set<Node>> replicaTypeToNodes,
        Set<Node> nodesWithReplicas,
        Map<Node, Integer> coresOnNodes,
        PlacementPlanFactory placementPlanFactory,
        Set<ReplicaPlacement> replicaPlacements,
        boolean doSpreadAcrossDomains)
        throws PlacementException {
      // Count existing replicas per AZ. We count only instances of the type of replica for which we
      // need to do placement. If we ever want to balance replicas of any type across AZ's (and not
      // each replica type balanced independently), we'd have to move this data structure to the
      // caller of this method so it can be reused across different replica type placements for a
      // given shard. Note then that this change would be risky. For example all NRT's and PULL
      // replicas for a shard my be correctly balanced over three AZ's, but then all NRT can end up
      // in the same AZ...
      Map<String, Integer> azToNumReplicas = new HashMap<>();
      for (String az : availabilityZones) {
        azToNumReplicas.put(az, 0);
      }

      // Build the set of candidate nodes for the placement, i.e. nodes that can accept the replica
      // type
      Set<Node> candidateNodes = new HashSet<>(replicaTypeToNodes.get(replicaType));
      // Remove nodes that already have a replica for the shard (no two replicas of same shard can
      // be put on same node)
      candidateNodes.removeAll(nodesWithReplicas);

      // This Map will include the affinity labels for the nodes that are currently hosting replicas
      // of this shard. It will be modified with new placement decisions.
      Map<String, Integer> spreadDomainsInUse = new HashMap<>();
      Shard shard = solrCollection.getShard(shardName);
      if (shard != null) {
        // shard is non null if we're adding replicas to an already existing collection.
        // If we're creating the collection, the shards do not exist yet.
        for (Replica replica : shard.replicas()) {
          // The node's AZ is counted as having a replica if it has a replica of the same type as
          // the one we need to place here.
          if (replica.getType() == replicaType) {
            final String az = getNodeAZ(replica.getNode(), attrValues);
            if (azToNumReplicas.containsKey(az)) {
              // We do not count replicas on AZ's for which we don't have any node to place on
              // because it would not help the placement decision. If we did want to do that, note
              // the dereferencing below can't be assumed as the entry will not exist in the map.
              azToNumReplicas.put(az, azToNumReplicas.get(az) + 1);
            }
            if (doSpreadAcrossDomains) {
              spreadDomainsInUse.merge(
                  attrValues
                      .getSystemProperty(
                          replica.getNode(), AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP)
                      .get(),
                  1,
                  Integer::sum);
            }
          }
        }
      }

      // We now have the set of real candidate nodes, we've enforced "No more than one replica of a
      // given shard on a given node". We also counted for the shard and replica type under
      // consideration how many replicas were per AZ, so we can place (or try to place) replicas on
      // AZ's that have fewer replicas

      Map<String, List<Node>> nodesPerAz = new HashMap<>();
      if (availabilityZones.size() == 1) {
        // If AZs are not being used (all undefined for example) or a single AZ exists, we add all
        // nodes
        // to the same entry
        nodesPerAz.put(availabilityZones.iterator().next(), new ArrayList<>(candidateNodes));
      } else {
        // Get the candidate nodes per AZ in order to build (further down) a mapping of AZ to
        // placement candidates.
        for (Node node : candidateNodes) {
          String nodeAz = getNodeAZ(node, attrValues);
          List<Node> nodesForAz = nodesPerAz.computeIfAbsent(nodeAz, k -> new ArrayList<>());
          nodesForAz.add(node);
        }
      }

      Comparator<Node> interGroupNodeComparator =
          new CoresAndDiskComparator(attrValues, coresOnNodes, prioritizedFreeDiskGB);

      // Build a treeMap sorted by the number of replicas per AZ and including candidates nodes
      // suitable for placement on the AZ, so we can easily select the next AZ to get a replica
      // assignment and quickly (constant time) decide if placement on this AZ is possible or not.
      Map<Integer, Set<AzWithNodes>> azByExistingReplicas =
          new TreeMap<>(Comparator.naturalOrder());
      for (Map.Entry<String, List<Node>> e : nodesPerAz.entrySet()) {
        azByExistingReplicas
            .computeIfAbsent(azToNumReplicas.get(e.getKey()), k -> new HashSet<>())
            .add(
                new AzWithNodes(
                    e.getKey(),
                    e.getValue(),
                    doSpreadAcrossDomains,
                    interGroupNodeComparator,
                    replicaPlacementRandom,
                    attrValues,
                    leaderMetrics,
                    spreadDomainsInUse));
      }

      for (int i = 0; i < numReplicas; i++) {
        // We have for each AZ on which we might have a chance of placing a replica, the list of
        // candidate nodes for replicas (candidate: does not already have a replica of this shard
        // and is in the corresponding AZ). Among the AZ's with the minimal number of replicas of
        // the given replica type for the shard, we must pick the AZ that offers the best placement
        // (based on number of cores and free disk space). In order to do so, for these "minimal"
        // AZ's we sort the nodes from best to worst placement candidate (based on the number of
        // cores and free disk space) then pick the AZ that has the best best node. We don't sort
        // all AZ's because that will not necessarily be needed.
        int minNumberOfReplicasPerAz = 0; // This value never observed but compiler can't tell
        Set<Map.Entry<Integer, AzWithNodes>> candidateAzEntries = null;
        // Iterate over AZ's (in the order of increasing number of replicas on that AZ) and do two
        // things: 1. remove those AZ's that have no nodes, no use iterating over these again and
        // again (as we compute placement for more replicas), and 2. collect all those AZ with a
        // minimal number of replicas.
        for (Map.Entry<Integer, Set<AzWithNodes>> mapEntry : azByExistingReplicas.entrySet()) {
          Iterator<AzWithNodes> it = mapEntry.getValue().iterator();
          while (it.hasNext()) {
            Map.Entry<Integer, AzWithNodes> entry = Map.entry(mapEntry.getKey(), it.next());
            int numberOfNodes = entry.getValue().numNodes();
            if (numberOfNodes == 0) {
              it.remove();
            } else { // AZ does have node(s) for placement
              if (candidateAzEntries == null) {
                // First AZ with nodes that can take the replica. Initialize tracking structures
                minNumberOfReplicasPerAz = numberOfNodes;
                candidateAzEntries = new HashSet<>();
              }
              if (minNumberOfReplicasPerAz != numberOfNodes) {
                // AZ's with more replicas than the minimum number seen are not placement candidates
                break;
              }
              candidateAzEntries.add(entry);
              // We remove all entries that are candidates: the "winner" will be modified, all
              // entries
              // might also be sorted, so we'll insert back the updated versions later.
              it.remove();
            }
          }
        }

        if (candidateAzEntries == null) {
          // This can happen because not enough nodes for the placement request or already too many
          // nodes with replicas of the shard that can't accept new replicas or not enough nodes
          // with enough free disk space.
          throw new PlacementException(
              "Not enough eligible nodes to place "
                  + numReplicas
                  + " replica(s) of type "
                  + replicaType
                  + " for shard "
                  + shardName
                  + " of collection "
                  + solrCollection.getName());
        }

        // Iterate over all candidate AZ's, sort them if needed and find the best one to use for
        // this placement
        Map.Entry<Integer, AzWithNodes> selectedAz = null;
        Node selectedAzBestNode = null;
        for (Map.Entry<Integer, AzWithNodes> candidateAzEntry : candidateAzEntries) {
          AzWithNodes azWithNodes = candidateAzEntry.getValue();
          azWithNodes.ensureSorted();

          // Which one is better, the new one or the previous best?
          if (selectedAz == null
              || interGroupNodeComparator.compare(azWithNodes.getBestNode(), selectedAzBestNode)
                  < 0) {
            selectedAz = candidateAzEntry;
            selectedAzBestNode = azWithNodes.getBestNode();
          }
        }

        // Now actually remove the selected node from the winning AZ
        AzWithNodes azWithNodes = selectedAz.getValue();
        Node assignTarget = azWithNodes.removeBestNode();

        // Insert back all the qualifying but non winning AZ's removed while searching for the one
        for (Map.Entry<Integer, AzWithNodes> removedAzs : candidateAzEntries) {
          if (removedAzs != selectedAz) {
            azByExistingReplicas
                .computeIfAbsent(removedAzs.getKey(), k -> new HashSet<>())
                .add(removedAzs.getValue());
          }
        }

        // Insert back a corrected entry for the winning AZ: one more replica living there and one
        // less node that can accept new replicas (the remaining candidate node list might be empty,
        // in which case it will be cleaned up on the next iteration).
        azByExistingReplicas
            .computeIfAbsent(selectedAz.getKey() + 1, k -> new HashSet<>())
            .add(azWithNodes);

        // Do not assign that node again for replicas of other replica type for this shard (this
        // update of the set is not useful in the current execution of this method but for following
        // ones only)
        nodesWithReplicas.add(assignTarget);

        // Track that the node has one more core. These values are only used during the current run
        // of the plugin.
        coresOnNodes.merge(assignTarget, 1, Integer::sum);

        // Register the replica assignment just decided
        replicaPlacements.add(
            placementPlanFactory.createReplicaPlacement(
                solrCollection, shardName, assignTarget, replicaType));
      }
    }

    private Set<Node> filterNodesWithCollection(
        Cluster cluster,
        PlacementRequest request,
        AttributeValues attributeValues,
        Set<Node> initialNodes)
        throws PlacementException {
      // if there's a `withCollection` constraint for this collection then remove nodes
      // that are not eligible
      String withCollectionName = withCollections.get(request.getCollection().getName());
      if (withCollectionName == null) {
        return initialNodes;
      }
      SolrCollection withCollection;
      try {
        withCollection = cluster.getCollection(withCollectionName);
      } catch (Exception e) {
        throw new PlacementException(
            "Error getting info of withCollection=" + withCollectionName, e);
      }
      Set<Node> withCollectionNodes = new HashSet<>();
      withCollection
          .shards()
          .forEach(s -> s.replicas().forEach(r -> withCollectionNodes.add(r.getNode())));
      if (withCollectionNodes.isEmpty()) {
        throw new PlacementException(
            "Collection "
                + withCollection
                + " defined in `withCollection` has no replicas on eligible nodes.");
      }
      HashSet<Node> filteredNodes = new HashSet<>(initialNodes);
      filteredNodes.retainAll(withCollectionNodes);
      if (filteredNodes.isEmpty()) {
        throw new PlacementException(
            "Collection "
                + withCollection
                + " defined in `withCollection` has no replicas on eligible nodes.");
      }
      return filteredNodes;
    }

    private Set<Node> filterNodesByNodeType(
        Cluster cluster,
        PlacementRequest request,
        AttributeValues attributeValues,
        Set<Node> initialNodes)
        throws PlacementException {
      Set<String> collNodeTypes = nodeTypes.get(request.getCollection().getName());
      if (collNodeTypes == null) {
        // no filtering by node type
        return initialNodes;
      }
      Set<Node> filteredNodes =
          initialNodes.stream()
              .filter(
                  n -> {
                    Optional<String> nodePropOpt =
                        attributeValues.getSystemProperty(
                            n, AffinityPlacementConfig.NODE_TYPE_SYSPROP);
                    if (nodePropOpt.isEmpty()) {
                      return false;
                    }
                    Set<String> nodeTypes =
                        new HashSet<>(StrUtils.splitSmart(nodePropOpt.get(), ','));
                    nodeTypes.retainAll(collNodeTypes);
                    return !nodeTypes.isEmpty();
                  })
              .collect(Collectors.toSet());
      if (filteredNodes.isEmpty()) {
        throw new PlacementException(
            "There are no nodes with types: "
                + collNodeTypes
                + " expected by collection "
                + request.getCollection().getName());
      }
      return filteredNodes;
    }
    /**
     * Comparator implementing the placement strategy based on free space and number of cores: we
     * want to place new replicas on nodes with the less number of cores, but only if they do have
     * enough disk space (expressed as a threshold value).
     */
    static class CoresAndDiskComparator implements Comparator<Node> {
      private final AttributeValues attrValues;
      private final Map<Node, Integer> coresOnNodes;
      private final long prioritizedFreeDiskGB;

      /**
       * The data we sort on is not part of the {@link Node} instances but has to be retrieved from
       * the attributes and configuration. The number of cores per node is passed in a map whereas
       * the free disk is fetched from the attributes due to the fact that we update the number of
       * cores per node as we do allocations, but we do not update the free disk. The attrValues
       * corresponding to the number of cores per node are the initial values, but we want to
       * compare the actual value taking into account placement decisions already made during the
       * current execution of the placement plugin.
       */
      CoresAndDiskComparator(
          AttributeValues attrValues, Map<Node, Integer> coresOnNodes, long prioritizedFreeDiskGB) {
        this.attrValues = attrValues;
        this.coresOnNodes = coresOnNodes;
        this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
      }

      @Override
      public int compare(Node a, Node b) {
        // Note all nodes do have free disk defined. This has been verified earlier.
        boolean aHasLowFreeSpace =
            attrValues.getNodeMetric(a, BuiltInMetrics.NODE_FREE_DISK_GB).orElse(0D)
                < prioritizedFreeDiskGB;
        boolean bHasLowFreeSpace =
            attrValues.getNodeMetric(b, BuiltInMetrics.NODE_FREE_DISK_GB).orElse(0D)
                < prioritizedFreeDiskGB;
        if (aHasLowFreeSpace != bHasLowFreeSpace) {
          // A node with low free space should be considered > node with high free space since it
          // needs to come later in sort order
          return Boolean.compare(aHasLowFreeSpace, bHasLowFreeSpace);
        }
        // The ordering on the number of cores is the natural order.
        return Integer.compare(coresOnNodes.get(a), coresOnNodes.get(b));
      }
    }

    static class SpreadDomainComparator implements Comparator<SpreadDomainWithNodes> {
      private final Map<String, Integer> spreadDomainUsage;

      SpreadDomainComparator(Map<String, Integer> spreadDomainUsage) {
        this.spreadDomainUsage = spreadDomainUsage;
      }

      @Override
      public int compare(SpreadDomainWithNodes group1, SpreadDomainWithNodes group2) {
        // This comparator will compare groups by:
        // 1. The number of usages for the domain they represent: We want groups that are
        // less used to be the best ones
        // 2. On equal number of usages, by the internal comparator (which uses core count and disk
        // space) on the best node for each group (which, since the list is sorted, it's always the
        // one in the position 0)
        Integer usagesLabel1 = spreadDomainUsage.getOrDefault(group1.spreadDomainName, 0);
        Integer usagesLabel2 = spreadDomainUsage.getOrDefault(group2.spreadDomainName, 0);
        if (usagesLabel1.equals(usagesLabel2)) {
          return group1.compareTo(group2);
        }
        return usagesLabel1.compareTo(usagesLabel2);
      }
    }
  }

  private static class AffinityNodeContext {

    private final AttributeValues attrValues;

    private final boolean useSpreadDomains;
    private final Map<String, Integer> spreadDomainUsage;

    private final long minimalFreeDiskGB;

    private final long prioritizedFreeDiskGB;

    private AffinityNodeContext(
        AttributeValues attrValues, boolean useSpreadDomains, Map<String, Integer> spreadDomainUsage,
        long minimalFreeDiskGB, long prioritizedFreeDiskGB) {
      this.attrValues = attrValues;
      this.useSpreadDomains = useSpreadDomains;
      this.spreadDomainUsage = spreadDomainUsage;
      this.minimalFreeDiskGB = minimalFreeDiskGB;
      this.prioritizedFreeDiskGB = prioritizedFreeDiskGB;
    }

    AffinityNode newNodeFromMetrics(Node node) {
      Set<Replica.ReplicaType> supportedReplicaTypes =
          attrValues
              .getSystemProperty(node, AffinityPlacementConfig.REPLICA_TYPE_SYSPROP)
              .stream()
              .flatMap(s -> Arrays.stream(s.split(",")))
              .map(String::trim)
              .map(s -> s.toUpperCase(Locale.ROOT))
              .map(Replica.ReplicaType::valueOf)
              .collect(Collectors.toSet());
      if (supportedReplicaTypes.isEmpty()) {
        // If property not defined or is only whitespace on a node, assuming node can take any
        // replica type
        supportedReplicaTypes = Set.of(Replica.ReplicaType.values());
      }
      Optional<Double> nodeFreeDiskGB =
          attrValues.getNodeMetric(node, BuiltInMetrics.NODE_FREE_DISK_GB);
      Optional<Integer> nodeNumCores =
          attrValues.getNodeMetric(node, BuiltInMetrics.NODE_NUM_CORES);
      String spreadDomain = null;
      if (useSpreadDomains) {
        spreadDomain = attrValues.getSystemProperty(node, AffinityPlacementConfig.SPREAD_DOMAIN_SYSPROP).orElse(null);
        if (spreadDomain == null) {
          if (nodeFreeDiskGB.isEmpty()) {
            if (log.isWarnEnabled()) {
              log.warn(
                  "Unknown spreadDomain on node {}, excluding it from placement decisions.",
                  node.getName());
            }
          }
          return null;
        }
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
        return new AffinityNode(node, supportedReplicaTypes, nodeNumCores.get(), nodeFreeDiskGB.get(), spreadDomain);
      }
    }

    private class AffinityNode extends WeightedNodeSelection.WeightedNode {

      private final Set<Replica.ReplicaType> supportedReplicaTypes;

      private int coresOnNode;
      private double nodeFreeDiskGB;
      private final String spreadDomain;

      AffinityNode(Node node, Set<Replica.ReplicaType> supportedReplicaTypes,
                   int coresOnNode, double nodeFreeDiskGB, String spreadDomain) {
        super(node);
        this.supportedReplicaTypes = supportedReplicaTypes;
        this.coresOnNode = coresOnNode;
        this.nodeFreeDiskGB = nodeFreeDiskGB;
        this.spreadDomain = spreadDomain;
      }

      @Override
      public int getWeight() {
        return calculateWeight(coresOnNode, nodeFreeDiskGB, 0);
      }

      @Override
      public boolean canAddReplica(Replica replica) {
        return
            // By default, do not allow two replicas of the same shard on a node
            super.canAddReplica(replica) &&
            supportedReplicaTypes.contains(replica.getType()) &&
            getProjectedSizeOfReplica(replica) - nodeFreeDiskGB > minimalFreeDiskGB;
      }

      @Override
      public int getWeightWithReplica(Replica replica) {
        return calculateWeight(
            coresOnNode + 1,
            nodeFreeDiskGB - getProjectedSizeOfReplica(replica),
            1
        );
      }

      @Override
      protected void addProjectedReplicaWeights(Replica replica) {
        nodeFreeDiskGB -= getProjectedSizeOfReplica(replica);
        coresOnNode += 1;
        if (useSpreadDomains) {
          spreadDomainUsage.merge(spreadDomain, 1, Integer::sum);
        }
      }

      @Override
      public int getWeightWithoutReplica(Replica replica) {
        return calculateWeight(
            coresOnNode - 1,
            nodeFreeDiskGB + getProjectedSizeOfReplica(replica),
            -1
        );
      }

      @Override
      protected void removeProjectedReplicaWeights(Replica replica) {
        nodeFreeDiskGB += getProjectedSizeOfReplica(replica);
        coresOnNode -= 1;
        if (useSpreadDomains) {
          spreadDomainUsage.computeIfPresent(spreadDomain, (k,v) -> v - 1);
        }
      }

      private int calculateWeight(int cores, double freeDiskGB, int deltaReplicas) {
        return
            cores +
            100 * (freeDiskGB < prioritizedFreeDiskGB ? 1 : 0) +
            10000 * (useSpreadDomains ? (getSpreadDomainReplicas() + deltaReplicas) : 0);
      }

      private double getProjectedSizeOfReplica(Replica replica) {
        return attrValues
            .getCollectionMetrics(replica.getShard().getCollection().getName())
            .flatMap(colMetrics -> colMetrics.getShardMetrics(replica.getShard().getShardName()))
            .flatMap(ShardMetrics::getLeaderMetrics)
            .flatMap(lrm -> lrm.getReplicaMetric(BuiltInMetrics.REPLICA_INDEX_SIZE_GB))
            .orElse(0D);
      }

      private int getSpreadDomainReplicas() {
        return Optional.ofNullable(spreadDomain)
              .map(spreadDomainUsage::get)
              .orElse(0);
      }
    }
  }
}
