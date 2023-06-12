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

package org.apache.solr.cluster.placement.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.cloud.api.collections.Assign;
import org.apache.solr.cluster.Node;
import org.apache.solr.cluster.placement.BalanceRequest;
import org.apache.solr.cluster.placement.DeleteCollectionRequest;
import org.apache.solr.cluster.placement.DeleteReplicasRequest;
import org.apache.solr.cluster.placement.PlacementContext;
import org.apache.solr.cluster.placement.PlacementException;
import org.apache.solr.cluster.placement.PlacementPlan;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.PlacementRequest;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This assign strategy delegates placement computation to "plugin" code. */
public class PlacementPluginAssignStrategy implements Assign.AssignStrategy {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final PlacementPlugin plugin;

  public PlacementPluginAssignStrategy(PlacementPlugin plugin) {
    this.plugin = plugin;
  }

  @Override
  public List<ReplicaPosition> assign(
      SolrCloudManager solrCloudManager, List<Assign.AssignRequest> assignRequests)
      throws Assign.AssignmentException, IOException, InterruptedException {

    PlacementContext placementContext = new SimplePlacementContextImpl(solrCloudManager);
    if (assignRequests.size() == 1
        && assignRequests.get(0).collectionName.startsWith(Assign.SYSTEM_COLL_PREFIX)) {
      // this is a system collection
      Assign.AssignRequest assignRequest = assignRequests.get(0);
      if (assignRequest.nodes != null && !assignRequest.nodes.isEmpty()) {
        return computeSystemCollectionPositions(placementContext, assignRequest);
      }
    }

    List<PlacementRequest> placementRequests = new ArrayList<>(assignRequests.size());
    for (Assign.AssignRequest assignRequest : assignRequests) {
      placementRequests.add(
          PlacementRequestImpl.toPlacementRequest(
              placementContext.getCluster(),
              placementContext.getCluster().getCollection(assignRequest.collectionName),
              assignRequest));
    }

    final List<ReplicaPosition> replicaPositions = new ArrayList<>();
    try {
      List<PlacementPlan> placementPlans =
          plugin.computePlacements(placementRequests, placementContext);
      if (placementPlans != null) {
        for (PlacementPlan placementPlan : placementPlans) {
          replicaPositions.addAll(
              ReplicaPlacementImpl.toReplicaPositions(
                  placementPlan.getRequest().getCollection().getName(),
                  placementPlan.getReplicaPlacements()));
        }
      }
    } catch (PlacementException pe) {
      throw new Assign.AssignmentException(pe);
    }

    return replicaPositions;
  }

  @Override
  public Map<Replica, String> computeReplicaBalancing(
      SolrCloudManager solrCloudManager, Set<String> nodes, int maxBalanceSkew)
      throws Assign.AssignmentException, IOException, InterruptedException {
    PlacementContext placementContext = new SimplePlacementContextImpl(solrCloudManager);

    BalanceRequest balanceRequest =
        BalanceRequestImpl.create(placementContext.getCluster(), nodes, maxBalanceSkew);
    try {
      Map<org.apache.solr.cluster.Replica, Node> rawReplicaMovements =
          plugin.computeBalancing(balanceRequest, placementContext).getReplicaMovements();
      Map<Replica, String> replicaMovements = CollectionUtil.newHashMap(rawReplicaMovements.size());
      for (Map.Entry<org.apache.solr.cluster.Replica, Node> movement :
          rawReplicaMovements.entrySet()) {
        Replica converted = findReplica(solrCloudManager, movement.getKey());
        if (converted == null) {
          throw new Assign.AssignmentException(
              "Could not find replica when balancing: " + movement.getKey().toString());
        }
        replicaMovements.put(converted, movement.getValue().getName());
      }
      return replicaMovements;
    } catch (PlacementException pe) {
      throw new Assign.AssignmentException(pe);
    }
  }

  private Replica findReplica(
      SolrCloudManager solrCloudManager, org.apache.solr.cluster.Replica replica) {
    DocCollection collection = null;
    try {
      collection =
          solrCloudManager
              .getClusterState()
              .getCollection(replica.getShard().getCollection().getName());
    } catch (IOException e) {
      throw new Assign.AssignmentException(
          "Could not load cluster state when balancing replicas", e);
    }
    if (collection != null) {
      Slice slice = collection.getSlice(replica.getShard().getShardName());
      if (slice != null) {
        return slice.getReplica(replica.getReplicaName());
      }
    }
    return null;
  }

  @Override
  public void verifyDeleteCollection(SolrCloudManager solrCloudManager, DocCollection collection)
      throws Assign.AssignmentException, IOException, InterruptedException {
    PlacementContext placementContext = new SimplePlacementContextImpl(solrCloudManager);
    DeleteCollectionRequest modificationRequest =
        ModificationRequestImpl.createDeleteCollectionRequest(collection);
    try {
      plugin.verifyAllowedModification(modificationRequest, placementContext);
    } catch (PlacementException pe) {
      throw new Assign.AssignmentException(pe);
    }
  }

  @Override
  public void verifyDeleteReplicas(
      SolrCloudManager solrCloudManager,
      DocCollection collection,
      String shardId,
      Set<Replica> replicas)
      throws Assign.AssignmentException, IOException, InterruptedException {
    PlacementContext placementContext = new SimplePlacementContextImpl(solrCloudManager);
    DeleteReplicasRequest modificationRequest =
        ModificationRequestImpl.createDeleteReplicasRequest(collection, shardId, replicas);
    try {
      plugin.verifyAllowedModification(modificationRequest, placementContext);
    } catch (PlacementException pe) {
      throw new Assign.AssignmentException(pe);
    }
  }

  /** Very minimal placement logic for System collections */
  private static List<ReplicaPosition> computeSystemCollectionPositions(
      PlacementContext placementContext, Assign.AssignRequest assignRequest) throws IOException {
    Set<Node> nodes = SimpleClusterAbstractionsImpl.NodeImpl.getNodes(assignRequest.nodes);
    for (Node n : nodes) {
      if (!placementContext.getCluster().getLiveNodes().contains(n)) {
        throw new Assign.AssignmentException(
            "Bad assign request: specified node is not a live node ("
                + n.getName()
                + ") for collection "
                + assignRequest.collectionName);
      }
    }
    PlacementRequestImpl request =
        new PlacementRequestImpl(
            placementContext.getCluster().getCollection(assignRequest.collectionName),
            new HashSet<>(assignRequest.shardNames),
            nodes,
            assignRequest.numNrtReplicas,
            assignRequest.numTlogReplicas,
            assignRequest.numPullReplicas);
    final List<ReplicaPosition> replicaPositions = new ArrayList<>();
    ArrayList<Node> nodeList = new ArrayList<>(request.getTargetNodes());
    for (String shard : request.getShardNames()) {
      int replicaNumOfShard = 0;
      for (org.apache.solr.cluster.Replica.ReplicaType replicaType :
          org.apache.solr.cluster.Replica.ReplicaType.values()) {
        for (int i = 0; i < request.getCountReplicasToCreate(replicaType); i++) {
          Node assignedNode = nodeList.get(replicaNumOfShard++ % nodeList.size());
          replicaPositions.add(
              new ReplicaPosition(
                  request.getCollection().getName(),
                  shard,
                  i,
                  SimpleClusterAbstractionsImpl.ReplicaImpl.toCloudReplicaType(replicaType),
                  assignedNode.getName()));
        }
      }
    }
    return replicaPositions;
  }
}
