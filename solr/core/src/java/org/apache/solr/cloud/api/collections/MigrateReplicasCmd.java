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

package org.apache.solr.cloud.api.collections;

import static org.apache.solr.common.params.CommonAdminParams.ASYNC;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.NamedList;

public class MigrateReplicasCmd implements CollApiCmds.CollectionApiCommand {

  private final CollectionCommandContext ccc;

  public MigrateReplicasCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    ZkStateReader zkStateReader = ccc.getZkStateReader();
    Set<String> sourceNodes = getNodesFromParam(message, CollectionParams.SOURCE_NODES);
    Set<String> targetNodes = getNodesFromParam(message, CollectionParams.TARGET_NODES);
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);
    if (sourceNodes.isEmpty()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "sourceNodes is a required param");
    }
    String async = message.getStr(ASYNC);
    int timeout = message.getInt("timeout", 10 * 60); // 10 minutes
    boolean parallel = message.getBool("parallel", false);
    ClusterState clusterState = zkStateReader.getClusterState();

    for (String sourceNode : sourceNodes) {
      if (!clusterState.liveNodesContain(sourceNode)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Source Node: " + sourceNode + " is not live");
      }
    }
    for (String targetNode : targetNodes) {
      if (!clusterState.liveNodesContain(targetNode)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST, "Target Node: " + targetNode + " is not live");
      }
    }

    if (targetNodes.isEmpty()) {
      // If no target nodes are provided, use all other live nodes that are not the sourceNodes
      targetNodes =
          clusterState.getLiveNodes().stream()
              .filter(n -> !sourceNodes.contains(n))
              .collect(Collectors.toSet());
      if (targetNodes.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "No nodes other than the source nodes are live, therefore replicas cannot be migrated");
      }
    }
    List<Replica> sourceReplicas =
        ReplicaMigrationUtils.getReplicasOfNodes(sourceNodes, clusterState);
    Map<Replica, String> replicaMovements = CollectionUtil.newHashMap(sourceReplicas.size());

    if (targetNodes.size() > 1) {
      List<Assign.AssignRequest> assignRequests = new ArrayList<>(sourceReplicas.size());
      List<String> targetNodeList = new ArrayList<>(targetNodes);
      for (Replica sourceReplica : sourceReplicas) {
        Replica.Type replicaType = sourceReplica.getType();
        Assign.AssignRequest assignRequest =
            new Assign.AssignRequestBuilder()
                .forCollection(sourceReplica.getCollection())
                .forShard(Collections.singletonList(sourceReplica.getShard()))
                .assignNrtReplicas(replicaType == Replica.Type.NRT ? 1 : 0)
                .assignTlogReplicas(replicaType == Replica.Type.TLOG ? 1 : 0)
                .assignPullReplicas(replicaType == Replica.Type.PULL ? 1 : 0)
                .onNodes(targetNodeList)
                .build();
        assignRequests.add(assignRequest);
      }
      Assign.AssignStrategy assignStrategy = Assign.createAssignStrategy(ccc.getCoreContainer());
      List<ReplicaPosition> replicaPositions =
          assignStrategy.assign(ccc.getSolrCloudManager(), assignRequests);
      int position = 0;
      for (Replica sourceReplica : sourceReplicas) {
        replicaMovements.put(sourceReplica, replicaPositions.get(position++).node);
      }
    } else {
      String targetNode = targetNodes.stream().findFirst().get();
      for (Replica sourceReplica : sourceReplicas) {
        replicaMovements.put(sourceReplica, targetNode);
      }
    }

    boolean migrationSuccessful =
        ReplicaMigrationUtils.migrateReplicas(
            ccc, replicaMovements, parallel, waitForFinalState, timeout, async, results);
    if (migrationSuccessful) {
      results.add(
          "success",
          "MIGRATE_REPLICAS action completed successfully from  : ["
              + String.join(",", sourceNodes)
              + "] to : ["
              + String.join(",", targetNodes)
              + "]");
    }
  }

  @SuppressWarnings({"unchecked"})
  protected Set<String> getNodesFromParam(ZkNodeProps message, String paramName) {
    Object rawParam = message.get(paramName);
    if (rawParam == null) {
      return Collections.emptySet();
    } else if (rawParam instanceof Set) {
      return (Set<String>) rawParam;
    } else if (rawParam instanceof Collection) {
      return new HashSet<>((Collection<String>) rawParam);
    } else if (rawParam instanceof String) {
      return Set.of(((String) rawParam).split(","));
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'"
              + paramName
              + "' was not passed as a correct type (Set/List/String): "
              + rawParam.getClass().getName());
    }
  }
}
