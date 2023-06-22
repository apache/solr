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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

public class ReplaceNodeCmd implements CollApiCmds.CollectionApiCommand {

  private final CollectionCommandContext ccc;

  public ReplaceNodeCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    ZkStateReader zkStateReader = ccc.getZkStateReader();
    String source = message.getStr(CollectionParams.SOURCE_NODE);
    String target = message.getStr(CollectionParams.TARGET_NODE);
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);
    if (source == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "sourceNode is a required param");
    }
    String async = message.getStr(ASYNC);
    int timeout = message.getInt("timeout", 10 * 60); // 10 minutes
    boolean parallel = message.getBool("parallel", false);
    ClusterState clusterState = zkStateReader.getClusterState();

    if (!clusterState.liveNodesContain(source)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Source Node: " + source + " is not live");
    }
    if (target != null && !clusterState.liveNodesContain(target)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Target Node: " + target + " is not live");
    } else if (clusterState.getLiveNodes().size() <= 1) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "No nodes other than the source node: "
              + source
              + " are live, therefore replicas cannot be moved");
    }
    List<Replica> sourceReplicas = ReplicaMigrationUtils.getReplicasOfNode(source, clusterState);
    Map<Replica, String> replicaMovements = CollectionUtil.newHashMap(sourceReplicas.size());

    if (target == null || target.isEmpty()) {
      List<Assign.AssignRequest> assignRequests = new ArrayList<>(sourceReplicas.size());
      for (Replica sourceReplica : sourceReplicas) {
        Replica.Type replicaType = sourceReplica.getType();
        int numNrtReplicas = replicaType == Replica.Type.NRT ? 1 : 0;
        int numTlogReplicas = replicaType == Replica.Type.TLOG ? 1 : 0;
        int numPullReplicas = replicaType == Replica.Type.PULL ? 1 : 0;
        Assign.AssignRequest assignRequest =
            new Assign.AssignRequestBuilder()
                .forCollection(sourceReplica.getCollection())
                .forShard(Collections.singletonList(sourceReplica.getShard()))
                .assignNrtReplicas(numNrtReplicas)
                .assignTlogReplicas(numTlogReplicas)
                .assignPullReplicas(numPullReplicas)
                .onNodes(
                    ccc.getSolrCloudManager().getClusterStateProvider().getLiveNodes().stream()
                        .filter(node -> !node.equals(source))
                        .collect(Collectors.toList()))
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
      for (Replica sourceReplica : sourceReplicas) {
        replicaMovements.put(sourceReplica, target);
      }
    }

    boolean migrationSuccessful =
        ReplicaMigrationUtils.migrateReplicas(
            ccc, replicaMovements, parallel, waitForFinalState, timeout, async, results);
    if (migrationSuccessful) {
      results.add(
          "success",
          "REPLACENODE action completed successfully from  : " + source + " to : " + target);
    }
  }
}
