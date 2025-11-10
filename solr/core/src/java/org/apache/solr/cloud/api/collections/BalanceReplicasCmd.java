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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.util.NamedList;

public class BalanceReplicasCmd implements CollApiCmds.CollectionApiCommand {
  private final CollectionCommandContext ccc;

  public BalanceReplicasCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @SuppressWarnings({"unchecked"})
  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    Set<String> nodes;
    Object nodesRaw = message.get(CollectionParams.NODES);
    if (nodesRaw == null) {
      nodes = Collections.emptySet();
    } else if (nodesRaw instanceof Set) {
      nodes = (Set<String>) nodesRaw;
    } else if (nodesRaw instanceof Collection) {
      nodes = new HashSet<>((Collection<String>) nodesRaw);
    } else if (nodesRaw instanceof String) {
      nodes = Set.of(((String) nodesRaw).split(","));
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "'nodes' was not passed as a correct type (Set/List/String): "
              + nodesRaw.getClass().getName());
    }
    boolean waitForFinalState = message.getBool(CommonAdminParams.WAIT_FOR_FINAL_STATE, false);
    String async = message.getStr(ASYNC);
    int timeout = message.getInt("timeout", 10 * 60); // 10 minutes
    boolean parallel = message.getBool("parallel", false);

    if (nodes.size() == 1) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Cannot balance across a single node: " + nodes.stream().findAny().get());
    }

    Assign.AssignStrategy assignStrategy = Assign.createAssignStrategy(ccc.getCoreContainer());
    Map<Replica, String> replicaMovements =
        assignStrategy.computeReplicaBalancing(
            ccc.getSolrCloudManager(),
            nodes,
            message.getInt(CollectionParams.MAX_BALANCE_SKEW, -1));

    boolean migrationSuccessful =
        ReplicaMigrationUtils.migrateReplicas(
            ccc, replicaMovements, parallel, waitForFinalState, timeout, async, results);
    if (migrationSuccessful) {
      results.add(
          "success",
          "BalanceReplicas action completed successfully across nodes  : ["
              + String.join(", ", nodes)
              + "]");
    }
  }
}
