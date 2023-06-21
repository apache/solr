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
import java.util.List;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.util.NamedList;

public class DeleteNodeCmd implements CollApiCmds.CollectionApiCommand {
  private final CollectionCommandContext ccc;

  public DeleteNodeCmd(CollectionCommandContext ccc) {
    this.ccc = ccc;
  }

  @Override
  public void call(ClusterState state, ZkNodeProps message, NamedList<Object> results)
      throws Exception {
    CollectionHandlingUtils.checkRequired(message, "node");
    String node = message.getStr("node");
    List<Replica> sourceReplicas = ReplicaMigrationUtils.getReplicasOfNode(node, state);
    List<String> singleReplicas = verifyReplicaAvailability(sourceReplicas, state);
    if (!singleReplicas.isEmpty()) {
      results.add(
          "failure",
          "Can't delete the only existing non-PULL replica(s) on node "
              + node
              + ": "
              + singleReplicas);
    } else {
      ReplicaMigrationUtils.cleanupReplicas(
          results, state, sourceReplicas, ccc, message.getStr(ASYNC));
    }
  }

  // collect names of replicas that cannot be deleted
  static List<String> verifyReplicaAvailability(List<Replica> sourceReplicas, ClusterState state) {
    List<String> res = new ArrayList<>();
    for (Replica sourceReplica : sourceReplicas) {
      String coll = sourceReplica.getCollection();
      String shard = sourceReplica.getShard();
      String replicaName = sourceReplica.getName();
      DocCollection collection = state.getCollection(coll);
      Slice slice = collection.getSlice(shard);
      if (slice.getReplicas().size() < 2) {
        // can't delete the only replica in existence
        res.add(coll + "/" + shard + "/" + replicaName + ", type=" + sourceReplica.getType());
      } else { // check replica types
        int otherNonPullReplicas = 0;
        for (Replica r : slice.getReplicas()) {
          if (!r.getName().equals(replicaName) && !r.getType().equals(Replica.Type.PULL)) {
            otherNonPullReplicas++;
          }
        }
        // can't delete - there are no other non-pull replicas
        if (otherNonPullReplicas == 0) {
          res.add(coll + "/" + shard + "/" + replicaName + ", type=" + sourceReplica.getType());
        }
      }
    }
    return res;
  }
}
