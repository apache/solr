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
package org.apache.solr.cloud.overseer;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.PerReplicaStatesOps;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMutator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected SolrZkClient zkClient;

  public NodeMutator(SolrCloudManager cloudManager) {
    zkClient = SliceMutator.getZkClient(cloudManager);
  }

  public List<ZkWriteCommand> downNode(ClusterState clusterState, ZkNodeProps message) {
    String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);

    log.debug("DownNode state invoked for node: {}", nodeName);

    List<ZkWriteCommand> zkWriteCommands = new ArrayList<>();

    Map<String, DocCollection> collections = clusterState.getCollectionsMap();
    for (Map.Entry<String, DocCollection> entry : collections.entrySet()) {
      String collectionName = entry.getKey();
      DocCollection docCollection = entry.getValue();
      if (docCollection.isPerReplicaState()) continue;

      Optional<ZkWriteCommand> zkWriteCommand =
          computeCollectionUpdate(nodeName, collectionName, docCollection, zkClient);

      if (zkWriteCommand.isPresent()) {
        zkWriteCommands.add(zkWriteCommand.get());
      }
    }

    return zkWriteCommands;
  }

  /**
   * Returns the write command needed to update the replicas of a given collection given the
   * identity of a node being down.
   *
   * @return An optional with the write command or an empty one if the collection does not need any
   *     state modification. The returned write command might be for per replica state updates or
   *     for an update to state.json, depending on the configuration of the collection.
   */
  public static Optional<ZkWriteCommand> computeCollectionUpdate(
      String nodeName, String collectionName, DocCollection docCollection, SolrZkClient client) {
    boolean needToUpdateCollection = false;
    List<String> downedReplicas = new ArrayList<>();
    final Map<String, Slice> slicesCopy = new LinkedHashMap<>(docCollection.getSlicesMap());

    List<Replica> replicasOnNode = docCollection.getReplicas(nodeName);
    if (replicasOnNode == null || replicasOnNode.isEmpty()) {
      return Optional.empty();
    }
    for (Replica replica : replicasOnNode) {
      if (replica.getState() != Replica.State.DOWN) {
        log.debug("Update replica state for {} to {}", replica, Replica.State.DOWN);
        needToUpdateCollection = true;
        downedReplicas.add(replica.getName());
        slicesCopy.computeIfPresent(
            replica.getShard(),
            (name, slice) -> slice.copyWith(replica.copyWith(Replica.State.DOWN)));
      }
    }

    if (needToUpdateCollection) {
      if (docCollection.isPerReplicaState()) {
        PerReplicaStates prs =
            client == null
                ? docCollection.getPerReplicaStates()
                : PerReplicaStatesOps.fetch(
                    docCollection.getZNode(), client, docCollection.getPerReplicaStates());

        return Optional.of(
            new ZkWriteCommand(
                collectionName,
                docCollection.copyWithSlices(slicesCopy),
                PerReplicaStatesOps.downReplicas(downedReplicas, prs),
                false));
      } else {
        return Optional.of(
            new ZkWriteCommand(collectionName, docCollection.copyWithSlices(slicesCopy)));
      }
    } else {
      // No update needed for this collection
      return Optional.empty();
    }
  }
}
