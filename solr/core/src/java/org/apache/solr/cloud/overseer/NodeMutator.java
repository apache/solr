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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
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
import org.apache.solr.common.util.EnvUtils;
import org.apache.zookeeper.client.ZKClientConfig;
import org.apache.zookeeper.common.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeMutator {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  protected SolrZkClient zkClient;

  /**
   * DOWNNODE message property listing the collections that had a replica on the node when the node
   * sent the message.
   */
  public static final String DOWN_NODE_COLLECTIONS_PROP = "collections";

  private final boolean useListedCollectionsOnly;

  public NodeMutator(SolrCloudManager cloudManager) {
    zkClient = SliceMutator.getZkClient(cloudManager);
    useListedCollectionsOnly =
        cloudManager != null
            && Boolean.parseBoolean(
                String.valueOf(
                    cloudManager
                        .getClusterStateProvider()
                        .getClusterProperty(
                            ZkStateReader.USE_LISTED_COLLECTIONS_ONLY_FOR_DOWN_NODE)));
  }

  public List<ZkWriteCommand> downNode(ClusterState clusterState, ZkNodeProps message) {
    String nodeName = message.getStr(ZkStateReader.NODE_NAME_PROP);

    log.debug("DownNode state invoked for node: {}", nodeName);

    return collectionsToCheck(clusterState, message)
        .filter(entry -> !entry.isPerReplicaState())
        .map(docCollection -> computeCollectionUpdate(nodeName, docCollection, zkClient))
        .flatMap(Optional::stream)
        .toList();
  }

  /**
   * Returns the list a node puts in its DOWNNODE message under {@link #DOWN_NODE_COLLECTIONS_PROP},
   * or null when the names are too long to send, in which case the overseer loads every collection.
   */
  public static List<String> collectionsForDownNodeMessage(Collection<String> collections) {
    // ZooKeeper rejects a queue entry larger than jute.maxbuffer, which would lose the DOWNNODE
    // and leave the node's replicas in their last published state
    int maxBytes = maxListedCollectionsBytes();
    long bytes = 0;
    for (String collection : collections) {
      // 3 for the quotes and the comma
      bytes += collection.getBytes(StandardCharsets.UTF_8).length + 3;
      if (bytes > maxBytes) {
        return null;
      }
    }
    return List.copyOf(collections);
  }

  /**
   * The most bytes of collection names a DOWNNODE message may list: half of this node's
   * jute.maxbuffer, which leaves room for the rest of the message and is expected to match the
   * limit the ZooKeeper servers enforce.
   */
  private static int maxListedCollectionsBytes() {
    String juteMaxBuffer = EnvUtils.getProperty(ZKConfig.JUTE_MAXBUFFER);
    int limit = ZKClientConfig.CLIENT_MAX_PACKET_LENGTH_DEFAULT;
    if (juteMaxBuffer != null) {
      try {
        // ZooKeeper reads it with Integer.getInteger, which takes hex and octal too
        limit = Integer.decode(juteMaxBuffer.trim());
      } catch (NumberFormatException e) {
        // ZooKeeper falls back to its default as well
      }
    }
    return limit / 2;
  }

  /**
   * The collections a DOWNNODE has to look at. Loading a collection the overseer hasn't written yet
   * is a ZooKeeper round trip, so when the message lists the node's collections only those are
   * loaded. A message without the list (from a node that doesn't send it, or whose list was too
   * long) loads every collection. A replica added to the node after it built the list is not looked
   * at.
   */
  private Stream<DocCollection> collectionsToCheck(ClusterState clusterState, ZkNodeProps message) {
    Object collectionsFromNode = message.get(DOWN_NODE_COLLECTIONS_PROP);
    if (!useListedCollectionsOnly || collectionsFromNode == null) {
      return clusterState.collectionStream();
    }
    return ((Collection<?>) collectionsFromNode)
        .stream()
            .map(String::valueOf)
            .distinct()
            .map(clusterState::getCollectionOrNull)
            .filter(Objects::nonNull);
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
      String nodeName, DocCollection docCollection, SolrZkClient client) {
    boolean needToUpdateCollection = false;
    List<String> downedReplicas = new ArrayList<>();
    final Map<String, Slice> slicesCopy = new LinkedHashMap<>(docCollection.getSlicesMap());

    List<Replica> replicasOnNode = docCollection.getReplicasOnNode(nodeName);
    if (replicasOnNode.isEmpty()) {
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
                docCollection.getName(),
                docCollection.copyWithSlices(slicesCopy),
                PerReplicaStatesOps.downReplicas(downedReplicas, prs),
                false));
      } else {
        return Optional.of(
            new ZkWriteCommand(docCollection.getName(), docCollection.copyWithSlices(slicesCopy)));
      }
    } else {
      // No update needed for this collection
      return Optional.empty();
    }
  }
}
