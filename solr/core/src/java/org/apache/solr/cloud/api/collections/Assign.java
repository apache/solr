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

import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.common.cloud.ZkStateReader.CORE_NAME_PROP;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.*;

import org.apache.solr.client.solrj.cloud.DistribStateManager;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.cloud.AlreadyExistsException;
import org.apache.solr.client.solrj.cloud.BadVersionException;
import org.apache.solr.client.solrj.cloud.VersionedData;
import org.apache.solr.cluster.placement.PlacementPlugin;
import org.apache.solr.cluster.placement.impl.PlacementPluginAssignStrategy;
import org.apache.solr.cluster.placement.plugins.SimplePlacementFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ReplicaPosition;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeRoles;
import org.apache.solr.handler.ClusterAPI;
import org.apache.solr.util.NumberUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Assign {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static String getCounterNodePath(String collection) {
    return ZkStateReader.COLLECTIONS_ZKNODE + "/"+collection+"/counter";
  }

  public static int incAndGetId(DistribStateManager stateManager, String collection, int defaultValue) {
    String path = ZkStateReader.COLLECTIONS_ZKNODE + "/"+collection;
    try {
      if (!stateManager.hasData(path)) {
        try {
          stateManager.makePath(path);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
      path += "/counter";
      if (!stateManager.hasData(path)) {
        try {
          stateManager.createData(path, NumberUtils.intToBytes(defaultValue), CreateMode.PERSISTENT);
        } catch (AlreadyExistsException e) {
          // it's okay if another beats us creating the node
        }
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating counter node in Zookeeper for collection:" + collection, e);
    } catch (IOException | KeeperException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error creating counter node in Zookeeper for collection:" + collection, e);
    }

    while (true) {
      try {
        int version = 0;
        int currentId = 0;
        VersionedData data = stateManager.getData(path, null);
        if (data != null) {
          currentId = NumberUtils.bytesToInt(data.getData());
          version = data.getVersion();
        }
        byte[] bytes = NumberUtils.intToBytes(++currentId);
        stateManager.setData(path, bytes, version);
        return currentId;
      } catch (BadVersionException e) {
        continue;
      } catch (IOException | KeeperException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error inc and get counter from Zookeeper for collection:"+collection, e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error inc and get counter from Zookeeper for collection:" + collection, e);
      }
    }
  }

  public static String assignCoreNodeName(DistribStateManager stateManager, DocCollection collection) {
    // for backward compatibility;
    int defaultValue = defaultCounterValue(collection, false);
    String coreNodeName = "core_node" + incAndGetId(stateManager, collection.getName(), defaultValue);
    while (collection.getReplica(coreNodeName) != null) {
      // there is wee chance that, the new coreNodeName id not totally unique,
      // but this will be guaranteed unique for new collections
      coreNodeName = "core_node" + incAndGetId(stateManager, collection.getName(), defaultValue);
    }
    return coreNodeName;
  }

  /**
   * Assign a new unique id up to slices count - then add replicas evenly.
   *
   * @return the assigned shard id
   */
  public static String assignShard(DocCollection collection, Integer numShards) {
    if (numShards == null) {
      numShards = 1;
    }
    String returnShardId = null;
    Map<String, Slice> sliceMap = collection != null ? collection.getActiveSlicesMap() : null;


    // TODO: now that we create shards ahead of time, is this code needed?  Esp since hash ranges aren't assigned when creating via this method?

    if (sliceMap == null) {
      return "shard1";
    }

    List<String> shardIdNames = new ArrayList<>(sliceMap.keySet());

    if (shardIdNames.size() < numShards) {
      return "shard" + (shardIdNames.size() + 1);
    }

    // TODO: don't need to sort to find shard with fewest replicas!

    // else figure out which shard needs more replicas
    final Map<String, Integer> map = new HashMap<>();
    for (String shardId : shardIdNames) {
      int cnt = sliceMap.get(shardId).getReplicasMap().size();
      map.put(shardId, cnt);
    }

    Collections.sort(shardIdNames, (String o1, String o2) -> {
      Integer one = map.get(o1);
      Integer two = map.get(o2);
      return one.compareTo(two);
    });

    returnShardId = shardIdNames.get(0);
    return returnShardId;
  }

  public static String buildSolrCoreName(String collectionName, String shard, Replica.Type type, int replicaNum) {
    // TODO: Adding the suffix is great for debugging, but may be an issue if at some point we want to support a way to change replica type
    return String.format(Locale.ROOT, "%s_%s_replica_%s%s", collectionName, shard, type.name().substring(0,1).toLowerCase(Locale.ROOT), replicaNum);
  }

  private static int defaultCounterValue(DocCollection collection, boolean newCollection, String shard) {
    if (newCollection || collection == null) return 0;

    int defaultValue;
    if (collection.getSlice(shard) != null && collection.getSlice(shard).getReplicas().isEmpty()) {
      return 0;
    } else {
      defaultValue = collection.getReplicas().size() * 2;
    }

    if (collection.getReplicationFactor() != null) {
      // numReplicas and replicationFactor * numSlices can be not equals,
      // in case of many addReplicas or deleteReplicas are executed
      defaultValue = Math.max(defaultValue,
          collection.getReplicationFactor() * collection.getSlices().size());
    }
    return defaultValue;
  }

  private static int defaultCounterValue(DocCollection collection, boolean newCollection) {
    if (newCollection) return 0;
    int defaultValue = collection.getReplicas().size();
    return defaultValue;
  }

  public static String buildSolrCoreName(DistribStateManager stateManager, String collectionName, DocCollection collection, String shard, Replica.Type type, boolean newCollection) {

    int defaultValue = defaultCounterValue(collection, newCollection, shard);
    int replicaNum = incAndGetId(stateManager, collectionName, defaultValue);
    String coreName = buildSolrCoreName(collectionName, shard, type, replicaNum);
    while (collection != null && existCoreName(coreName, collection.getSlice(shard))) {
      replicaNum = incAndGetId(stateManager, collectionName, defaultValue);
      coreName = buildSolrCoreName(collectionName, shard, type, replicaNum);
    }
    return coreName;
  }

  public static String buildSolrCoreName(DistribStateManager stateManager, DocCollection collection, String shard, Replica.Type type) {
    return buildSolrCoreName(stateManager, collection.getName(), collection, shard, type, false);
  }

  private static boolean existCoreName(String coreName, Slice slice) {
    if (slice == null) return false;
    for (Replica replica : slice.getReplicas()) {
      if (coreName.equals(replica.getStr(CORE_NAME_PROP))) {
        return true;
      }
    }
    return false;
  }

  public static List<String> getLiveOrLiveAndCreateNodeSetList(final Set<String> liveNodes, final ZkNodeProps message, final Random random,
                                                               DistribStateManager zk) {

    List<String> nodeList;
    final String createNodeSetStr = message.getStr(CREATE_NODE_SET);
    final List<String> createNodeList = (createNodeSetStr == null) ? null :
        StrUtils.splitSmart((CollectionHandlingUtils.CREATE_NODE_SET_EMPTY.equals(createNodeSetStr) ?
            "" : createNodeSetStr), ",", true);

    if (createNodeList != null) {
      nodeList = new ArrayList<>(createNodeList);
      nodeList.retainAll(liveNodes);
      if (message.getBool(CollectionHandlingUtils.CREATE_NODE_SET_SHUFFLE,
          CollectionHandlingUtils.CREATE_NODE_SET_SHUFFLE_DEFAULT)) {
        Collections.shuffle(nodeList, random);
      }
    } else {
      nodeList = new ArrayList<>(filterNonDataNodes(zk, liveNodes));
      Collections.shuffle(nodeList, random);
    }

    return nodeList;
  }

  public static Collection<String> filterNonDataNodes(DistribStateManager zk, Collection<String> liveNodes) {
    try {
     List<String> noData =  ClusterAPI.getNodesByRole(NodeRoles.Role.DATA, NodeRoles.MODE_OFF, zk);
      if (noData.isEmpty()) {
        return liveNodes;
      } else {
        liveNodes = new HashSet<>(liveNodes);
        liveNodes.removeAll(noData);
        return liveNodes;
      }
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error fetching roles from Zookeeper", e);
    }
  }



  // Only called from addReplica (and by extension createShard) (so far).
  //
  // Gets a list of candidate nodes to put the required replica(s) on. Throws errors if the AssignStrategy
  // can't allocate valid positions.
  @SuppressWarnings({"unchecked"})
  public static List<ReplicaPosition> getNodesForNewReplicas(ClusterState clusterState, String collectionName,
                                                          String shard, int nrtReplicas, int tlogReplicas, int pullReplicas,
                                                          Object createNodeSet, SolrCloudManager cloudManager,
                                                          CoreContainer coreContainer) throws IOException, InterruptedException, AssignmentException {
    log.debug("getNodesForNewReplicas() shard: {} , nrtReplicas : {} , tlogReplicas: {} , pullReplicas: {} , createNodeSet {}"
        , shard, nrtReplicas, tlogReplicas, pullReplicas, createNodeSet);
    List<String> createNodeList;

    if (createNodeSet instanceof List) {
      createNodeList = (List<String>) createNodeSet;
    } else {
      // deduplicate
      createNodeList = createNodeSet == null ? null : new ArrayList<>(new LinkedHashSet<>(StrUtils.splitSmart((String) createNodeSet, ",", true)));
    }

    // produces clear message when down nodes are the root cause, without this the user just
    // gets a log message of detail about the nodes that are up, and a message that policies could not
    // be satisfied which then requires study to diagnose the issue.
    checkLiveNodes(createNodeList,clusterState);

    AssignRequest assignRequest = new AssignRequestBuilder()
        .forCollection(collectionName)
        .forShard(Collections.singletonList(shard))
        .assignNrtReplicas(nrtReplicas)
        .assignTlogReplicas(tlogReplicas)
        .assignPullReplicas(pullReplicas)
        .onNodes(createNodeList)
        .build();
    AssignStrategy assignStrategy = createAssignStrategy(coreContainer);
    return assignStrategy.assign(cloudManager, assignRequest);
  }

  // throw an exception if any node in the supplied list is not live.
  // Empty or null list always succeeds and returns the input.
  private static List<String> checkLiveNodes(List<String> createNodeList, ClusterState clusterState) {
    Set<String> liveNodes = clusterState.getLiveNodes();
    if (createNodeList != null) {
      if (!liveNodes.containsAll(createNodeList)) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
            "At least one of the node(s) specified " + createNodeList + " are not currently active in "
                + liveNodes + ", no action taken.");
      }
      // the logic that was extracted to this method used to create a defensive copy but no code
      // was modifying the copy, if this method is made protected or public we want to go back to that
    }
    return createNodeList; // unmodified, but return for inline use
  }

  /**
   * Thrown if there is an exception while assigning nodes for replicas
   */
  public static class AssignmentException extends RuntimeException {
    public AssignmentException() {
    }

    public AssignmentException(String message) {
      super(message);
    }

    public AssignmentException(String message, Throwable cause) {
      super(message, cause);
    }

    public AssignmentException(Throwable cause) {
      super(cause);
    }

    public AssignmentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
    }
  }

  /**
   * Strategy for assigning replicas to nodes.
   */
  public interface AssignStrategy {

    /**
     * Assign new replicas to nodes.
     * If multiple {@link AssignRequest}s are provided, then every {@link ReplicaPosition} made for an
     * {@link AssignRequest} will be applied to the {@link SolrCloudManager}'s state when processing subsequent {@link AssignRequest}s.
     * Therefore, the order in which {@link AssignRequest}s are provided can and will affect the {@link ReplicaPosition}s returned.
     *
     * @param solrCloudManager current instance of {@link SolrCloudManager}.
     * @param assignRequests assign request.
     * @return list of {@link ReplicaPosition}-s for new replicas.
     * @throws AssignmentException when assignment request cannot produce any valid assignments.
     */
    default List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, AssignRequest... assignRequests)
        throws AssignmentException, IOException, InterruptedException {
      return assign(solrCloudManager, Arrays.asList(assignRequests));
    }

    /**
     * Assign new replicas to nodes.
     * If multiple {@link AssignRequest}s are provided, then every {@link ReplicaPosition} made for an
     * {@link AssignRequest} will be applied to the {@link SolrCloudManager}'s state when processing subsequent {@link AssignRequest}s.
     * Therefore, the order in which {@link AssignRequest}s are provided can and will affect the {@link ReplicaPosition}s returned.
     *
     * @param solrCloudManager current instance of {@link SolrCloudManager}.
     * @param assignRequests list of assign requests to process together ().
     * @return list of {@link ReplicaPosition}-s for new replicas.
     * @throws AssignmentException when assignment request cannot produce any valid assignments.
     */
    List<ReplicaPosition> assign(SolrCloudManager solrCloudManager, List<AssignRequest> assignRequests)
        throws AssignmentException, IOException, InterruptedException;

    /**
     * Verify that deleting a collection doesn't violate the replica assignment constraints.
     * @param solrCloudManager current instance of {@link SolrCloudManager}.
     * @param collection collection to delete.
     * @throws AssignmentException when deleting the collection would violate replica assignment constraints.
     * @throws IOException on general errors.
     */
    default void verifyDeleteCollection(SolrCloudManager solrCloudManager, DocCollection collection)
        throws AssignmentException, IOException, InterruptedException {

    }

    /**
     * Verify that deleting these replicas doesn't violate the replica assignment constraints.
     * @param solrCloudManager current instance of {@link SolrCloudManager}.
     * @param collection collection to delete replicas from.
     * @param shardName shard name.
     * @param replicas replicas to delete.
     * @throws AssignmentException when deleting the replicas would violate replica assignment constraints.
     * @throws IOException on general errors.
     */
    default void verifyDeleteReplicas(SolrCloudManager solrCloudManager, DocCollection collection, String shardName, Set<Replica> replicas)
        throws AssignmentException, IOException, InterruptedException {

    }
  }

  public static class AssignRequest {
    public final String collectionName;
    public final List<String> shardNames;
    public final List<String> nodes;
    public final int numNrtReplicas;
    public final int numTlogReplicas;
    public final int numPullReplicas;

    public AssignRequest(String collectionName, List<String> shardNames, List<String> nodes, int numNrtReplicas, int numTlogReplicas, int numPullReplicas) {
      this.collectionName = collectionName;
      this.shardNames = shardNames;
      this.nodes = nodes;
      this.numNrtReplicas = numNrtReplicas;
      this.numTlogReplicas = numTlogReplicas;
      this.numPullReplicas = numPullReplicas;
    }
  }

  public static class AssignRequestBuilder {
    private String collectionName;
    private List<String> shardNames;
    private List<String> nodes;
    private int numNrtReplicas;
    private int numTlogReplicas;
    private int numPullReplicas;

    public AssignRequestBuilder forCollection(String collectionName) {
      this.collectionName = collectionName;
      return this;
    }

    public AssignRequestBuilder forShard(List<String> shardNames) {
      this.shardNames = shardNames;
      return this;
    }

    public AssignRequestBuilder onNodes(List<String> nodes) {
      this.nodes = nodes;
      return this;
    }

    public AssignRequestBuilder assignNrtReplicas(int numNrtReplicas) {
      this.numNrtReplicas = numNrtReplicas;
      return this;
    }

    public AssignRequestBuilder assignTlogReplicas(int numTlogReplicas) {
      this.numTlogReplicas = numTlogReplicas;
      return this;
    }

    public AssignRequestBuilder assignPullReplicas(int numPullReplicas) {
      this.numPullReplicas = numPullReplicas;
      return this;
    }

    public AssignRequest build() {
      Objects.requireNonNull(collectionName, "The collectionName cannot be null");
      Objects.requireNonNull(shardNames, "The shard names cannot be null");
      return new AssignRequest(collectionName, shardNames, nodes, numNrtReplicas,
          numTlogReplicas, numPullReplicas);
    }
  }

  /**
   * Creates the appropriate instance of {@link AssignStrategy} based on how the cluster and/or individual collections are
   * configured.
   * <p>If {@link PlacementPlugin} instance is null this call will return a strategy from {@link SimplePlacementFactory}, otherwise
   * {@link PlacementPluginAssignStrategy} will be used.</p>
   */
  public static AssignStrategy createAssignStrategy(CoreContainer coreContainer) {
    // If a cluster wide placement plugin is configured (and that's the only way to define a placement plugin)
    PlacementPlugin placementPlugin = coreContainer.getPlacementPluginFactory().createPluginInstance();
    if (placementPlugin == null) {
      // Otherwise use the default
      // TODO: Replace this with a better options, such as the AffinityPlacementFactory
      placementPlugin = (new SimplePlacementFactory()).createPluginInstance();
    }
    return new PlacementPluginAssignStrategy(placementPlugin);
  }
}
