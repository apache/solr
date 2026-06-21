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
package org.apache.solr.handler.admin;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterStatus {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final ZkStateReader zkStateReader;
  private final ZkNodeProps message;
  private final String collection; // maybe null

  public ClusterStatus(ZkStateReader zkStateReader, ZkNodeProps props) {
    this.zkStateReader = zkStateReader;
    this.message = props;
    collection = props.getStr(ZkStateReader.COLLECTION_PROP);
  }

  @SuppressWarnings("unchecked")
  public void getClusterStatus(@SuppressWarnings({"rawtypes"})NamedList results)
      throws KeeperException, InterruptedException {
    // read aliases
    Aliases aliases = zkStateReader.getAliases();
    Map<String, List<String>> collectionVsAliases = new HashMap<>();
    Map<String, List<String>> aliasVsCollections = aliases.getCollectionAliasListMap();
    aliasVsCollections.forEach((alias, colls) -> {
      for (String coll : colls) {
        if (collection == null || collection.equals(coll)) {
          List<String> list = collectionVsAliases.computeIfAbsent(coll, k -> new ArrayList<>());
          list.add(alias);
        }
      }
    });

    @SuppressWarnings({"rawtypes"})
    Map roles = null;
    if (zkStateReader.getZkClient().exists(ZkStateReader.ROLES, true)) {
      roles = (Map) Utils.fromJSON(zkStateReader.getZkClient().getData(ZkStateReader.ROLES, null, (Stat) null, true));
    }

    ClusterState clusterState = zkStateReader.getClusterState();

    // convert cluster state into a map of writable types
    byte[] bytes = Utils.toJSON(clusterState);
    Map<String, Object> stateMap = (Map<String,Object>) Utils.fromJSON(bytes);

    String routeKey = message.getStr(ShardParams._ROUTE_);
    String shard = message.getStr(ZkStateReader.SHARD_ID_PROP);

    Map<String, DocCollection> collectionsMap = null;
    if (collection == null) {
      collectionsMap = clusterState.getCollectionsMap();
    } else  {
      collectionsMap = Collections.singletonMap(collection, zkStateReader.getCollectionOrNull(collection));
    }

    boolean isAlias = aliasVsCollections.containsKey(collection);
    // Guard the null-collection case (e.g. CLUSTERSTATUS with no collection param, used to fetch live_nodes):
    // collectionsMap comes from ClusterState.getCollectionsMap() which returns a NonBlockingHashMap, and
    // NonBlockingHashMap.get(null) throws NPE (unlike a plain HashMap). When no collection was requested the
    // alias/not-found handling below is irrelevant (isAlias is already false), so treat it as found.
    boolean didNotFindCollection = collection != null && collectionsMap.get(collection) == null;

    if (didNotFindCollection && isAlias) {
      // In this case this.collection is an alias name not a collection
      // get all collections and filter out collections not in the alias
      collectionsMap = clusterState.getCollectionsMap().entrySet().stream()
          .filter((entry) -> aliasVsCollections.get(collection).contains(entry.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    NamedList<Object> collectionProps = new SimpleOrderedMap<>();

    for (Map.Entry<String, DocCollection> entry : collectionsMap.entrySet()) {
      Map<String, Object> collectionStatus;
      String name = entry.getKey();
      DocCollection clusterStateCollection = entry.getValue();
      if (clusterStateCollection == null) {
        if (collection != null) {
          SolrException solrException = new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + name + " not found");
          solrException.setMetadata("CLUSTERSTATUS","NOT_FOUND");
          throw solrException;
        } else {
          //collection might have got deleted at the same time
          continue;
        }
      }

      Set<String> requestedShards = new HashSet<>();
      if (routeKey != null) {
        DocRouter router = clusterStateCollection.getRouter();
        Collection<Slice> slices = router.getSearchSlices(routeKey, null, clusterStateCollection);
        for (Slice slice : slices) {
          requestedShards.add(slice.getName());
        }
      }
      if (shard != null) {
        String[] paramShards = shard.split(",");
        requestedShards.addAll(Arrays.asList(paramShards));
      }

      if (DocCollection.getStateFormat() > 1) {
        bytes = Utils.toJSON(clusterStateCollection);
        Map<String, Object> docCollection = (Map<String, Object>) Utils.fromJSON(bytes);
        collectionStatus = getCollectionStatus(docCollection, name, requestedShards);
      } else {
        collectionStatus = getCollectionStatus((Map<String, Object>) stateMap.get(name), name, requestedShards);
      }

      // The serialized DocCollection JSON in this fork does not carry per-replica "core" or
      // "leader" entries (the replica name IS the core name, and leadership lives in the
      // StateUpdates channel rather than as a serialized replica property). Re-derive them from
      // the live DocCollection so CLUSTERSTATUS consumers (e.g. the prometheus exporter) see the
      // same shape as upstream Solr.
      enrichReplicaCoreAndLeader(collectionStatus, clusterStateCollection, zkStateReader.getLiveNodes());

      collectionStatus.put("znodeVersion", clusterStateCollection.getZNodeVersion());
      if (collectionVsAliases.containsKey(name) && !collectionVsAliases.get(name).isEmpty()) {
        collectionStatus.put("aliases", collectionVsAliases.get(name));
      }
      try {
        String configName = zkStateReader.readConfigName(name);
        collectionStatus.put("configName", configName);
        collectionProps.add(name, collectionStatus);
      } catch (KeeperException.NoNodeException ex) {
        // skip this collection because the configset's znode has been deleted
        // which can happen during aggressive collection removal, see SOLR-10720
      }
    }

    Set<String> liveNodes = zkStateReader.getLiveNodes();

    // now we need to walk the collectionProps tree to cross-check replica state with live nodes
    crossCheckReplicaStateWithLiveNodes(liveNodes, collectionProps);

    NamedList<Object> clusterStatus = new SimpleOrderedMap<>();
    clusterStatus.add("collections", collectionProps);

    // read cluster properties
    Map<String, Object> clusterProps = zkStateReader.getClusterProperties();
    if (clusterProps != null && !clusterProps.isEmpty())  {
      clusterStatus.add("properties", clusterProps);
    }

    // add the alias map too
    Map<String, String> collectionAliasMap = aliases.getCollectionAliasMap(); // comma delim
    if (!collectionAliasMap.isEmpty())  {
      clusterStatus.add("aliases", collectionAliasMap);
    }

    // add the roles map
    if (roles != null)  {
      clusterStatus.add("roles", roles);
    }

    // add live_nodes
    clusterStatus.add("live_nodes", liveNodes);

    results.add("cluster", clusterStatus);
  }

  /**
   * Adds per-replica {@code core} and {@code leader} entries to the CLUSTERSTATUS collection map.
   *
   * <p>In this fork the serialized {@link DocCollection} JSON does not contain a {@code core}
   * property (the replica name is the core name) nor a {@code leader} property (leadership is held
   * in the StateUpdates channel, exposed via {@link Replica#getRawState()} rather than serialized).
   * Upstream CLUSTERSTATUS consumers (notably the prometheus exporter) expect both, so we re-derive
   * them here from the live {@link DocCollection}.</p>
   */
  @SuppressWarnings("unchecked")
  private static void enrichReplicaCoreAndLeader(Map<String, Object> collectionStatus,
                                                 DocCollection docCollection, Set<String> liveNodes) {
    if (collectionStatus == null || docCollection == null) {
      return;
    }
    Map<String, Object> shards = (Map<String, Object>) collectionStatus.get("shards");
    if (shards == null) {
      return;
    }
    for (Map.Entry<String, Object> shardEntry : shards.entrySet()) {
      Slice slice = docCollection.getSlice(shardEntry.getKey());
      if (slice == null) {
        continue;
      }
      Replica leader = slice.getLeader(liveNodes);
      String leaderName = leader == null ? null : leader.getName();
      Map<String, Object> shardMap = (Map<String, Object>) shardEntry.getValue();
      Map<String, Object> replicas = (Map<String, Object>) shardMap.get("replicas");
      if (replicas == null) {
        continue;
      }
      for (Map.Entry<String, Object> replicaEntry : replicas.entrySet()) {
        String replicaName = replicaEntry.getKey();
        Map<String, Object> replicaMap = (Map<String, Object>) replicaEntry.getValue();
        // Replica name IS the core name in this fork.
        replicaMap.putIfAbsent("core", replicaName);
        replicaMap.put("leader", String.valueOf(replicaName.equals(leaderName)));
      }
    }
  }

  /**
   * Get collection status from cluster state.
   * Can return collection status by given shard name.
   *
   *
   * @param collection collection map parsed from JSON-serialized {@link ClusterState}
   * @param name  collection name
   * @param requestedShards a set of shards to be returned in the status.
   *                        An empty or null values indicates <b>all</b> shards.
   * @return map of collection properties
   */
  @SuppressWarnings("unchecked")
  private static Map<String, Object> getCollectionStatus(Map<String,Object> collection, String name, Set<String> requestedShards) {
    if (collection == null)  {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + name + " not found");
    }
    if (requestedShards == null || requestedShards.isEmpty()) {
      return collection;
    } else {
      Map<String, Object> shards = (Map<String, Object>) collection.get("shards");
      if (shards == null)  {
        log.error("Illegal state detected, shards is null collection={}", collection);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Illegal state detected, shards is null collection=" + collection);
      }
      Map<String, Object>  selected = new HashMap<>();
      for (String selectedShard : requestedShards) {
        if (!shards.containsKey(selectedShard)) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection: " + name + " shard: " + selectedShard + " not found");
        }
        selected.put(selectedShard, shards.get(selectedShard));
        collection.put("shards", selected);
      }
      return collection;
    }
  }



  /**
   * Walks the tree of collection status to verify that any replicas not reporting a "down" status is
   * on a live node, if any replicas reporting their status as "active" but the node is not live is
   * marked as "down"; used by CLUSTERSTATUS.
   * @param liveNodes List of currently live node names.
   * @param collectionProps Map of collection status information pulled directly from ZooKeeper.
   */

  @SuppressWarnings("unchecked")
  protected static void crossCheckReplicaStateWithLiveNodes(Set<String> liveNodes, NamedList<Object> collectionProps) {
    for (Map.Entry<String,Object> next : collectionProps) {
      Map<String,Object> collMap = (Map<String,Object>) next.getValue();
      Map<String,Object> shards = (Map<String,Object>) collMap.get("shards");
      for (Object nextShard : shards.values()) {
        Map<String,Object> shardMap = (Map<String,Object>) nextShard;
        Map<String,Object> replicas = (Map<String,Object>) shardMap.get("replicas");
        for (Object nextReplica : replicas.values()) {
          Map<String,Object> replicaMap = (Map<String,Object>) nextReplica;
          if (Replica.State.getState((String) replicaMap.get(ZkStateReader.STATE_PROP)) != Replica.State.DOWN) {
            // not down, so verify the node is live
            String node_name = (String) replicaMap.get(ZkStateReader.NODE_NAME_PROP);
            if (!liveNodes.contains(node_name)) {
              // node is not live, so this replica is actually down
              replicaMap.put(ZkStateReader.STATE_PROP, Replica.State.DOWN.toString());
            }
          }
        }
      }
    }
  }
}
