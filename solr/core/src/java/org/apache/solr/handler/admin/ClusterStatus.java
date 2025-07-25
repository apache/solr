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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.PerReplicaStates;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.Utils;
import org.apache.zookeeper.KeeperException;

public class ClusterStatus {

  private final ZkStateReader zkStateReader;
  private final SolrParams solrParams;
  private final String collection; // maybe null

  public static final String INCLUDE_ALL = "includeAll";
  public static final String LIVENODES_PROP = "liveNodes";
  public static final String CLUSTER_PROP = "clusterProperties";
  public static final String ROLES_PROP = "roles";
  public static final String ALIASES_PROP = "aliases";

  /** Shard / collection health state. */
  public enum Health {
    /** All replicas up, leader exists. */
    GREEN,
    /** Some replicas down, leader exists. */
    YELLOW,
    /** Most replicas down, leader exists. */
    ORANGE,
    /** No leader or all replicas down. */
    RED;

    public static final float ORANGE_LEVEL = 0.5f;
    public static final float RED_LEVEL = 0.0f;

    public static Health calcShardHealth(float fractionReplicasUp, boolean hasLeader) {
      if (hasLeader) {
        if (fractionReplicasUp == 1.0f) {
          return GREEN;
        } else if (fractionReplicasUp > ORANGE_LEVEL) {
          return YELLOW;
        } else if (fractionReplicasUp > RED_LEVEL) {
          return ORANGE;
        } else {
          return RED;
        }
      } else {
        return RED;
      }
    }

    /** Combine multiple states into one. Always reports as the worst state. */
    public static Health combine(Collection<Health> states) {
      Health res = GREEN;
      for (Health state : states) {
        if (state.ordinal() > res.ordinal()) {
          res = state;
        }
      }
      return res;
    }
  }

  public ClusterStatus(ZkStateReader zkStateReader, SolrParams params) {
    this.zkStateReader = zkStateReader;
    this.solrParams = params;
    collection = params.get(ZkStateReader.COLLECTION_PROP);
  }

  public void getClusterStatus(NamedList<Object> results, SolrVersion solrVersion)
      throws KeeperException, InterruptedException {
    NamedList<Object> clusterStatus = new SimpleOrderedMap<>();

    boolean includeAll = solrParams.getBool(INCLUDE_ALL, true);
    boolean withLiveNodes = solrParams.getBool(LIVENODES_PROP, includeAll);
    boolean withClusterProperties = solrParams.getBool(CLUSTER_PROP, includeAll);
    boolean withRoles = solrParams.getBool(ROLES_PROP, includeAll);
    boolean withCollection = includeAll || (collection != null);
    boolean withAliases = solrParams.getBool(ALIASES_PROP, includeAll);

    List<String> liveNodes = null;
    if (withLiveNodes || collection != null) {
      liveNodes =
          zkStateReader.getZkClient().getChildren(ZkStateReader.LIVE_NODES_ZKNODE, null, true);
      // add live_nodes
      if (withLiveNodes) clusterStatus.add("live_nodes", liveNodes);
    }

    Aliases aliases = null;
    if (withCollection || withAliases) {
      aliases = zkStateReader.getAliases();
    }

    if (withCollection) {
      assert liveNodes != null;
      fetchClusterStatusForCollOrAlias(clusterStatus, liveNodes, aliases, solrVersion);
    }

    if (withAliases) {
      addAliasMap(aliases, clusterStatus);
    }

    if (withClusterProperties) {
      Map<String, Object> clusterProps = zkStateReader.getClusterProperties();
      if (clusterProps == null) {
        clusterProps = Collections.emptyMap();
      }
      clusterStatus.add("properties", clusterProps);
    }

    // add the roles map
    if (withRoles) {
      Map<?, ?> roles = Collections.emptyMap();
      if (zkStateReader.getZkClient().exists(ZkStateReader.ROLES, true)) {
        roles =
            (Map<?, ?>)
                Utils.fromJSON(
                    zkStateReader.getZkClient().getData(ZkStateReader.ROLES, null, null, true));
      }
      clusterStatus.add("roles", roles);
    }

    results.add("cluster", clusterStatus);
  }

  private void fetchClusterStatusForCollOrAlias(
      NamedList<Object> clusterStatus,
      List<String> liveNodes,
      Aliases aliases,
      SolrVersion solrVersion) {

    // read aliases
    Map<String, List<String>> collectionVsAliases = new HashMap<>();
    Map<String, List<String>> aliasVsCollections = aliases.getCollectionAliasListMap();
    for (Map.Entry<String, List<String>> entry : aliasVsCollections.entrySet()) {
      String alias = entry.getKey();
      List<String> colls = entry.getValue();
      for (String coll : colls) {
        if (collection == null || collection.equals(coll)) {
          List<String> list = collectionVsAliases.computeIfAbsent(coll, k -> new ArrayList<>());
          list.add(alias);
        }
      }
    }

    ClusterState clusterState = zkStateReader.getClusterState();

    String routeKey = solrParams.get(ShardParams._ROUTE_);
    String shard = solrParams.get(ZkStateReader.SHARD_ID_PROP);

    Set<String> requestedShards = (shard != null) ? Set.of(shard.split(",")) : null;

    Stream<DocCollection> collectionStream;
    if (collection == null) {
      collectionStream = clusterState.collectionStream();
    } else {
      DocCollection collState = clusterState.getCollectionOrNull(collection);
      if (collState != null) {
        collectionStream = Stream.of(collState);
      } else { // couldn't find collection
        // hopefully an alias...
        if (!aliasVsCollections.containsKey(collection)) { // not an alias either
          SolrException solrException =
              new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Collection: " + collection + " not found");
          solrException.setMetadata("CLUSTERSTATUS", "NOT_FOUND");
          throw solrException;
        }
        // In this case this.collection is an alias name not a collection
        // Resolve them (not recursively but maybe should?).
        collectionStream =
            aliasVsCollections.get(collection).stream()
                .map(clusterState::getCollectionOrNull)
                .filter(Objects::nonNull);
      }
    }

    if (solrVersion == null || solrVersion.greaterThanOrEqualTo(SolrVersion.valueOf("9.9.0"))) {
      MapWriter collectionPropsWriter =
          ew -> {
            collectionStream.forEach(
                (collectionState) -> {
                  ew.putNoEx(
                      collectionState.getName(),
                      buildResponseForCollection(
                          collectionState,
                          collectionVsAliases,
                          routeKey,
                          liveNodes,
                          requestedShards));
                });
          };
      clusterStatus.add("collections", collectionPropsWriter);
    } else {
      NamedList<Object> collectionProps = new SimpleOrderedMap<>();
      collectionStream.forEach(
          collectionState -> {
            collectionProps.add(
                collectionState.getName(),
                buildResponseForCollection(
                    collectionState, collectionVsAliases, routeKey, liveNodes, requestedShards));
          });
      clusterStatus.add("collections", collectionProps);
    }
  }

  private void addAliasMap(Aliases aliases, NamedList<Object> clusterStatus) {
    // add the alias map too
    Map<String, String> collectionAliasMap = aliases.getCollectionAliasMap(); // comma delim
    if (!collectionAliasMap.isEmpty()) {
      clusterStatus.add("aliases", collectionAliasMap);
    }
  }

  /**
   * Get collection status from cluster state. Can return collection status by given shard name.
   *
   * @param collection collection map parsed from JSON-serialized {@link ClusterState}
   * @param name collection name
   * @param requestedShards a set of shards to be returned in the status. An empty or null values
   *     indicates <b>all</b> shards.
   * @return map of collection properties
   */
  @SuppressWarnings("unchecked")
  private Map<String, Object> getCollectionStatus(
      Map<String, Object> collection, String name, Set<String> requestedShards) {
    if (collection == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Collection: " + name + " not found");
    }
    if (requestedShards == null || requestedShards.isEmpty()) {
      return postProcessCollectionJSON(collection);
    } else {
      Map<String, Object> shards = (Map<String, Object>) collection.get("shards");
      Map<String, Object> selected = new HashMap<>();
      for (String selectedShard : requestedShards) {
        if (!shards.containsKey(selectedShard)) {
          throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST,
              "Collection: " + name + " shard: " + selectedShard + " not found");
        }
        selected.put(selectedShard, shards.get(selectedShard));
        collection.put("shards", selected);
      }
      return postProcessCollectionJSON(collection);
    }
  }

  /**
   * Walks the tree of collection status to verify that any replicas not reporting a "down" status
   * is on a live node, if any replicas reporting their status as "active" but the node is not live
   * is marked as "down"; used by CLUSTERSTATUS.
   *
   * @param liveNodes List of currently live node names.
   * @param collectionProps Map of collection status information pulled directly from ZooKeeper.
   */
  @SuppressWarnings("unchecked")
  protected void crossCheckReplicaStateWithLiveNodes(
      List<String> liveNodes, Map<String, Object> collectionProps) {
    var shards = (Map<String, Object>) collectionProps.get("shards");
    for (Object nextShard : shards.values()) {
      var shardMap = (Map<String, Object>) nextShard;
      var replicas = (Map<String, Object>) shardMap.get("replicas");
      for (Object nextReplica : replicas.values()) {
        var replicaMap = (Map<String, Object>) nextReplica;
        if (Replica.State.getState((String) replicaMap.get(ZkStateReader.STATE_PROP))
            != Replica.State.DOWN) {
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

  @SuppressWarnings("unchecked")
  public static Map<String, Object> postProcessCollectionJSON(Map<String, Object> collection) {
    final Map<String, Map<String, Object>> shards =
        collection != null
            ? (Map<String, Map<String, Object>>)
                collection.getOrDefault("shards", Collections.emptyMap())
            : Collections.emptyMap();
    final List<Health> healthStates = new ArrayList<>(shards.size());
    shards.forEach(
        (shardName, s) -> {
          final Map<String, Map<String, Object>> replicas =
              (Map<String, Map<String, Object>>) s.getOrDefault("replicas", Collections.emptyMap());
          int[] totalVsActive = new int[2];
          boolean hasLeader = false;
          for (Map<String, Object> r : replicas.values()) {
            totalVsActive[0]++;
            boolean active = false;
            if (Replica.State.ACTIVE.toString().equals(r.get("state"))) {
              totalVsActive[1]++;
              active = true;
            }
            if ("true".equals(r.get("leader")) && active) {
              hasLeader = true;
            }
          }
          float ratioActive;
          if (totalVsActive[0] == 0) {
            ratioActive = 0.0f;
          } else {
            ratioActive = (float) totalVsActive[1] / totalVsActive[0];
          }
          Health health = Health.calcShardHealth(ratioActive, hasLeader);
          s.put("health", health.toString());
          healthStates.add(health);
        });
    collection.put("health", Health.combine(healthStates).toString());
    return collection;
  }

  private Map<String, Object> buildResponseForCollection(
      DocCollection clusterStateCollection,
      Map<String, List<String>> collectionVsAliases,
      String routeKey,
      List<String> liveNodes,
      Set<String> requestedShards) {
    Map<String, Object> collectionStatus;
    Set<String> shards = new HashSet<>();
    String name = clusterStateCollection.getName();

    if (routeKey != null)
      clusterStateCollection
          .getRouter()
          .getSearchSlices(routeKey, null, clusterStateCollection)
          .forEach((slice) -> shards.add(slice.getName()));

    if (requestedShards != null) shards.addAll(requestedShards);

    byte[] bytes = Utils.toJSON(clusterStateCollection);
    @SuppressWarnings("unchecked")
    Map<String, Object> docCollection = (Map<String, Object>) Utils.fromJSON(bytes);
    collectionStatus = getCollectionStatus(docCollection, name, shards);

    collectionStatus.put("znodeVersion", clusterStateCollection.getZNodeVersion());
    collectionStatus.put(
        "creationTimeMillis", clusterStateCollection.getCreationTime().toEpochMilli());

    if (collectionVsAliases.containsKey(name) && !collectionVsAliases.get(name).isEmpty()) {
      collectionStatus.put("aliases", collectionVsAliases.get(name));
    }
    String configName = clusterStateCollection.getConfigName();
    collectionStatus.put("configName", configName);
    if (solrParams.getBool("prs", false) && clusterStateCollection.isPerReplicaState()) {
      PerReplicaStates prs = clusterStateCollection.getPerReplicaStates();
      collectionStatus.put("PRS", prs);
    }

    // now we need to walk the collectionProps tree to cross-check replica state with live nodes
    crossCheckReplicaStateWithLiveNodes(liveNodes, collectionStatus);

    return collectionStatus;
  }
}
