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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.api.endpoint.ListReplicasApi;
import org.apache.solr.client.api.model.ListReplicasResponse;
import org.apache.solr.client.api.model.ReplicaInfo;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.ReplicaStateProps;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for listing the replicas of a collection shard.
 *
 * <p>This API (GET /api/collections/collName/shards/shardName/replicas) has no dedicated v1
 * equivalent; {@code /admin/collections?action=CLUSTERSTATUS} is the closest v1 form.
 */
public class ListReplicas extends AdminAPIBase implements ListReplicasApi {

  @Inject
  public ListReplicas(CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  @Override
  @PermissionName(COLL_READ_PERM)
  public ListReplicasResponse listReplicas(String collectionName, String shardName) {
    final ListReplicasResponse response = instantiateJerseyResponse(ListReplicasResponse.class);
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);
    validateZooKeeperAwareCoreContainer(coreContainer);

    final ClusterState clusterState = coreContainer.getZkController().getClusterState();
    final DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    if (collection == null) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND, "Collection: " + collectionName + " not found");
    }

    final Slice slice = collection.getSlice(shardName);
    if (slice == null) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND,
          "Collection: " + collectionName + " shard: " + shardName + " not found");
    }

    final Set<String> liveNodes = clusterState.getLiveNodes();
    final Map<String, ReplicaInfo> replicas = new LinkedHashMap<>();
    for (Replica replica : slice) {
      replicas.put(replica.getName(), toReplicaInfo(replica, liveNodes));
    }
    response.replicas = replicas;
    return response;
  }

  /**
   * Convert a cluster-state replica into the v2 replica payload, applying the same live-node
   * cross-check CLUSTERSTATUS uses.
   */
  public static ReplicaInfo toReplicaInfo(Replica replica, Set<String> liveNodes) {
    final ReplicaInfo info = new ReplicaInfo();
    info.core = replica.getCoreName();
    info.baseUrl = replica.getBaseUrl();
    info.nodeName = replica.getNodeName();
    info.type = replica.getType().toString();

    Replica.State state = replica.getState();
    if (state != Replica.State.DOWN && !liveNodes.contains(replica.getNodeName())) {
      state = Replica.State.DOWN;
    }
    info.state = state.toString();

    if (replica.isLeader()) {
      info.leader = true;
    }

    final Object forceSetState = replica.get(ReplicaStateProps.FORCE_SET_STATE);
    if (forceSetState != null) {
      info.forceSetState = Boolean.parseBoolean(String.valueOf(forceSetState));
    }

    for (Map.Entry<String, Object> entry : replica.getProperties().entrySet()) {
      if (!ReplicaStateProps.WELL_KNOWN_PROPS.contains(entry.getKey())
          && entry.getValue() != null) {
        info.setAdditionalProperty(entry.getKey(), entry.getValue());
      }
    }
    return info;
  }
}
