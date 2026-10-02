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

import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_READ_PERM;

import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.solr.client.api.endpoint.ListShardsApi;
import org.apache.solr.client.api.model.ListShardsResponse;
import org.apache.solr.client.api.model.ListShardsResponse.ShardSummary;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ClusterStatus;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for listing shard-level status for a collection.
 *
 * <p>This API (GET /api/collections/collName/shards) has no dedicated v1 equivalent. {@code
 * /admin/collections?action=CLUSTERSTATUS&collection=collName} is the closest v1 form.
 */
public class ListShards extends AdminAPIBase implements ListShardsApi {

  @Inject
  public ListShards(CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  @Override
  @PermissionName(COLL_READ_PERM)
  public ListShardsResponse listShards(String collectionName) {
    final ListShardsResponse response = instantiateJerseyResponse(ListShardsResponse.class);
    ensureRequiredParameterProvided(COLLECTION, collectionName);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final ClusterState clusterState = coreContainer.getZkController().getClusterState();
    final DocCollection collection = clusterState.getCollectionOrNull(collectionName);
    if (collection == null) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND, "Collection not found: " + collectionName);
    }

    final Set<String> liveNodes = clusterState.getLiveNodes();
    final Map<String, ShardSummary> shards = new LinkedHashMap<>();
    collection
        .getSlicesMap()
        .forEach((shardName, slice) -> shards.put(shardName, toShardSummary(slice, liveNodes)));
    response.shards = shards;
    return response;
  }

  static ShardSummary toShardSummary(Slice slice, Set<String> liveNodes) {
    final ShardSummary summary = new ShardSummary();
    summary.state = slice.getState().toString();
    if (slice.getRange() != null) {
      summary.range = slice.getRange().toString();
    }

    final int replicaCount = slice.getReplicas().size();
    int activeReplicaCount = 0;
    boolean hasLeader = false;
    for (Replica replica : slice.getReplicas()) {
      if (replica.isActive(liveNodes)) {
        activeReplicaCount++;
        if (replica.isLeader()) {
          hasLeader = true;
        }
      }
    }

    final float ratioActive = replicaCount == 0 ? 0.0f : (float) activeReplicaCount / replicaCount;
    summary.replicaCount = replicaCount;
    summary.activeReplicaCount = activeReplicaCount;
    summary.replicaHealth = ClusterStatus.Health.calcShardHealth(ratioActive, hasLeader).toString();
    return summary;
  }
}
