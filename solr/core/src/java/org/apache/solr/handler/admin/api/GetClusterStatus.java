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
import java.util.Map;
import org.apache.solr.client.api.endpoint.GetClusterStatusApi;
import org.apache.solr.client.api.model.GetClusterStatusResponse;
import org.apache.solr.client.api.model.GetClusterStatusResponse.CollectionState;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ClusterStatus;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for the collections, shards, and replicas tree.
 *
 * <p>{@code GET /api/cluster} returns that tree and nothing else hanging off the cluster object.
 * Live nodes, aliases, and cluster properties stay on their own endpoints. v1 {@code CLUSTERSTATUS}
 * is unchanged.
 */
public class GetClusterStatus extends AdminAPIBase implements GetClusterStatusApi {

  @Inject
  public GetClusterStatus(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(coreContainer, req, rsp);
  }

  @Override
  @PermissionName(COLL_READ_PERM)
  public GetClusterStatusResponse getClusterStatus(
      String collection, String shard, String routeKey, Boolean prs) throws Exception {
    final GetClusterStatusResponse response =
        instantiateJerseyResponse(GetClusterStatusResponse.class);
    validateZooKeeperAwareCoreContainer(coreContainer);
    if (collection != null) {
      recordCollectionForLogAndTracing(collection, solrQueryRequest);
    }

    // Only the parameters this API documents. includeAll, liveNodes, aliases, and
    // clusterProperties are v1 CLUSTERSTATUS knobs and are not honored here.
    final ModifiableSolrParams params = new ModifiableSolrParams();
    params.setNonNull("collection", collection);
    params.setNonNull("shard", shard);
    params.setNonNull(ShardParams._ROUTE_, routeKey);
    if (prs != null) {
      params.set("prs", prs);
    }

    final ClusterStatus clusterStatus =
        new ClusterStatus(coreContainer.getZkController().getZkStateReader(), params);
    response.cluster = new GetClusterStatusResponse.Cluster();
    response.cluster.collections = typedCollections(clusterStatus.getCollectionStatuses());
    return response;
  }

  /**
   * Bind the state maps onto the response types. Fields the model knows about are typed; every
   * other entry stays on the object through its catch-all.
   */
  private static Map<String, CollectionState> typedCollections(Map<String, Object> raw) {
    var mapper = SolrJacksonMapper.getObjectMapper();
    Map<String, CollectionState> collections = CollectionUtil.newLinkedHashMap(raw.size());
    for (Map.Entry<String, Object> entry : raw.entrySet()) {
      collections.put(entry.getKey(), mapper.convertValue(entry.getValue(), CollectionState.class));
    }
    return collections;
  }
}
