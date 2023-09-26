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

import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.SyncShardApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation for triggering a shard-sync operation within a particular collection and
 * shard.
 *
 * <p>This API (POST /v2/collections/cName/shards/sName/sync {...}) is analogous to the v1
 * /admin/collections?action=SYNCSHARD command.
 */
public class SyncShard extends AdminAPIBase implements SyncShardApi {

  @Inject
  public SyncShard(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse syncShard(String collectionName, String shardName) throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, shardName);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    doSyncShard(collectionName, shardName);

    return response;
  }

  private void doSyncShard(String extCollectionName, String shardName)
      throws IOException, SolrServerException {
    String collection = coreContainer.getAliases().resolveSimpleAlias(extCollectionName);

    ClusterState clusterState = coreContainer.getZkController().getClusterState();

    DocCollection docCollection = clusterState.getCollection(collection);
    ZkNodeProps leaderProps = docCollection.getLeader(shardName);
    ZkCoreNodeProps nodeProps = new ZkCoreNodeProps(leaderProps);

    try (SolrClient client =
        new HttpSolrClient.Builder(nodeProps.getBaseUrl())
            .withConnectionTimeout(15000, TimeUnit.MILLISECONDS)
            .withSocketTimeout(60000, TimeUnit.MILLISECONDS)
            .build()) {
      CoreAdminRequest.RequestSyncShard reqSyncShard = new CoreAdminRequest.RequestSyncShard();
      reqSyncShard.setCollection(collection);
      reqSyncShard.setShard(shardName);
      reqSyncShard.setCoreName(nodeProps.getCoreName());
      client.request(reqSyncShard);
    }
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest request, SolrQueryResponse response)
      throws Exception {
    final var api = new SyncShard(coreContainer, request, response);
    final var params = request.getParams();
    params.required().check(COLLECTION_PROP, SHARD_ID_PROP);

    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        response, api.syncShard(params.get(COLLECTION_PROP), params.get(SHARD_ID_PROP)));
  }
}
