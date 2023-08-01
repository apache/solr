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

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for deleting a particular shard from its collection.
 *
 * <p>This API (DELETE /v2/collections/collectionName/shards/shardName) is analogous to the v1
 * /admin/collections?action=DELETESHARD command.
 */
@Path("/collections/{collectionName}/shards/{shardName}")
public class DeleteShardAPI extends AdminAPIBase {

  @Inject
  public DeleteShardAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @DELETE
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse deleteShard(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      @QueryParam(DELETE_INSTANCE_DIR) Boolean deleteInstanceDir,
      @QueryParam(DELETE_DATA_DIR) Boolean deleteDataDir,
      @QueryParam(DELETE_INDEX) Boolean deleteIndex,
      @QueryParam(FOLLOW_ALIASES) Boolean followAliases,
      @QueryParam(ASYNC) String asyncId)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, shardName);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(
            collectionName,
            shardName,
            deleteInstanceDir,
            deleteDataDir,
            deleteIndex,
            followAliases,
            asyncId);
    submitRemoteMessageAndHandleResponse(
        response, CollectionParams.CollectionAction.DELETESHARD, remoteMessage, asyncId);
    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName,
      String shardName,
      Boolean deleteInstanceDir,
      Boolean deleteDataDir,
      Boolean deleteIndex,
      Boolean followAliases,
      String asyncId) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.DELETESHARD.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(SHARD_ID_PROP, shardName);
    insertIfNotNull(remoteMessage, FOLLOW_ALIASES, followAliases);
    insertIfNotNull(remoteMessage, DELETE_INSTANCE_DIR, deleteInstanceDir);
    insertIfNotNull(remoteMessage, DELETE_DATA_DIR, deleteDataDir);
    insertIfNotNull(remoteMessage, DELETE_INDEX, deleteIndex);
    insertIfNotNull(remoteMessage, ASYNC, asyncId);

    return new ZkNodeProps(remoteMessage);
  }

  public static void invokeWithV1Params(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse)
      throws Exception {
    final var v1Params = solrQueryRequest.getParams();
    v1Params.required().check(COLLECTION_PROP, SHARD_ID_PROP);

    final var deleteShardApi =
        new DeleteShardAPI(coreContainer, solrQueryRequest, solrQueryResponse);
    final var v2Response =
        deleteShardApi.deleteShard(
            v1Params.get(COLLECTION_PROP),
            v1Params.get(SHARD_ID_PROP),
            v1Params.getBool(DELETE_INSTANCE_DIR),
            v1Params.getBool(DELETE_DATA_DIR),
            v1Params.getBool(DELETE_INDEX),
            v1Params.getBool(FOLLOW_ALIASES),
            v1Params.get(ASYNC));
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(solrQueryResponse, v2Response);
  }
}
