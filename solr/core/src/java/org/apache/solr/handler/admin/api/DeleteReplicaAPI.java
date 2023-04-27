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
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ONLY_IF_DOWN;
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COUNT_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INDEX;
import static org.apache.solr.common.params.CoreAdminParams.DELETE_INSTANCE_DIR;
import static org.apache.solr.common.params.CoreAdminParams.REPLICA;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for deleting one or more existing replicas from a shard.
 *
 * <p>These APIs are analogous to the v1 /admin/collections?action=DELETEREPLICA command.
 */
@Path("/collections/{collectionName}/shards/{shardName}/replicas")
public class DeleteReplicaAPI extends AdminAPIBase {

  @Inject
  public DeleteReplicaAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @DELETE
  @Path("/{replicaName}")
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse deleteReplicaByName(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      @PathParam("replicaName") String replicaName,
      // Optional params below
      @QueryParam(FOLLOW_ALIASES) Boolean followAliases,
      @QueryParam(DELETE_INSTANCE_DIR) Boolean deleteInstanceDir,
      @QueryParam(DELETE_DATA_DIR) Boolean deleteDataDir,
      @QueryParam(DELETE_INDEX) Boolean deleteIndex,
      @QueryParam(ONLY_IF_DOWN) Boolean onlyIfDown,
      @QueryParam(ASYNC) String asyncId)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, shardName);
    ensureRequiredParameterProvided(REPLICA, replicaName);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(
            collectionName,
            shardName,
            replicaName,
            null,
            followAliases,
            deleteInstanceDir,
            deleteDataDir,
            deleteIndex,
            onlyIfDown,
            asyncId);
    submitRemoteMessageAndHandleResponse(
        response, CollectionParams.CollectionAction.DELETEREPLICA, remoteMessage, asyncId);
    return response;
  }

  @DELETE
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse deleteReplicasByCount(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      @QueryParam(COUNT_PROP) Integer numToDelete,
      // Optional params below
      @QueryParam(FOLLOW_ALIASES) Boolean followAliases,
      @QueryParam(DELETE_INSTANCE_DIR) Boolean deleteInstanceDir,
      @QueryParam(DELETE_DATA_DIR) Boolean deleteDataDir,
      @QueryParam(DELETE_INDEX) Boolean deleteIndex,
      @QueryParam(ONLY_IF_DOWN) Boolean onlyIfDown,
      @QueryParam(ASYNC) String asyncId)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, shardName);
    ensureRequiredParameterProvided(COUNT_PROP, numToDelete);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(
            collectionName,
            shardName,
            null,
            numToDelete,
            followAliases,
            deleteInstanceDir,
            deleteDataDir,
            deleteIndex,
            onlyIfDown,
            asyncId);
    submitRemoteMessageAndHandleResponse(
        response, CollectionParams.CollectionAction.DELETEREPLICA, remoteMessage, asyncId);
    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName,
      String shardName,
      String replicaName,
      Integer numReplicasToDelete,
      Boolean followAliases,
      Boolean deleteInstanceDir,
      Boolean deleteDataDir,
      Boolean deleteIndex,
      Boolean onlyIfDown,
      String asyncId) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.DELETEREPLICA.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(SHARD_ID_PROP, shardName);
    insertIfNotNull(remoteMessage, REPLICA, replicaName);
    insertIfNotNull(remoteMessage, COUNT_PROP, numReplicasToDelete);
    insertIfNotNull(remoteMessage, FOLLOW_ALIASES, followAliases);
    insertIfNotNull(remoteMessage, DELETE_INSTANCE_DIR, deleteInstanceDir);
    insertIfNotNull(remoteMessage, DELETE_DATA_DIR, deleteDataDir);
    insertIfNotNull(remoteMessage, DELETE_INDEX, deleteIndex);
    insertIfNotNull(remoteMessage, ONLY_IF_DOWN, onlyIfDown);
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

    final var deleteReplicaApi =
        new DeleteReplicaAPI(coreContainer, solrQueryRequest, solrQueryResponse);
    final var v2Response = invokeApiMethod(deleteReplicaApi, v1Params);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(solrQueryResponse, v2Response);
  }

  private static SubResponseAccumulatingJerseyResponse invokeApiMethod(
      DeleteReplicaAPI deleteReplicaApi, SolrParams v1Params) throws Exception {
    if (v1Params.get(REPLICA) != null) {
      return deleteReplicaApi.deleteReplicaByName(
          v1Params.get(COLLECTION_PROP),
          v1Params.get(SHARD_ID_PROP),
          v1Params.get(REPLICA),
          v1Params.getBool(FOLLOW_ALIASES),
          v1Params.getBool(DELETE_INSTANCE_DIR),
          v1Params.getBool(DELETE_DATA_DIR),
          v1Params.getBool(DELETE_INDEX),
          v1Params.getBool(ONLY_IF_DOWN),
          v1Params.get(ASYNC));
    } else if (v1Params.get(COUNT_PROP) != null) {
      return deleteReplicaApi.deleteReplicasByCount(
          v1Params.get(COLLECTION_PROP),
          v1Params.get(SHARD_ID_PROP),
          v1Params.getInt(COUNT_PROP),
          v1Params.getBool(FOLLOW_ALIASES),
          v1Params.getBool(DELETE_INSTANCE_DIR),
          v1Params.getBool(DELETE_DATA_DIR),
          v1Params.getBool(DELETE_INDEX),
          v1Params.getBool(ONLY_IF_DOWN),
          v1Params.get(ASYNC));
    } else {
      throw new SolrException(
          BAD_REQUEST,
          "DELETEREPLICA requires either " + COUNT_PROP + " or " + REPLICA + " parameters");
    }
  }
}
