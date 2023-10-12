/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.handler.admin.api;

import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICATION_FACTOR;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.SHARD;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.admin.api.CreateCollection.copyPrefixedPropertiesWithoutPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.CreateShardApi;
import org.apache.solr.client.api.model.CreateShardRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.ImplicitDocRouter;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for creating a new shard in a collection.
 *
 * <p>This API (POST /v2/collections/collectionName/shards {...}) is analogous to the v1
 * /admin/collections?action=CREATESHARD command.
 */
public class CreateShard extends AdminAPIBase implements CreateShardApi {

  @Inject
  public CreateShard(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse createShard(
      String collectionName, CreateShardRequestBody requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    if (requestBody == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Required request-body is missing");
    }
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, requestBody.shardName);
    SolrIdentifierValidator.validateShardName(requestBody.shardName);
    final String resolvedCollectionName =
        resolveAndValidateAliasIfEnabled(
            collectionName, Boolean.TRUE.equals(requestBody.followAliases));
    ensureCollectionUsesImplicitRouter(resolvedCollectionName);

    final ZkNodeProps remoteMessage = createRemoteMessage(resolvedCollectionName, requestBody);
    submitRemoteMessageAndHandleResponse(
        response, CollectionParams.CollectionAction.CREATESHARD, remoteMessage, requestBody.async);
    return response;
  }

  public static CreateShardRequestBody createRequestBodyFromV1Params(SolrParams params) {
    params.required().check(COLLECTION, SHARD);

    final var requestBody = new CreateShardRequestBody();
    requestBody.shardName = params.get(SHARD);
    requestBody.replicationFactor = params.getInt(REPLICATION_FACTOR);
    requestBody.nrtReplicas = params.getInt(NRT_REPLICAS);
    requestBody.tlogReplicas = params.getInt(TLOG_REPLICAS);
    requestBody.pullReplicas = params.getInt(PULL_REPLICAS);
    if (params.get(CREATE_NODE_SET_PARAM) != null) {
      final String nodeSetStr = params.get(CREATE_NODE_SET_PARAM);
      if ("EMPTY".equals(nodeSetStr)) {
        requestBody.createReplicas = false;
      } else {
        requestBody.nodeSet = Arrays.asList(nodeSetStr.split(","));
      }
    }
    requestBody.waitForFinalState = params.getBool(WAIT_FOR_FINAL_STATE);
    requestBody.followAliases = params.getBool(FOLLOW_ALIASES);
    requestBody.async = params.get(ASYNC);
    requestBody.properties =
        copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);

    return requestBody;
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse)
      throws Exception {
    final var requestBody = CreateShard.createRequestBodyFromV1Params(solrQueryRequest.getParams());
    final var createShardApi = new CreateShard(coreContainer, solrQueryRequest, solrQueryResponse);
    final var response =
        createShardApi.createShard(solrQueryRequest.getParams().get(COLLECTION), requestBody);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(solrQueryResponse, response);
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, CreateShardRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATESHARD.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(SHARD_ID_PROP, requestBody.shardName);
    if (requestBody.createReplicas == null || requestBody.createReplicas) {
      // The remote message expects a single comma-delimited string, so nodeSet requires flattening
      if (requestBody.nodeSet != null) {
        remoteMessage.put(CREATE_NODE_SET_PARAM, String.join(",", requestBody.nodeSet));
      }
    } else {
      remoteMessage.put(CREATE_NODE_SET, "EMPTY");
    }
    insertIfNotNull(remoteMessage, REPLICATION_FACTOR, requestBody.replicationFactor);
    insertIfNotNull(remoteMessage, NRT_REPLICAS, requestBody.nrtReplicas);
    insertIfNotNull(remoteMessage, TLOG_REPLICAS, requestBody.tlogReplicas);
    insertIfNotNull(remoteMessage, PULL_REPLICAS, requestBody.pullReplicas);
    insertIfNotNull(remoteMessage, WAIT_FOR_FINAL_STATE, requestBody.waitForFinalState);
    insertIfNotNull(remoteMessage, FOLLOW_ALIASES, requestBody.followAliases);
    insertIfNotNull(remoteMessage, ASYNC, requestBody.async);

    if (requestBody.properties != null) {
      requestBody
          .properties
          .entrySet()
          .forEach(
              entry -> {
                remoteMessage.put(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
              });
    }

    return new ZkNodeProps(remoteMessage);
  }

  private void ensureCollectionUsesImplicitRouter(String collectionName) {
    final ClusterState clusterState = coreContainer.getZkController().getClusterState();
    if (!ImplicitDocRouter.NAME.equals(
        ((Map<?, ?>)
                clusterState
                    .getCollection(collectionName)
                    .get(DocCollection.CollectionStateProps.DOC_ROUTER))
            .get(NAME)))
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "shards can be added only to 'implicit' collections");
  }
}
