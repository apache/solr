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
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.NUM_SUB_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_BY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_FUZZ;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.TIMING;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.handler.admin.api.CreateCollection.copyPrefixedPropertiesWithoutPrefix;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.SplitShardApi;
import org.apache.solr.client.api.model.SplitShardRequestBody;
import org.apache.solr.client.api.model.SplitShardResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for splitting an existing shard into multiple pieces.
 *
 * <p>This API (POST /v2/collections/collectionName/shards/shardName/split) is analogous to the v1
 * /admin/collections?action=SPLITSHARD command.
 */
public class SplitShard extends AdminAPIBase implements SplitShardApi {

  @Inject
  public SplitShard(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SplitShardResponse splitShard(
      String collectionName, String shardName, SplitShardRequestBody requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SplitShardResponse.class);
    fetchAndValidateZooKeeperAwareCoreContainer();
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, shardName);
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);
    if (requestBody == null) {
      requestBody = new SplitShardRequestBody();
    }
    validateSplitOptions(requestBody);
    return submitSplitMessage(response, collectionName, shardName, requestBody);
  }

  /**
   * The v1 form of split-by-key: the request names a collection and a split key but no shard, so
   * there is no shard path parameter to populate. The overseer resolves the shard from the key.
   */
  private SplitShardResponse splitShardByKey(
      String collectionName, SplitShardRequestBody requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SplitShardResponse.class);
    fetchAndValidateZooKeeperAwareCoreContainer();
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);
    validateSplitOptions(requestBody);
    return submitSplitMessage(response, collectionName, null, requestBody);
  }

  private SplitShardResponse submitSplitMessage(
      SplitShardResponse response,
      String collectionName,
      String shardName,
      SplitShardRequestBody requestBody)
      throws Exception {
    final Map<String, Object> messageProps = new HashMap<>();
    messageProps.put(COLLECTION_PROP, collectionName);
    insertIfNotNull(messageProps, SHARD_ID_PROP, shardName);
    insertIfNotNull(messageProps, "ranges", requestBody.ranges);
    insertIfNotNull(messageProps, SPLIT_KEY, requestBody.splitKey);
    insertIfNotNull(messageProps, "numSubShards", requestBody.numSubShards);
    insertIfNotNull(messageProps, "splitFuzz", requestBody.splitFuzz);
    insertIfNotNull(messageProps, "timing", requestBody.timing);
    insertIfNotNull(messageProps, "splitByPrefix", requestBody.splitByPrefix);
    insertIfNotNull(messageProps, "followAliases", requestBody.followAliases);
    insertIfNotNull(messageProps, "splitMethod", requestBody.splitMethod);
    insertIfNotNull(messageProps, "waitForFinalState", requestBody.waitForFinalState);
    insertIfNotNull(messageProps, CREATE_NODE_SET_PARAM, requestBody.createNodeSet);
    if (requestBody.coreProperties != null) {
      flattenMapWithPrefix(requestBody.coreProperties, messageProps, PROPERTY_PREFIX);
    }

    final var remoteResponse =
        submitRemoteMessageAndHandleResponse(
            response,
            CollectionParams.CollectionAction.SPLITSHARD,
            new ZkNodeProps(messageProps),
            requestBody.async,
            DEFAULT_COLLECTION_OP_TIMEOUT * 5);
    final var timing = (NamedList<?>) remoteResponse.getResponse().get(TIMING);
    if (timing != null) {
      response.timing = new LinkedHashMap<>();
      timing.forEach(response.timing::put);
    }
    return response;
  }

  public static SplitShardRequestBody createRequestBodyFromV1Params(SolrParams params) {
    final var requestBody = new SplitShardRequestBody();
    requestBody.ranges = params.get(CoreAdminParams.RANGES);
    requestBody.splitKey = params.get(SPLIT_KEY);
    requestBody.numSubShards = params.getInt(NUM_SUB_SHARDS);
    requestBody.splitFuzz = params.get(SPLIT_FUZZ);
    requestBody.timing = params.getBool(TIMING);
    requestBody.splitByPrefix = params.getBool(SPLIT_BY_PREFIX);
    requestBody.followAliases = params.getBool(FOLLOW_ALIASES);
    requestBody.splitMethod = params.get(SPLIT_METHOD);
    requestBody.waitForFinalState = params.getBool(WAIT_FOR_FINAL_STATE);
    requestBody.createNodeSet = params.get(CREATE_NODE_SET_PARAM);
    final Map<String, String> coreProperties =
        copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);
    if (!coreProperties.isEmpty()) {
      requestBody.coreProperties = new HashMap<>(coreProperties);
    }
    // The v1 'async' parameter becomes the submission's async id: a delegated operation
    // returns no message map, so the v1 handler submits nothing itself.
    requestBody.async = params.get(ASYNC);
    return requestBody;
  }

  public static void invokeWithV1Params(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse)
      throws Exception {
    final var v1Params = solrQueryRequest.getParams();
    final var collectionName = v1Params.required().get(COLLECTION_PROP);
    final var shardName = v1Params.get(SHARD_ID_PROP);
    final var requestBody = createRequestBodyFromV1Params(v1Params);
    if (shardName == null && requestBody.splitKey == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "At least one of shard, or split.key should be specified.");
    }
    if (shardName != null && requestBody.splitKey != null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Only one of 'shard' or 'split.key' should be specified");
    }

    final var splitShardApi = new SplitShard(coreContainer, solrQueryRequest, solrQueryResponse);
    final SplitShardResponse response;
    if (shardName != null) {
      response = splitShardApi.splitShard(collectionName, shardName, requestBody);
    } else {
      response = splitShardApi.splitShardByKey(collectionName, requestBody);
    }
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(solrQueryResponse, response);
  }

  private static void validateSplitOptions(SplitShardRequestBody requestBody) {
    if (requestBody.splitKey != null && requestBody.ranges != null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Only one of 'ranges' or 'split.key' should be specified");
    }
    if (requestBody.numSubShards != null
        && (requestBody.splitKey != null || requestBody.ranges != null)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "numSubShards can not be specified with split.key or ranges parameters");
    }
    if (requestBody.splitFuzz != null
        && (requestBody.splitKey != null || requestBody.ranges != null)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "fuzz can not be specified with split.key or ranges parameters");
    }
  }
}
