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
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
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
import static org.apache.solr.common.params.CoreAdminParams.RANGES;
import static org.apache.solr.handler.admin.api.CreateCollection.copyPrefixedPropertiesWithoutPrefix;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.SplitShardApi;
import org.apache.solr.client.api.model.SplitShardRequestBody;
import org.apache.solr.client.api.model.SplitShardResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for splitting an existing shard up into multiple pieces.
 *
 * <p>The new API (POST /v2/collections/{collectionName}/shards/split) is equivalent to the v1
 * /admin/collections?action=SPLITSHARD command.
 */
public class SplitShardAPI extends AdminAPIBase implements SplitShardApi {

  // Deep enough to cover every level SplitShardCmd's RTimerTree nests sub-timers to.
  private static final int MAX_TIMING_TREE_DEPTH = 10;

  // Splits can run long on large shards, so allow more time than the default collection-op
  // timeout before giving up on waiting for a response.
  private static final long SPLIT_SHARD_TIMEOUT_MS =
      CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT * 5;

  @Inject
  public SplitShardAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SplitShardResponse splitShard(String collectionName, SplitShardRequestBody requestBody)
      throws Exception {
    ensureRequiredParameterProvided(COLLECTION, collectionName);
    ensureRequiredRequestBodyProvided(requestBody);
    validateSplitParameters(requestBody);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final SplitShardResponse response = instantiateJerseyResponse(SplitShardResponse.class);
    final ZkNodeProps remoteMessage = createRemoteMessage(collectionName, requestBody);
    final var remoteResponse =
        submitRemoteMessageAndHandleResponse(
            response,
            CollectionParams.CollectionAction.SPLITSHARD,
            remoteMessage,
            requestBody.async,
            SPLIT_SHARD_TIMEOUT_MS);

    final Object timing = remoteResponse.getResponse().get(TIMING);
    if (timing instanceof NamedList<?> timingNamedList) {
      @SuppressWarnings("unchecked")
      final Map<String, Object> timingMap = timingNamedList.asMap(MAX_TIMING_TREE_DEPTH);
      response.timing = timingMap;
    }

    return response;
  }

  private static void validateSplitParameters(SplitShardRequestBody requestBody) {
    if (requestBody.splitKey == null && requestBody.shard == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "At least one of shard, or split.key should be specified.");
    }
    if (requestBody.splitKey != null && requestBody.shard != null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Only one of 'shard' or 'split.key' should be specified");
    }
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

  public static ZkNodeProps createRemoteMessage(
      String collectionName, SplitShardRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(COLLECTION_PROP, collectionName);
    insertIfNotNull(remoteMessage, SHARD_ID_PROP, requestBody.shard);
    insertIfNotNull(remoteMessage, RANGES, requestBody.ranges);
    insertIfNotNull(remoteMessage, SPLIT_KEY, requestBody.splitKey);
    insertIfNotNull(remoteMessage, NUM_SUB_SHARDS, requestBody.numSubShards);
    insertIfNotNull(remoteMessage, SPLIT_FUZZ, requestBody.splitFuzz);
    insertIfNotNull(remoteMessage, TIMING, requestBody.timing);
    insertIfNotNull(remoteMessage, SPLIT_BY_PREFIX, requestBody.splitByPrefix);
    insertIfNotNull(remoteMessage, FOLLOW_ALIASES, requestBody.followAliases);
    insertIfNotNull(remoteMessage, SPLIT_METHOD, requestBody.splitMethod);
    insertIfNotNull(remoteMessage, WAIT_FOR_FINAL_STATE, requestBody.waitForFinalState);
    if (requestBody.nodeSet != null) {
      remoteMessage.put(CREATE_NODE_SET_PARAM, String.join(",", requestBody.nodeSet));
    }
    flattenMapWithPrefix(requestBody.coreProperties, remoteMessage, PROPERTY_PREFIX);

    return new ZkNodeProps(remoteMessage);
  }

  public static SplitShardRequestBody createRequestBodyFromV1Params(SolrParams params) {
    final var requestBody = new SplitShardRequestBody();
    requestBody.shard = params.get(SHARD_ID_PROP);
    requestBody.ranges = params.get(RANGES);
    requestBody.splitKey = params.get(SPLIT_KEY);
    requestBody.numSubShards = params.getInt(NUM_SUB_SHARDS);
    requestBody.splitFuzz = params.get(SPLIT_FUZZ);
    requestBody.timing = params.getBool(TIMING);
    requestBody.splitByPrefix = params.getBool(SPLIT_BY_PREFIX);
    requestBody.followAliases = params.getBool(FOLLOW_ALIASES);
    requestBody.splitMethod = params.get(SPLIT_METHOD);
    requestBody.waitForFinalState = params.getBool(WAIT_FOR_FINAL_STATE);
    requestBody.async = params.get(ASYNC);
    if (params.get(CREATE_NODE_SET_PARAM) != null) {
      requestBody.nodeSet = Arrays.asList(params.get(CREATE_NODE_SET_PARAM).split(","));
    }
    final Map<String, String> coreProperties =
        copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);
    if (!coreProperties.isEmpty()) {
      requestBody.coreProperties = new HashMap<>(coreProperties);
    }

    return requestBody;
  }

  public static void invokeWithV1Params(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse)
      throws Exception {
    final var v1Params = solrQueryRequest.getParams();
    v1Params.required().check(COLLECTION_PROP);

    final var requestBody = createRequestBodyFromV1Params(v1Params);
    final var splitShardApi = new SplitShardAPI(coreContainer, solrQueryRequest, solrQueryResponse);
    final var response = splitShardApi.splitShard(v1Params.get(COLLECTION_PROP), requestBody);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(solrQueryResponse, response);
  }
}
