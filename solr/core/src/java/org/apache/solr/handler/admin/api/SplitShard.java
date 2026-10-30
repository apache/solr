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
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonParams.TIMING;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
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
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
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

    final Map<String, Object> messageProps = new HashMap<>();
    messageProps.put(COLLECTION_PROP, collectionName);
    messageProps.put(SHARD_ID_PROP, shardName);
    insertIfNotNull(messageProps, "ranges", requestBody.ranges);
    insertIfNotNull(messageProps, SPLIT_KEY, requestBody.splitKey);
    insertIfNotNull(messageProps, "numSubShards", requestBody.numSubShards);
    insertIfNotNull(messageProps, "splitFuzz", requestBody.splitFuzz);
    insertIfNotNull(messageProps, "timing", requestBody.timing);
    insertIfNotNull(messageProps, "splitByPrefix", requestBody.splitByPrefix);
    insertIfNotNull(messageProps, "followAliases", requestBody.followAliases);
    insertIfNotNull(messageProps, "splitMethod", requestBody.splitMethod);
    insertIfNotNull(messageProps, "waitForFinalState", requestBody.waitForFinalState);
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
