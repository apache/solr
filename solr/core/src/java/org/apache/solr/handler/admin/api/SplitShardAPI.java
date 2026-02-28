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

import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLLECTION;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.NUM_SUB_SHARDS;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_BY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_FUZZ;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CommonParams.ACTION;
import static org.apache.solr.common.params.CommonParams.TIMING;
import static org.apache.solr.common.params.CoreAdminParams.RANGES;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.handler.api.V2ApiUtils.flattenMapWithPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.SplitShardApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SplitShardRequestBody;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for splitting an existing shard up into multiple pieces.
 *
 * <p>The new API (POST /v2/collections/{collectionName}/shards/split) is equivalent to the v1
 * /admin/collections?action=SPLITSHARD command. The logic remains in the V1 {@link
 * CollectionsHandler}.
 */
public class SplitShardAPI extends AdminAPIBase implements SplitShardApi {

  @Inject
  public SplitShardAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse splitShard(String collectionName, SplitShardRequestBody requestBody)
      throws Exception {
    ensureRequiredParameterProvided(COLLECTION, collectionName);
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

    final Map<String, Object> v1Params = new HashMap<>();
    v1Params.put(ACTION, CollectionParams.CollectionAction.SPLITSHARD.toLower());
    v1Params.put(COLLECTION, collectionName);

    if (requestBody != null) {
      if (requestBody.shard != null) {
        v1Params.put(SHARD_ID_PROP, requestBody.shard);
      }
      if (requestBody.ranges != null) {
        v1Params.put(RANGES, requestBody.ranges);
      }
      if (requestBody.splitKey != null) {
        v1Params.put(SPLIT_KEY, requestBody.splitKey);
      }
      if (requestBody.numSubShards != null) {
        v1Params.put(NUM_SUB_SHARDS, requestBody.numSubShards);
      }
      if (requestBody.splitFuzz != null) {
        v1Params.put(SPLIT_FUZZ, requestBody.splitFuzz);
      }
      if (requestBody.timing != null) {
        v1Params.put(TIMING, requestBody.timing);
      }
      if (requestBody.splitByPrefix != null) {
        v1Params.put(SPLIT_BY_PREFIX, requestBody.splitByPrefix);
      }
      if (requestBody.followAliases != null) {
        v1Params.put(FOLLOW_ALIASES, requestBody.followAliases);
      }
      if (requestBody.splitMethod != null) {
        v1Params.put(SPLIT_METHOD, requestBody.splitMethod);
      }
      if (requestBody.async != null) {
        v1Params.put(ASYNC, requestBody.async);
      }
      if (requestBody.waitForFinalState != null) {
        v1Params.put(WAIT_FOR_FINAL_STATE, requestBody.waitForFinalState);
      }
      flattenMapWithPrefix(requestBody.coreProperties, v1Params, PROPERTY_PREFIX);
    }

    CollectionsHandler collectionsHandler = coreContainer.getCollectionsHandler();
    collectionsHandler.handleRequestBody(wrapParams(solrQueryRequest, v1Params), solrQueryResponse);
    return response;
  }
}
