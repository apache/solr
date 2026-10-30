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
import static org.apache.solr.common.cloud.ZkStateReader.MAX_AT_ONCE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.MAX_WAIT_SECONDS_PROP;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.Map;
import org.apache.solr.client.api.endpoint.RebalanceLeadersApi;
import org.apache.solr.client.api.model.FlexibleSolrJerseyResponse;
import org.apache.solr.client.api.model.RebalanceLeadersRequestBody;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for rebalancing shard leaders in a collection.
 *
 * <p>This API (POST /v2/collections/collectionName/rebalance-leaders) is analogous to the v1
 * /admin/collections?action=REBALANCELEADERS command.
 */
public class RebalanceLeaders extends AdminAPIBase implements RebalanceLeadersApi {

  @Inject
  public RebalanceLeaders(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  @SuppressWarnings("unchecked")
  public FlexibleSolrJerseyResponse rebalanceLeaders(
      String collectionName, RebalanceLeadersRequestBody requestBody) throws Exception {
    final var response = instantiateJerseyResponse(FlexibleSolrJerseyResponse.class);
    fetchAndValidateZooKeeperAwareCoreContainer();
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);
    if (requestBody == null) {
      requestBody = new RebalanceLeadersRequestBody();
    }

    final var params = new ModifiableSolrParams(solrQueryRequest.getParams());
    params.set(COLLECTION_PROP, collectionName);
    if (requestBody.maxAtOnce != null) {
      params.set(MAX_AT_ONCE_PROP, requestBody.maxAtOnce);
    }
    if (requestBody.maxWaitSeconds != null) {
      params.set(MAX_WAIT_SECONDS_PROP, requestBody.maxWaitSeconds);
    }
    solrQueryRequest.setParams(params);

    new org.apache.solr.handler.admin.RebalanceLeaders(
            solrQueryRequest, solrQueryResponse, coreContainer.getCollectionsHandler())
        .execute();
    final Map<String, Object> results = solrQueryResponse.getValues().asMap(Integer.MAX_VALUE);
    results.forEach(response::setUnknownProperty);
    return response;
  }
}
