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
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.ReloadCollectionApi;
import org.apache.solr.client.api.model.ReloadCollectionRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * V2 API implementation for reloading collections.
 *
 * <p>The new API (POST /v2/collections/collectionName/reload {...}) is analogous to the v1
 * /admin/collections?action=RELOAD command.
 */
public class ReloadCollectionAPI extends AdminAPIBase implements ReloadCollectionApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public ReloadCollectionAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse reloadCollection(
      String collectionName, ReloadCollectionRequestBody requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final ZkNodeProps remoteMessage = createRemoteMessage(collectionName, requestBody);
    submitRemoteMessageAndHandleResponse(
        response,
        CollectionParams.CollectionAction.RELOAD,
        remoteMessage,
        requestBody != null ? requestBody.async : null);
    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, ReloadCollectionRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.RELOAD.toLower());
    remoteMessage.put(NAME, collectionName);
    if (requestBody != null) {
      insertIfNotNull(remoteMessage, ASYNC, requestBody.async);
    }

    return new ZkNodeProps(remoteMessage);
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest request, SolrQueryResponse response)
      throws Exception {
    final var api = new ReloadCollectionAPI(coreContainer, request, response);
    final var params = request.getParams();
    params.required().check(NAME);
    final var requestBody = new ReloadCollectionRequestBody();
    requestBody.async = params.get(ASYNC); // Note, 'async' may or may not have been provided.

    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        response, api.reloadCollection(params.get(NAME), requestBody));
  }
}
