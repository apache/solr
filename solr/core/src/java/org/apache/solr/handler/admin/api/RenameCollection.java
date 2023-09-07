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
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.TARGET;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.RenameCollectionApi;
import org.apache.solr.client.api.model.RenameCollectionRequestBody;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation to "rename" an existing collection
 *
 * <p>This API is analogous to the v1 /admin/collections?action=RENAME command.
 */
public class RenameCollection extends AdminAPIBase implements RenameCollectionApi {

  @Inject
  public RenameCollection(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse renameCollection(
      String collectionName, RenameCollectionRequestBody requestBody) throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided("to", requestBody.to);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final ZkNodeProps remoteMessage = createRemoteMessage(collectionName, requestBody);
    submitRemoteMessageAndHandleResponse(
        response,
        CollectionParams.CollectionAction.RENAME,
        remoteMessage,
        requestBody != null ? requestBody.async : null);
    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, RenameCollectionRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.RENAME.toLower());
    remoteMessage.put(NAME, collectionName);
    remoteMessage.put(TARGET, requestBody.to);
    insertIfNotNull(remoteMessage, FOLLOW_ALIASES, requestBody.followAliases);
    insertIfNotNull(remoteMessage, ASYNC, requestBody.async);

    return new ZkNodeProps(remoteMessage);
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest request, SolrQueryResponse response)
      throws Exception {
    final var api = new RenameCollection(coreContainer, request, response);
    final var params = request.getParams();
    params.required().check(COLLECTION_PROP, TARGET);
    final var requestBody = new RenameCollectionRequestBody();
    requestBody.to = params.get(TARGET);
    // Optional parameters
    requestBody.async = params.get(ASYNC);
    requestBody.followAliases = params.getBool(FOLLOW_ALIASES);

    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        response, api.renameCollection(params.get(COLLECTION_PROP), requestBody));
  }
}
