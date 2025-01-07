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
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.REQUESTID;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CancelCollectionApiCallApi;
import org.apache.solr.client.api.model.CancelCollectionApiResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CancelCollectionsApiCall extends AdminAPIBase implements CancelCollectionApiCallApi {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Inject
  public CancelCollectionsApiCall(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse cancelCollectionApiCall(String requestid, String asyncId)
      throws Exception {

    final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();

    final ZkNodeProps remoteMessage = createRemoteMessage(requestid, asyncId);
    log.info("cancel: {}", remoteMessage);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.CANCEL,
            DEFAULT_COLLECTION_OP_TIMEOUT);

    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }
    return populateResponse(remoteResponse);
  }

  public static ZkNodeProps createRemoteMessage(String requestid, String asyncId) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CANCEL.toLower());
    remoteMessage.put(REQUESTID, requestid);
    insertIfNotNull(remoteMessage, ASYNC, asyncId);

    return new ZkNodeProps(remoteMessage);
  }

  private CancelCollectionApiResponse populateResponse(SolrResponse remoteResponse) {
    final CancelCollectionApiResponse response =
        instantiateJerseyResponse(CancelCollectionApiResponse.class);

    response.asyncid = (String) remoteResponse.getResponse().get("asyncId");
    response.status = (String) remoteResponse.getResponse().get("status");
    response.msg = (String) remoteResponse.getResponse().get("message");

    return response;
  }
}
