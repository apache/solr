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
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.DeleteNodeApi;
import org.apache.solr.client.api.model.DeleteNodeRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for deleting all replicas of all collections in one node. Please note that the node itself
 * will remain as a live node after this operation.
 *
 * <p>This API is analogous to the V1 /admin/collections?action=DELETENODE
 */
public class DeleteNode extends AdminAPIBase implements DeleteNodeApi {

  @Inject
  public DeleteNode(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse deleteNode(String nodeName, DeleteNodeRequestBody requestBody)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    final ZkNodeProps remoteMessage = createRemoteMessage(nodeName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.DELETENODE,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }
    disableResponseCaching();
    return response;
  }

  public static SolrJerseyResponse invokeUsingV1Inputs(DeleteNode apiInstance, SolrParams params)
      throws Exception {
    final RequiredSolrParams requiredParams = params.required();
    final var requestBody = new DeleteNodeRequestBody();
    requestBody.async = params.get(ASYNC);
    return apiInstance.deleteNode(requiredParams.get(NODE), requestBody);
  }

  public static ZkNodeProps createRemoteMessage(
      String nodeName, DeleteNodeRequestBody requestBody) {
    Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(NODE, nodeName);
    if (requestBody != null) {
      if (requestBody.async != null) {
        remoteMessage.put(ASYNC, requestBody.async);
      }
    }
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.DELETENODE.toLower());

    return new ZkNodeProps(remoteMessage);
  }
}
