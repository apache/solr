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
import static org.apache.solr.common.params.CollectionParams.SOURCE_NODE;
import static org.apache.solr.common.params.CollectionParams.TARGET_NODE;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.ReplaceNodeApi;
import org.apache.solr.client.api.model.ReplaceNodeRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for recreating replicas in one node (the source) on another node(s) (the target).
 *
 * <p>This API is analogous to the v1 /admin/collections?action=REPLACENODE command.
 */
public class ReplaceNode extends AdminAPIBase implements ReplaceNodeApi {

  @Inject
  public ReplaceNode(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse replaceNode(String sourceNodeName, ReplaceNodeRequestBody requestBody)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    // TODO Record node for log and tracing
    final ZkNodeProps remoteMessage = createRemoteMessage(sourceNodeName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.REPLACENODE,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    disableResponseCaching();
    return response;
  }

  public ZkNodeProps createRemoteMessage(String nodeName, ReplaceNodeRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(SOURCE_NODE, nodeName);
    if (requestBody != null) {
      insertIfValueNotNull(remoteMessage, TARGET_NODE, requestBody.targetNodeName);
      insertIfValueNotNull(remoteMessage, WAIT_FOR_FINAL_STATE, requestBody.waitForFinalState);
      insertIfValueNotNull(remoteMessage, ASYNC, requestBody.async);
    }
    remoteMessage.put(QUEUE_OPERATION, CollectionAction.REPLACENODE.toLower());

    return new ZkNodeProps(remoteMessage);
  }

  private void insertIfValueNotNull(Map<String, Object> dest, String key, Object value) {
    if (value != null) {
      dest.put(key, value);
    }
  }
}
