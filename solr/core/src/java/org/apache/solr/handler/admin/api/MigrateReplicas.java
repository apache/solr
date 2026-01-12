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

import static org.apache.solr.common.params.CollectionParams.SOURCE_NODES;
import static org.apache.solr.common.params.CollectionParams.TARGET_NODES;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.MigrateReplicasApi;
import org.apache.solr.client.api.model.MigrateReplicasRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API for migrating replicas from a set of nodes to another set of nodes. */
public class MigrateReplicas extends AdminAPIBase implements MigrateReplicasApi {

  @Inject
  public MigrateReplicas(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse migrateReplicas(MigrateReplicasRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(null, solrQueryRequest);
    submitRemoteMessageAndHandleResponse(
        response,
        CollectionAction.MIGRATE_REPLICAS,
        createRemoteMessage(requestBody),
        requestBody.async);

    disableResponseCaching();
    return response;
  }

  public ZkNodeProps createRemoteMessage(MigrateReplicasRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    if (requestBody != null) {
      if (requestBody.sourceNodes == null || requestBody.sourceNodes.isEmpty()) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "No 'sourceNodes' provided in the request body. The MigrateReplicas API requires a 'sourceNodes' list in the request body.");
      }
      insertIfNotNull(remoteMessage, SOURCE_NODES, requestBody.sourceNodes);
      insertIfNotNull(remoteMessage, TARGET_NODES, requestBody.targetNodes);
      insertIfNotNull(remoteMessage, WAIT_FOR_FINAL_STATE, requestBody.waitForFinalState);
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "No request body sent with request. The MigrateReplicas API requires a body.");
    }

    return new ZkNodeProps(remoteMessage);
  }
}
