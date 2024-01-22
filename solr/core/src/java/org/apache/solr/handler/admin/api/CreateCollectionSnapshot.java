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
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.CreateCollectionSnapshotApi;
import org.apache.solr.client.api.model.CreateCollectionSnapshotRequestBody;
import org.apache.solr.client.api.model.CreateCollectionSnapshotResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API implementation for creating a collection-level snapshot. */
public class CreateCollectionSnapshot extends AdminAPIBase implements CreateCollectionSnapshotApi {

  @Inject
  public CreateCollectionSnapshot(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  /** This API is analogous to V1's (POST /solr/admin/collections?action=CREATESNAPSHOT) */
  @Override
  @PermissionName(COLL_EDIT_PERM)
  public CreateCollectionSnapshotResponse createCollectionSnapshot(
      String collName, String snapshotName, CreateCollectionSnapshotRequestBody requestBody)
      throws Exception {
    final CreateCollectionSnapshotResponse response =
        instantiateJerseyResponse(CreateCollectionSnapshotResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);

    final String collectionName = resolveCollectionName(collName, requestBody.followAliases);

    final SolrZkClient client = coreContainer.getZkController().getZkClient();
    if (SolrSnapshotManager.snapshotExists(client, collectionName, snapshotName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Snapshot with name '"
              + snapshotName
              + "' already exists for collection '"
              + collectionName
              + "', no action taken.");
    }

    final ZkNodeProps remoteMessage =
        createRemoteMessage(collName, requestBody.followAliases, snapshotName, requestBody.async);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.CREATESNAPSHOT,
            DEFAULT_COLLECTION_OP_TIMEOUT);

    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    response.collection = collName;
    response.followAliases = requestBody.followAliases;
    response.snapshotName = snapshotName;
    response.requestId = requestBody.async;

    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, boolean followAliases, String snapshotName, String asyncId) {
    final Map<String, Object> remoteMessage = new HashMap<>();

    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.CREATESNAPSHOT.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(CoreAdminParams.COMMIT_NAME, snapshotName);
    remoteMessage.put(FOLLOW_ALIASES, followAliases);
    if (asyncId != null) remoteMessage.put(ASYNC, asyncId);

    return new ZkNodeProps(remoteMessage);
  }
}
