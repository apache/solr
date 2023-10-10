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

import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.DeleteCollectionSnapshotApi;
import org.apache.solr.client.api.model.DeleteCollectionSnapshotResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API impl for Deleting Collection Snapshots. */
public class DeleteCollectionSnapshot extends AdminAPIBase implements DeleteCollectionSnapshotApi {

  @Inject
  public DeleteCollectionSnapshot(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(COLL_EDIT_PERM)
  public DeleteCollectionSnapshotResponse deleteSnapshot(
      String collName, String snapshotName, boolean followAliases, String asyncId)
      throws Exception {
    final var response = instantiateJerseyResponse(DeleteCollectionSnapshotResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);

    final String collectionName = resolveCollectionName(collName, followAliases);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(collectionName, followAliases, snapshotName, asyncId);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.DELETESNAPSHOT,
            DEFAULT_COLLECTION_OP_TIMEOUT);

    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    response.collection = collName;
    response.snapshotName = snapshotName;
    response.followAliases = followAliases;
    response.requestId = asyncId;

    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, boolean followAliases, String snapshotName, String asyncId) {
    final Map<String, Object> remoteMessage = new HashMap<>();

    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.DELETESNAPSHOT.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(CoreAdminParams.COMMIT_NAME, snapshotName);
    remoteMessage.put(FOLLOW_ALIASES, followAliases);

    if (asyncId != null) remoteMessage.put(ASYNC, asyncId);

    return new ZkNodeProps(remoteMessage);
  }
}
