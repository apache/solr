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
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;

import java.util.Map;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.DeleteReplicaPropertyApi;
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
import org.apache.solr.security.PermissionNameProvider;

/**
 * V2 API implementation for removing a property from a collection replica
 *
 * @see DeleteReplicaPropertyApi
 */
public class DeleteReplicaProperty extends AdminAPIBase implements DeleteReplicaPropertyApi {

  @Inject
  public DeleteReplicaProperty(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(PermissionNameProvider.Name.COLL_EDIT_PERM)
  public SolrJerseyResponse deleteReplicaProperty(
      String collName, String shardName, String replicaName, String propertyName) throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(collName, shardName, replicaName, propertyName);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.DELETEREPLICAPROP,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    return response;
  }

  public static SolrJerseyResponse invokeUsingV1Inputs(
      DeleteReplicaProperty apiInstance, SolrParams solrParams) throws Exception {
    final RequiredSolrParams requiredParams = solrParams.required();
    final String propNameToDelete = requiredParams.get(PROPERTY_PROP);
    final String trimmedPropNameToDelete =
        propNameToDelete.startsWith(PROPERTY_PREFIX)
            ? propNameToDelete.substring(PROPERTY_PREFIX.length())
            : propNameToDelete;
    return apiInstance.deleteReplicaProperty(
        requiredParams.get(COLLECTION_PROP),
        requiredParams.get(SHARD_ID_PROP),
        requiredParams.get(REPLICA_PROP),
        trimmedPropNameToDelete);
  }

  // XXX should this command support followAliases?
  public static ZkNodeProps createRemoteMessage(
      String collName, String shardName, String replicaName, String propName) {
    final Map<String, Object> messageProperties =
        Map.of(
            QUEUE_OPERATION,
            CollectionParams.CollectionAction.DELETEREPLICAPROP.toLower(),
            COLLECTION_PROP,
            collName,
            SHARD_ID_PROP,
            shardName,
            REPLICA_PROP,
            replicaName,
            PROPERTY_PROP,
            propName);
    return new ZkNodeProps(messageProperties);
  }
}
