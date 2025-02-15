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

import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;

import java.util.Map;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.tracing.TraceUtils;

/** A common parent for "admin" (i.e. container-level) APIs. */
public abstract class AdminAPIBase extends JerseyResource {

  protected final CoreContainer coreContainer;
  protected final SolrQueryRequest solrQueryRequest;
  protected final SolrQueryResponse solrQueryResponse;

  public AdminAPIBase(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    this.coreContainer = coreContainer;
    this.solrQueryRequest = solrQueryRequest;
    this.solrQueryResponse = solrQueryResponse;
  }

  protected CoreContainer fetchAndValidateZooKeeperAwareCoreContainer() {
    validateZooKeeperAwareCoreContainer(coreContainer);
    return coreContainer;
  }

  protected String resolveAndValidateAliasIfEnabled(
      String unresolvedCollectionName, boolean aliasResolutionEnabled) {
    final String resolvedCollectionName =
        aliasResolutionEnabled ? resolveAlias(unresolvedCollectionName) : unresolvedCollectionName;
    final ClusterState clusterState = coreContainer.getZkController().getClusterState();
    if (!clusterState.hasCollection(resolvedCollectionName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection '" + resolvedCollectionName + "' does not exist, no action taken.");
    }

    return resolvedCollectionName;
  }

  private String resolveAlias(String aliasName) {
    return coreContainer
        .getZkController()
        .getZkStateReader()
        .getAliases()
        .resolveSimpleAlias(aliasName);
  }

  public static void validateZooKeeperAwareCoreContainer(CoreContainer coreContainer) {
    if (coreContainer == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Core container instance missing");
    }

    // Make sure that the core is ZKAware
    if (!coreContainer.isZooKeeperAware()) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Solr instance is not running in SolrCloud mode.");
    }
  }

  protected String resolveCollectionName(String collName, boolean followAliases) {
    final String collectionName =
        followAliases
            ? coreContainer
                .getZkController()
                .getZkStateReader()
                .getAliases()
                .resolveSimpleAlias(collName)
            : collName;

    final ClusterState clusterState = coreContainer.getZkController().getClusterState();
    if (!clusterState.hasCollection(collectionName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection '" + collectionName + "' does not exist, no action taken.");
    }

    return collectionName;
  }

  /**
   * TODO Taken from CollectionsHandler.handleRequestBody, but its unclear where (if ever) this gets
   * cleared.
   */
  public static void recordCollectionForLogAndTracing(
      String collection, SolrQueryRequest solrQueryRequest) {
    MDCLoggingContext.setCollection(collection);
    TraceUtils.setDbInstance(solrQueryRequest, collection);
  }

  public void disableResponseCaching() {
    solrQueryResponse.setHttpCaching(false);
  }

  protected SolrResponse submitRemoteMessageAndHandleResponse(
      SubResponseAccumulatingJerseyResponse response,
      CollectionParams.CollectionAction action,
      ZkNodeProps remoteMessage,
      String asyncId)
      throws Exception {
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            action,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    if (asyncId != null) {
      response.requestId = asyncId;
    }

    // Values fetched from remoteResponse may be null
    response.successfulSubResponsesByNodeName = remoteResponse.getResponse().get("success");
    response.failedSubResponsesByNodeName = remoteResponse.getResponse().get("failure");

    return remoteResponse;
  }

  protected static void insertIfNotNull(Map<String, Object> destination, String key, Object value) {
    if (value != null) {
      destination.put(key, value);
    }
  }
}
