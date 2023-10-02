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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CollectionParams.SOURCE_NODES;
import static org.apache.solr.common.params.CollectionParams.TARGET_NODES;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams.CollectionAction;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API for migrating replicas from a set of nodes to another set of nodes. */
@Path("cluster/replicas/migrate")
public class MigrateReplicasAPI extends AdminAPIBase {

  @Inject
  public MigrateReplicasAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  @Operation(summary = "Migrate Replicas from a given set of nodes.")
  public SolrJerseyResponse migrateReplicas(
      @RequestBody(description = "Contains user provided parameters", required = true)
          MigrateReplicasRequestBody requestBody)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    // TODO Record node for log and tracing
    final ZkNodeProps remoteMessage = createRemoteMessage(requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionAction.MIGRATE_REPLICAS,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

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
      insertIfNotNull(remoteMessage, ASYNC, requestBody.async);
    } else {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "No request body sent with request. The MigrateReplicas API requires a body.");
    }
    remoteMessage.put(QUEUE_OPERATION, CollectionAction.MIGRATE_REPLICAS.toLower());

    return new ZkNodeProps(remoteMessage);
  }

  public static class MigrateReplicasRequestBody implements JacksonReflectMapWriter {

    public MigrateReplicasRequestBody() {}

    public MigrateReplicasRequestBody(
        Set<String> sourceNodes, Set<String> targetNodes, Boolean waitForFinalState, String async) {
      this.sourceNodes = sourceNodes;
      this.targetNodes = targetNodes;
      this.waitForFinalState = waitForFinalState;
      this.async = async;
    }

    @Schema(description = "The set of nodes which all replicas will be migrated off of.")
    @JsonProperty(value = "sourceNodes", required = true)
    public Set<String> sourceNodes;

    @Schema(
        description =
            "A set of nodes to migrate the replicas to. If this is not provided, then the API will use the live data nodes not in 'sourceNodes'.")
    @JsonProperty(value = "targetNodes")
    public Set<String> targetNodes;

    @Schema(
        description =
            "If true, the request will complete only when all affected replicas become active. "
                + "If false, the API will return the status of the single action, which may be "
                + "before the new replicas are online and active.")
    @JsonProperty("waitForFinalState")
    public Boolean waitForFinalState = false;

    @Schema(description = "Request ID to track this action which will be processed asynchronously.")
    @JsonProperty("async")
    public String async;
  }
}
