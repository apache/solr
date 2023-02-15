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

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.RequiredSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for deleting all replicas of all collections in one node. Please note that the node itself
 * will remain as a live node after this operation.
 *
 * <p>This API is analogous to the V1 /admin/collections?action=DELETENODE
 */
@Path("cluster/nodes/{nodeName}/clear/")
public class DeleteNodeAPI extends AdminAPIBase {

  @Inject
  public DeleteNodeAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse deleteNode(
      @Parameter(
              description =
                  "The name of the node to be cleared.  Usually of the form 'host:1234_solr'.",
              required = true)
          @PathParam("nodeName")
          String nodeName,
      @RequestBody(description = "Contains user provided parameters", required = true)
          DeleteNodeRequestBody requestBody)
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

  public static SolrJerseyResponse invokeUsingV1Inputs(DeleteNodeAPI apiInstance, SolrParams params)
      throws Exception {
    final RequiredSolrParams requiredParams = params.required();
    final DeleteNodeRequestBody requestBody = new DeleteNodeRequestBody(params.get(ASYNC));
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

  public static class DeleteNodeRequestBody implements JacksonReflectMapWriter {

    public DeleteNodeRequestBody() {}

    public DeleteNodeRequestBody(String async) {
      this.async = async;
    }

    @Schema(description = "Request ID to track this action which will be processed asynchronously.")
    @JsonProperty("async")
    public String async;
  }
}
