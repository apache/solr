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
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.ONLY_ACTIVE_NODES;
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARD_UNIQUE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for insuring that a particular property is distributed evenly amongst the physical nodes
 * comprising a collection.
 *
 * <p>The new API (POST /v2/collections/collectionName/balance-shard-unique {...} ) is analogous to
 * the v1 /admin/collections?action=BALANCESHARDUNIQUE command.
 */
@Path("/collections/{collectionName}/balance-shard-unique")
public class BalanceShardUniqueAPI extends AdminAPIBase {
  @Inject
  public BalanceShardUniqueAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse balanceShardUnique(
      @PathParam("collectionName") String collectionName, BalanceShardUniqueRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(PROPERTY_PROP, requestBody.property);
    validatePropertyToBalance(requestBody.property, Boolean.TRUE.equals(requestBody.shardUnique));
    fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    final ZkNodeProps remoteMessage = createRemoteMessage(collectionName, requestBody);
    submitRemoteMessageAndHandleResponse(
        response,
        CollectionParams.CollectionAction.BALANCESHARDUNIQUE,
        remoteMessage,
        requestBody.asyncId);

    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, BalanceShardUniqueRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(
        QUEUE_OPERATION, CollectionParams.CollectionAction.BALANCESHARDUNIQUE.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(PROPERTY_PROP, requestBody.property);
    insertIfNotNull(remoteMessage, ONLY_ACTIVE_NODES, requestBody.onlyActiveNodes);
    insertIfNotNull(remoteMessage, SHARD_UNIQUE, requestBody.shardUnique);
    insertIfNotNull(remoteMessage, ASYNC, requestBody.asyncId);

    return new ZkNodeProps(remoteMessage);
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse)
      throws Exception {
    final var api = new BalanceShardUniqueAPI(coreContainer, solrQueryRequest, solrQueryResponse);
    final SolrParams params = solrQueryRequest.getParams();
    params.required().check(COLLECTION_PROP, PROPERTY_PROP);

    final String collection = params.get(COLLECTION_PROP);
    final var requestBody = new BalanceShardUniqueRequestBody();
    requestBody.property = params.get(PROPERTY_PROP);
    requestBody.onlyActiveNodes = params.getBool(ONLY_ACTIVE_NODES);
    requestBody.shardUnique = params.getBool(SHARD_UNIQUE);
    requestBody.asyncId = params.get(ASYNC);

    V2ApiUtils.squashIntoSolrResponseWithoutHeader(
        solrQueryResponse, api.balanceShardUnique(collection, requestBody));
  }

  public static class BalanceShardUniqueRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(required = true)
    public String property;

    @JsonProperty(ONLY_ACTIVE_NODES)
    public Boolean onlyActiveNodes;

    @JsonProperty public Boolean shardUnique;

    @JsonProperty(ASYNC)
    public String asyncId;
  }

  private void validatePropertyToBalance(String prop, boolean shardUnique) {
    prop = prop.toLowerCase(Locale.ROOT);
    if (!prop.startsWith(PROPERTY_PREFIX)) {
      prop = PROPERTY_PREFIX + prop;
    }

    if (!shardUnique && !SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(prop)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Balancing properties amongst replicas in a slice requires that"
              + " the property be pre-defined as a unique property (e.g. 'preferredLeader') or that 'shardUnique' be set to 'true'. "
              + " Property: "
              + prop
              + " shardUnique: "
              + shardUnique);
    }
  }
}
