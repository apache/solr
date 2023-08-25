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
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.CREATE_NODE_SET;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.NRT_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.PULL_REPLICAS;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_TYPE;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.TLOG_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.SKIP_NODE_ASSIGNMENT;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonAdminParams.WAIT_FOR_FINAL_STATE;
import static org.apache.solr.common.params.CoreAdminParams.DATA_DIR;
import static org.apache.solr.common.params.CoreAdminParams.INSTANCE_DIR;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.NODE;
import static org.apache.solr.common.params.CoreAdminParams.ULOG_DIR;
import static org.apache.solr.common.params.ShardParams._ROUTE_;
import static org.apache.solr.handler.admin.api.CreateCollectionAPI.copyPrefixedPropertiesWithoutPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for adding a new replica to an existing shard.
 *
 * <p>This API (POST /v2/collections/cName/shards/sName/replicas {...}) is analogous to the v1
 * /admin/collections?action=ADDREPLICA command.
 */
@Path("/collections/{collectionName}/shards/{shardName}/replicas")
public class CreateReplicaAPI extends AdminAPIBase {

  @Inject
  public CreateReplicaAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse createReplica(
      @PathParam("collectionName") String collectionName,
      @PathParam("shardName") String shardName,
      AddReplicaRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);
    if (requestBody == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Required request-body is missing");
    }
    ensureRequiredParameterProvided(COLLECTION_PROP, collectionName);
    ensureRequiredParameterProvided(SHARD_ID_PROP, shardName);
    final String resolvedCollectionName =
        resolveAndValidateAliasIfEnabled(
            collectionName, Boolean.TRUE.equals(requestBody.followAliases));

    final ZkNodeProps remoteMessage =
        createRemoteMessage(resolvedCollectionName, shardName, requestBody);
    submitRemoteMessageAndHandleResponse(
        response, CollectionParams.CollectionAction.ADDREPLICA, remoteMessage, requestBody.asyncId);
    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, String shardName, AddReplicaRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.ADDREPLICA.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(SHARD_ID_PROP, shardName);
    insertIfNotNull(remoteMessage, CoreAdminParams.NAME, requestBody.name);
    insertIfNotNull(remoteMessage, _ROUTE_, requestBody.route);
    insertIfNotNull(remoteMessage, NODE, requestBody.node);
    if (CollectionUtil.isNotEmpty(requestBody.nodeSet)) {
      remoteMessage.put(CREATE_NODE_SET_PARAM, String.join(",", requestBody.nodeSet));
    }
    insertIfNotNull(remoteMessage, SKIP_NODE_ASSIGNMENT, requestBody.skipNodeAssignment);
    insertIfNotNull(remoteMessage, INSTANCE_DIR, requestBody.instanceDir);
    insertIfNotNull(remoteMessage, DATA_DIR, requestBody.dataDir);
    insertIfNotNull(remoteMessage, ULOG_DIR, requestBody.ulogDir);
    insertIfNotNull(remoteMessage, REPLICA_TYPE, requestBody.type);
    insertIfNotNull(remoteMessage, WAIT_FOR_FINAL_STATE, requestBody.waitForFinalState);
    insertIfNotNull(remoteMessage, NRT_REPLICAS, requestBody.nrtReplicas);
    insertIfNotNull(remoteMessage, TLOG_REPLICAS, requestBody.tlogReplicas);
    insertIfNotNull(remoteMessage, PULL_REPLICAS, requestBody.pullReplicas);
    insertIfNotNull(remoteMessage, FOLLOW_ALIASES, requestBody.followAliases);
    insertIfNotNull(remoteMessage, ASYNC, requestBody.asyncId);

    if (requestBody.properties != null) {
      requestBody
          .properties
          .entrySet()
          .forEach(
              entry -> {
                remoteMessage.put(PROPERTY_PREFIX + entry.getKey(), entry.getValue());
              });
    }

    return new ZkNodeProps(remoteMessage);
  }

  public static class AddReplicaRequestBody implements JacksonReflectMapWriter {
    @JsonProperty public String name;
    @JsonProperty public String type; // TODO Make this an enum - see SOLR-15796
    @JsonProperty public String instanceDir;
    @JsonProperty public String dataDir;
    @JsonProperty public String ulogDir;
    @JsonProperty public String route;
    @JsonProperty public Integer nrtReplicas;
    @JsonProperty public Integer tlogReplicas;
    @JsonProperty public Integer pullReplicas;
    @JsonProperty public Boolean waitForFinalState;
    @JsonProperty public Boolean followAliases;

    @JsonProperty(ASYNC)
    public String asyncId;

    // TODO This cluster of properties could probably be simplified down to just "nodeSet".  See
    // SOLR-15542
    @JsonProperty public String node;

    @JsonProperty("nodeSet")
    public List<String> nodeSet;

    @JsonProperty public Boolean skipNodeAssignment;

    @JsonProperty public Map<String, String> properties;

    public static AddReplicaRequestBody fromV1Params(SolrParams params) {
      final var requestBody = new AddReplicaRequestBody();

      requestBody.name = params.get(NAME);
      requestBody.type = params.get(REPLICA_TYPE);
      requestBody.instanceDir = params.get(INSTANCE_DIR);
      requestBody.dataDir = params.get(DATA_DIR);
      requestBody.ulogDir = params.get(ULOG_DIR);
      requestBody.route = params.get(_ROUTE_);
      requestBody.nrtReplicas = params.getInt(NRT_REPLICAS);
      requestBody.tlogReplicas = params.getInt(TLOG_REPLICAS);
      requestBody.pullReplicas = params.getInt(PULL_REPLICAS);
      requestBody.waitForFinalState = params.getBool(WAIT_FOR_FINAL_STATE);
      requestBody.followAliases = params.getBool(FOLLOW_ALIASES);
      requestBody.asyncId = params.get(ASYNC);

      requestBody.node = params.get(NODE);
      if (params.get(CREATE_NODE_SET_PARAM) != null) {
        requestBody.nodeSet = Arrays.asList(params.get(CREATE_NODE_SET).split(","));
      }
      requestBody.skipNodeAssignment = params.getBool(SKIP_NODE_ASSIGNMENT);

      requestBody.properties =
          copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);

      return requestBody;
    }
  }
}
