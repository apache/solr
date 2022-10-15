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
import static org.apache.solr.cloud.api.collections.CollectionHandlingUtils.SHARD_UNIQUE;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.PROPERTY_VALUE_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.REPLICA_PROP;
import static org.apache.solr.common.cloud.ZkStateReader.SHARD_ID_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for adding a property to a collection replica
 *
 * <p>This API is analogous to the v1 /admin/collections?action=ADDREPLICAPROP command.
 */
@Path("/collections/{collName}/shards/{shardName}/replicas/{replicaName}/properties/{propName}")
public class AddReplicaPropertyAPI extends AdminAPIBase {

  @Inject
  public AddReplicaPropertyAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @PUT
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse addReplicaProperty(
      @Parameter(
              description = "The name of the collection the replica belongs to.",
              required = true)
          @PathParam("collName")
          String collName,
      @Parameter(description = "The name of the shard the replica belongs to.", required = true)
          @PathParam("shardName")
          String shardName,
      @Parameter(description = "The replica, e.g., `core_node1`.", required = true)
          @PathParam("replicaName")
          String replicaName,
      @Parameter(description = "The name of the property to add.", required = true)
          @PathParam("propName")
          String propertyName,
      @RequestBody(
              description = "The value of the replica property to create or update",
              required = true)
          AddReplicaPropertyRequestBody requestBody)
      throws Exception {
    final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collName, solrQueryRequest);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(collName, shardName, replicaName, propertyName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.ADDREPLICAPROP,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    disableResponseCaching();
    return response;
  }

  public ZkNodeProps createRemoteMessage(
      String collName,
      String shardName,
      String replicaName,
      String propertyName,
      AddReplicaPropertyRequestBody requestBody) {
    final Map<String, Object> remoteMessage = new HashMap<>();
    remoteMessage.put(COLLECTION_PROP, collName);
    remoteMessage.put(PROPERTY_PROP, propertyName);
    remoteMessage.put(SHARD_ID_PROP, shardName);
    remoteMessage.put(REPLICA_PROP, replicaName);
    remoteMessage.put(PROPERTY_VALUE_PROP, requestBody.value);
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.ADDREPLICAPROP.toLower());
    if (requestBody.shardUnique != null) {
      remoteMessage.put(SHARD_UNIQUE, requestBody.shardUnique);
    }
    final String prefixedPropertyName = PROPERTY_PREFIX + propertyName;
    boolean uniquePerSlice = requestBody.shardUnique == null ? false : requestBody.shardUnique;

    // Check if we're trying to set a property with parameters that allow us to set the
    // property on multiple replicas in a slice on properties that are known to only be
    // one-per-slice and error out if so.
    if (requestBody.shardUnique != null
        && SliceMutator.SLICE_UNIQUE_BOOLEAN_PROPERTIES.contains(
            prefixedPropertyName.toLowerCase(Locale.ROOT))
        && uniquePerSlice == false) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Overseer replica property command received for property "
              + prefixedPropertyName
              + " with the "
              + SHARD_UNIQUE
              + " parameter set to something other than 'true'. No action taken.");
    }

    return new ZkNodeProps(remoteMessage);
  }

  public static class AddReplicaPropertyRequestBody implements JacksonReflectMapWriter {

    public AddReplicaPropertyRequestBody() {}

    public AddReplicaPropertyRequestBody(String value) {
      this.value = value;
    }

    @Schema(description = "The value to assign to the property.", required = true)
    @JsonProperty("value")
    public String value;

    @Schema(
        description =
            "If `true`, then setting this property in one replica will remove the property from all other replicas in that shard. The default is `false`.\\nThere is one pre-defined property `preferredLeader` for which `shardUnique` is forced to `true` and an error returned if `shardUnique` is explicitly set to `false`.",
        defaultValue = "false")
    @JsonProperty("shardUnique")
    public Boolean shardUnique;
  }
}
