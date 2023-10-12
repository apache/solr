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
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.CREATE_COLLECTION_KEY;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.COLL_CONF;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.CREATE_NODE_SET_SHUFFLE_PARAM;
import static org.apache.solr.common.params.CollectionAdminParams.NRT_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CollectionAdminParams.PULL_REPLICAS;
import static org.apache.solr.common.params.CollectionAdminParams.REPLICATION_FACTOR;
import static org.apache.solr.common.params.CollectionAdminParams.TLOG_REPLICAS;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.CreateCollectionRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for restoring data into a collection
 *
 * <p>This API is analogous to the v1 /admin/collections?action=RESTORE command.
 */
@Path("/backups/{backupName}/restore")
public class RestoreCollectionAPI extends BackupAPIBase {

  private static final Set<String> CREATE_PARAM_ALLOWLIST =
      Set.of(
          COLL_CONF,
          REPLICATION_FACTOR,
          NRT_REPLICAS,
          TLOG_REPLICAS,
          PULL_REPLICAS,
          CREATE_NODE_SET_PARAM,
          CREATE_NODE_SET_SHUFFLE_PARAM);

  @Inject
  public RestoreCollectionAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SubResponseAccumulatingJerseyResponse restoreCollection(
      @PathParam("backupName") String backupName, RestoreCollectionRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SubResponseAccumulatingJerseyResponse.class);

    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    if (requestBody.collection == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Required parameter 'collection' missing");
    }
    if (backupName == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Required parameter 'backupName' missing");
    }

    final String collectionName = requestBody.collection;
    SolrIdentifierValidator.validateCollectionName(collectionName);
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);
    if (coreContainer.getAliases().hasAlias(collectionName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Collection '" + collectionName + "' is an existing alias, no action taken.");
    }

    requestBody.location =
        getAndValidateBackupLocation(requestBody.repository, requestBody.location);

    final var createRequestBody = requestBody.createCollectionParams;
    if (createRequestBody != null) {
      CreateCollection.populateDefaultsIfNecessary(coreContainer, createRequestBody);
      CreateCollection.validateRequestBody(createRequestBody);
      if (Boolean.FALSE.equals(createRequestBody.createReplicas)) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Replica-creation cannot be disabled for collections created by a restore operation.");
      }
    }

    final ZkNodeProps remoteMessage = createRemoteMessage(backupName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.RESTORE,
            DEFAULT_COLLECTION_OP_TIMEOUT);

    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    // Values fetched from remoteResponse may be null
    response.successfulSubResponsesByNodeName = remoteResponse.getResponse().get("success");
    response.failedSubResponsesByNodeName = remoteResponse.getResponse().get("failure");

    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String backupName, RestoreCollectionRequestBody requestBody) {
    final Map<String, Object> remoteMessage = requestBody.toMap(new HashMap<>());

    // If the RESTORE is setup to create a new collection, copy those parameters first
    final var createReqBody = requestBody.createCollectionParams;
    if (createReqBody != null) {
      // RESTORE only supports a subset of collection-creation params, so filter by those when
      // constructing the remote message
      remoteMessage.remove("create-collection");
      CreateCollection.createRemoteMessage(createReqBody).getProperties().entrySet().stream()
          .filter(
              e ->
                  CREATE_PARAM_ALLOWLIST.contains(e.getKey())
                      || e.getKey().startsWith(PROPERTY_PREFIX))
          .forEach(e -> remoteMessage.put(e.getKey(), e.getValue()));
    }

    // Copy restore-specific parameters
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.RESTORE.toLower());
    remoteMessage.put(COLLECTION_PROP, requestBody.collection);
    remoteMessage.put(NAME, backupName);
    remoteMessage.put(BACKUP_LOCATION, requestBody.location);
    if (requestBody.backupId != null) remoteMessage.put(BACKUP_ID, requestBody.backupId);
    if (requestBody.repository != null)
      remoteMessage.put(BACKUP_REPOSITORY, requestBody.repository);
    return new ZkNodeProps(remoteMessage);
  }

  public static SolrJerseyResponse invokeFromV1Params(
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse,
      CoreContainer coreContainer)
      throws Exception {
    final var params = solrQueryRequest.getParams();
    params.required().check(NAME, COLLECTION_PROP);
    final String backupName = params.get(NAME);
    final var requestBody = RestoreCollectionRequestBody.fromV1Params(params);

    final var restoreApi =
        new RestoreCollectionAPI(coreContainer, solrQueryRequest, solrQueryResponse);
    return restoreApi.restoreCollection(backupName, requestBody);
  }

  /** Request body for the v2 "restore collection" API. */
  public static class RestoreCollectionRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(required = true)
    public String collection;

    @JsonProperty public String location;
    @JsonProperty public String repository;
    @JsonProperty public Integer backupId;

    @JsonProperty(CREATE_COLLECTION_KEY)
    public CreateCollectionRequestBody createCollectionParams;

    @JsonProperty public String async;

    public static RestoreCollectionRequestBody fromV1Params(SolrParams solrParams) {
      final var restoreBody = new RestoreCollectionRequestBody();
      restoreBody.collection = solrParams.get(COLLECTION_PROP);
      restoreBody.location = solrParams.get(BACKUP_LOCATION);
      restoreBody.repository = solrParams.get(BACKUP_REPOSITORY);
      restoreBody.backupId = solrParams.getInt(BACKUP_ID);
      restoreBody.async = solrParams.get(ASYNC);

      restoreBody.createCollectionParams =
          CreateCollection.createRequestBodyFromV1Params(solrParams, false);

      return restoreBody;
    }
  }
}
