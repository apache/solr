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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.client.solrj.request.beans.V2ApiConstants.CREATE_COLLECTION_KEY;
import static org.apache.solr.cloud.Overseer.QUEUE_OPERATION;
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

// TODO NOCOMMIT - can the collectionName be in the path, since it may or may not be created by this command.
/**
 * V2 API for restoring data into a collection
 *
 * <p>This API is analogous to the v1 /admin/collections?action=RESTORE command.
 */
@Path("/collections/{collectionName}/backups/{backupName}/restore")
public class RestoreCollectionAPI extends AdminAPIBase {
    @Inject
    public RestoreCollectionAPI(CoreContainer coreContainer, SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse) {
        super(coreContainer, solrQueryRequest, solrQueryResponse);
    }


    @POST
    @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
    @PermissionName(COLL_EDIT_PERM)
    public SolrJerseyResponse restoreCollection(
            @PathParam("collectionName") String collectionName,
            @PathParam("backupName") String backupName,
            RestoreCollectionRequestBody requestBody) throws Exception {
        final SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);
        recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

        if (requestBody == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
        }
        if (collectionName == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required parameter 'collection' missing");
        }
        if (backupName == null) {
            throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Required parameter 'backupName' missing");
        }

        SolrIdentifierValidator.validateCollectionName(collectionName);
        if (coreContainer.getAliases().hasAlias(collectionName)) {
            throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "Collection '" + collectionName + "' is an existing alias, no action taken.");
        }

        final BackupRepository repository = coreContainer.newBackupRepository(requestBody.repository);
        requestBody.location = CreateCollectionBackupAPI.getLocation(coreContainer, repository, requestBody.location);

        // Check if the specified location is valid for this repository.
        final URI uri = repository.createDirectoryURI(requestBody.location);
        try {
            if (!repository.exists(uri)) {
                throw new SolrException(
                        SolrException.ErrorCode.SERVER_ERROR, "specified location " + uri + " does not exist.");
            }
        } catch (IOException ex) {
            throw new SolrException(
                    SolrException.ErrorCode.SERVER_ERROR,
                    "Failed to check the existence of " + uri + ". Is it valid?",
                    ex);
        }

        final var createRequestBody = requestBody.createCollectionParams;
        if (createRequestBody != null) {
            createRequestBody.validate();
            if (Boolean.FALSE.equals(createRequestBody.createReplicas)) {
                throw new SolrException(
                        SolrException.ErrorCode.BAD_REQUEST,
                        "Replica-creation cannot be disabled for collections created by a restore operation.");
            }
        }

        final ZkNodeProps remoteMessage =
                createRemoteMessage(coreContainer, collectionName, backupName, requestBody);
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

        // TODO NOCOMMIT, what should the response actually look like - don't I have some metadata about the restored data?
        return response;
    }

    // TODO NOCOMMIT Investigate removing the CoreContainer stuff from this and create-collection's cRM method...
    public static ZkNodeProps createRemoteMessage(CoreContainer coreContainer, String collectionName, String backupName, RestoreCollectionRequestBody requestBody) throws Exception {
        final Map<String, Object> remoteMessage = requestBody.toMap(new HashMap<>());

        // If the RESTORE is setup to create a new collection, copy those parameters first
        final var createReqBody = requestBody.createCollectionParams;
        if (createReqBody != null) {
            final var createMessage = CreateCollectionAPI.createRemoteMessage(coreContainer, createReqBody);
            remoteMessage.putAll(createMessage.getProperties());
        }

        // Copy restore-specific parameters
        remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.RESTORE.toLower());
        remoteMessage.put(COLLECTION_PROP, collectionName);
        remoteMessage.put(NAME, backupName);
        remoteMessage.put(BACKUP_LOCATION, requestBody.location);
        if (requestBody.backupId != null) remoteMessage.put(BACKUP_ID, requestBody.backupId);
        if (requestBody.repository != null) remoteMessage.put(BACKUP_REPOSITORY, requestBody.repository);
        return new ZkNodeProps(remoteMessage);
    }

    public static SolrJerseyResponse invokeFromV1Params(SolrQueryRequest solrQueryRequest, SolrQueryResponse solrQueryResponse, CoreContainer coreContainer) throws Exception {
        final var params = solrQueryRequest.getParams();
        params.required().check(NAME, COLLECTION_PROP);
        final String collectionName = params.get(COLLECTION_PROP);
        final String backupName = params.get(NAME);
        final var requestBody = RestoreCollectionRequestBody.fromV1Params(params);

        final var restoreApi = new RestoreCollectionAPI(coreContainer, solrQueryRequest, solrQueryResponse);
        return restoreApi.restoreCollection(collectionName, backupName, requestBody);
    }

    /**
     * Request body for the v2 "restore collection" API.
     */
    public static class RestoreCollectionRequestBody implements JacksonReflectMapWriter {
        @JsonProperty public String location;
        @JsonProperty public String repository;
        @JsonProperty public Integer backupId;
        @JsonProperty(CREATE_COLLECTION_KEY) public CreateCollectionAPI.CreateCollectionRequestBody createCollectionParams;
        @JsonProperty public String async;

        public static RestoreCollectionRequestBody fromV1Params(SolrParams solrParams) {
            final var restoreBody = new RestoreCollectionRequestBody();
            restoreBody.location = solrParams.get(BACKUP_LOCATION);
            restoreBody.repository = solrParams.get(BACKUP_REPOSITORY);
            restoreBody.backupId = solrParams.getInt(BACKUP_ID);
            restoreBody.async = solrParams.get(ASYNC);

            restoreBody.createCollectionParams = CreateCollectionAPI.buildRequestBodyFromParams(solrParams, false);

            return restoreBody;
        }
    }
}
