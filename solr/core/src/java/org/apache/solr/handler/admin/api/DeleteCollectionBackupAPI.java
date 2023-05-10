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
import static org.apache.solr.common.SolrException.ErrorCode.BAD_REQUEST;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_ID;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_PURGE_UNUSED;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.COLLECTION;
import static org.apache.solr.common.params.CoreAdminParams.MAX_NUM_BACKUP_POINTS;
import static org.apache.solr.common.params.CoreAdminParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.jersey.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API definitions for collection-backup deletion.
 *
 * <p>These APIs are equivalent to the v1 '/admin/collections?action=DELETEBACKUP' command.
 */
public class DeleteCollectionBackupAPI extends BackupAPIBase {

  private final ObjectMapper objectMapper;

  @Inject
  public DeleteCollectionBackupAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);

    this.objectMapper = SolrJacksonMapper.getObjectMapper();
  }

  @Path("/backups/{backupName}/versions/{backupId}")
  @DELETE
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public BackupDeletionResponseBody deleteSingleBackupById(
      @PathParam("backupName") String backupName,
      @PathParam(BACKUP_ID) String backupId,
      // Optional parameters below
      @QueryParam(BACKUP_LOCATION) String location,
      @QueryParam(BACKUP_REPOSITORY) String repositoryName,
      @QueryParam(ASYNC) String asyncId)
      throws Exception {
    final var response = instantiateJerseyResponse(BackupDeletionResponseBody.class);
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    ensureRequiredParameterProvided(NAME, backupName);
    ensureRequiredParameterProvided(BACKUP_ID, backupId);
    location = getAndValidateBackupLocation(repositoryName, location);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(backupName, backupId, null, null, location, repositoryName, asyncId);
    final var remoteResponse =
        submitRemoteMessageAndHandleResponse(
            response, CollectionParams.CollectionAction.DELETEBACKUP, remoteMessage, asyncId);
    response.deleted = fromRemoteResponse(objectMapper, remoteResponse);
    response.collection = (String) remoteResponse.getResponse().get(COLLECTION);
    return response;
  }

  @Path("/backups/{backupName}/versions")
  @DELETE
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public BackupDeletionResponseBody deleteMultipleBackupsByRecency(
      @PathParam("backupName") String backupName,
      @QueryParam("retainLatest") Integer versionsToRetain,
      // Optional parameters below
      @QueryParam(BACKUP_LOCATION) String location,
      @QueryParam(BACKUP_REPOSITORY) String repositoryName,
      @QueryParam(ASYNC) String asyncId)
      throws Exception {
    final var response = instantiateJerseyResponse(BackupDeletionResponseBody.class);
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    ensureRequiredParameterProvided(NAME, backupName);
    ensureRequiredParameterProvided("retainLatest", versionsToRetain);
    location = getAndValidateBackupLocation(repositoryName, location);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(
            backupName, null, versionsToRetain, null, location, repositoryName, asyncId);
    final var remoteResponse =
        submitRemoteMessageAndHandleResponse(
            response, CollectionParams.CollectionAction.DELETEBACKUP, remoteMessage, asyncId);
    response.deleted = fromRemoteResponse(objectMapper, remoteResponse);
    response.collection = (String) remoteResponse.getResponse().get(COLLECTION);
    return response;
  }

  @Path("/backups/{backupName}/purgeUnused")
  @PUT
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public PurgeUnusedResponse garbageCollectUnusedBackupFiles(
      @PathParam("backupName") String backupName, PurgeUnusedFilesRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(PurgeUnusedResponse.class);
    recordCollectionForLogAndTracing(null, solrQueryRequest);

    if (requestBody == null) {
      throw new SolrException(BAD_REQUEST, "Required request body is missing");
    }
    ensureRequiredParameterProvided(NAME, backupName);
    requestBody.location =
        getAndValidateBackupLocation(requestBody.repositoryName, requestBody.location);

    final ZkNodeProps remoteMessage =
        createRemoteMessage(
            backupName,
            null,
            null,
            Boolean.TRUE,
            requestBody.location,
            requestBody.repositoryName,
            requestBody.asyncId);
    final var remoteResponse =
        submitRemoteMessageAndHandleResponse(
            response,
            CollectionParams.CollectionAction.DELETEBACKUP,
            remoteMessage,
            requestBody.asyncId);

    final Object remoteDeleted = remoteResponse.getResponse().get("deleted");
    if (remoteDeleted != null) {
      response.deleted = objectMapper.convertValue(remoteDeleted, PurgeUnusedStats.class);
    }
    return response;
  }

  public static class PurgeUnusedResponse extends SubResponseAccumulatingJerseyResponse {
    @JsonProperty public PurgeUnusedStats deleted;
  }

  public static class PurgeUnusedStats implements JacksonReflectMapWriter {
    @JsonProperty public Integer numBackupIds;
    @JsonProperty public Integer numShardBackupIds;
    @JsonProperty public Integer numIndexFiles;
  }

  public static ZkNodeProps createRemoteMessage(
      String backupName,
      String backupId,
      Integer versionsToRetain,
      Boolean purgeUnused,
      String location,
      String repositoryName,
      String asyncId) {
    final Map<String, Object> remoteMessage = new HashMap<>();

    // Always provided
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.DELETEBACKUP.toLower());
    remoteMessage.put(NAME, backupName);
    // Mutually exclusive
    assert backupId != null || versionsToRetain != null || purgeUnused != null;
    insertIfNotNull(remoteMessage, BACKUP_ID, backupId);
    insertIfNotNull(remoteMessage, MAX_NUM_BACKUP_POINTS, versionsToRetain);
    insertIfNotNull(remoteMessage, BACKUP_PURGE_UNUSED, purgeUnused);
    // Remaining params are truly optional
    insertIfNotNull(remoteMessage, BACKUP_LOCATION, location);
    insertIfNotNull(remoteMessage, BACKUP_REPOSITORY, repositoryName);
    insertIfNotNull(remoteMessage, ASYNC, asyncId);

    return new ZkNodeProps(remoteMessage);
  }

  public static void invokeFromV1Params(
      CoreContainer coreContainer, SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    int deletionModesProvided = 0;
    if (req.getParams().get(MAX_NUM_BACKUP_POINTS) != null) deletionModesProvided++;
    if (req.getParams().get(BACKUP_PURGE_UNUSED) != null) deletionModesProvided++;
    if (req.getParams().get(BACKUP_ID) != null) deletionModesProvided++;
    if (deletionModesProvided != 1) {
      throw new SolrException(
          BAD_REQUEST,
          String.format(
              Locale.ROOT,
              "Exactly one of %s, %s, and %s parameters must be provided",
              MAX_NUM_BACKUP_POINTS,
              BACKUP_PURGE_UNUSED,
              BACKUP_ID));
    }

    final var deleteApi = new DeleteCollectionBackupAPI(coreContainer, req, rsp);
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, invokeApi(deleteApi, req.getParams()));
  }

  public static class BackupDeletionResponseBody extends SubResponseAccumulatingJerseyResponse {
    @JsonProperty public String collection;
    @JsonProperty public List<CreateCollectionBackupAPI.BackupDeletionData> deleted;
  }

  @SuppressWarnings("unchecked")
  public static List<CreateCollectionBackupAPI.BackupDeletionData> fromRemoteResponse(
      ObjectMapper objectMapper, SolrResponse response) {
    final var deleted = (List<SimpleOrderedMap<Object>>) response.getResponse().get("deleted");
    if (deleted == null) {
      return null;
    }

    final List<CreateCollectionBackupAPI.BackupDeletionData> statList = new ArrayList<>();
    for (SimpleOrderedMap<Object> remoteStat : deleted) {
      statList.add(
          objectMapper.convertValue(
              remoteStat, CreateCollectionBackupAPI.BackupDeletionData.class));
    }
    return statList;
  }

  /**
   * Request body for the {@link DeleteCollectionBackupAPI#garbageCollectUnusedBackupFiles(String,
   * PurgeUnusedFilesRequestBody)} API.
   */
  public static class PurgeUnusedFilesRequestBody implements JacksonReflectMapWriter {
    @JsonProperty(BACKUP_LOCATION)
    public String location;

    @JsonProperty(BACKUP_REPOSITORY)
    public String repositoryName;

    @JsonProperty(ASYNC)
    public String asyncId;
  }

  private static SolrJerseyResponse invokeApi(DeleteCollectionBackupAPI api, SolrParams params)
      throws Exception {
    if (params.get(MAX_NUM_BACKUP_POINTS) != null) {
      return api.deleteMultipleBackupsByRecency(
          params.get(NAME),
          params.getInt(MAX_NUM_BACKUP_POINTS),
          params.get(BACKUP_LOCATION),
          params.get(BACKUP_REPOSITORY),
          params.get(ASYNC));
    } else if (params.get(BACKUP_PURGE_UNUSED) != null) {
      final var requestBody = new PurgeUnusedFilesRequestBody();
      requestBody.location = params.get(BACKUP_LOCATION);
      requestBody.repositoryName = params.get(BACKUP_REPOSITORY);
      requestBody.asyncId = params.get(ASYNC);
      return api.garbageCollectUnusedBackupFiles(params.get(NAME), requestBody);
    } else { // BACKUP_ID != null
      return api.deleteSingleBackupById(
          params.get(NAME),
          params.get(BACKUP_ID),
          params.get(BACKUP_LOCATION),
          params.get(BACKUP_REPOSITORY),
          params.get(ASYNC));
    }
  }
}
