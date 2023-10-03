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
import static org.apache.solr.common.cloud.ZkStateReader.COLLECTION_PROP;
import static org.apache.solr.common.params.CollectionAdminParams.FOLLOW_ALIASES;
import static org.apache.solr.common.params.CollectionAdminParams.INDEX_BACKUP_STRATEGY;
import static org.apache.solr.common.params.CollectionAdminParams.PROPERTY_PREFIX;
import static org.apache.solr.common.params.CommonAdminParams.ASYNC;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_CONFIGSET;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_INCREMENTAL;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_LOCATION;
import static org.apache.solr.common.params.CoreAdminParams.BACKUP_REPOSITORY;
import static org.apache.solr.common.params.CoreAdminParams.COMMIT_NAME;
import static org.apache.solr.common.params.CoreAdminParams.MAX_NUM_BACKUP_POINTS;
import static org.apache.solr.handler.admin.CollectionsHandler.DEFAULT_COLLECTION_OP_TIMEOUT;
import static org.apache.solr.handler.admin.api.CreateCollectionAPI.copyPrefixedPropertiesWithoutPrefix;
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import org.apache.solr.client.api.model.BackupDeletionData;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SubResponseAccumulatingJerseyResponse;
import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJacksonMapper;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.zookeeper.common.StringUtils;

/**
 * V2 API for creating a new "backup" of a specified collection
 *
 * <p>This API is analogous to the v1 /admin/collections?action=BACKUP command.
 */
@Path("/collections/{collectionName}/backups/{backupName}/versions")
public class CreateCollectionBackupAPI extends BackupAPIBase {
  private final ObjectMapper objectMapper;

  @Inject
  public CreateCollectionBackupAPI(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);

    this.objectMapper = SolrJacksonMapper.getObjectMapper();
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createCollectionBackup(
      @PathParam("collectionName") String collectionName,
      @PathParam("backupName") String backupName,
      CreateCollectionBackupRequestBody requestBody)
      throws Exception {
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    if (StringUtils.isBlank(backupName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: 'backupName'");
    }
    if (collectionName == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: 'collection'");
    }
    final CoreContainer coreContainer = fetchAndValidateZooKeeperAwareCoreContainer();
    recordCollectionForLogAndTracing(collectionName, solrQueryRequest);

    collectionName =
        resolveAndValidateAliasIfEnabled(
            collectionName, Boolean.TRUE.equals(requestBody.followAliases));

    requestBody.location =
        getAndValidateBackupLocation(requestBody.repository, requestBody.location);

    if (requestBody.incremental == null) {
      requestBody.incremental = Boolean.TRUE;
    }
    if (requestBody.backupStrategy == null) {
      requestBody.backupStrategy = CollectionAdminParams.COPY_FILES_STRATEGY;
    }
    if (!CollectionAdminParams.INDEX_BACKUP_STRATEGIES.contains(requestBody.backupStrategy)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Unknown index backup strategy " + requestBody.backupStrategy);
    }

    final ZkNodeProps remoteMessage = createRemoteMessage(collectionName, backupName, requestBody);
    final SolrResponse remoteResponse =
        CollectionsHandler.submitCollectionApiCommand(
            coreContainer,
            coreContainer.getDistributedCollectionCommandRunner(),
            remoteMessage,
            CollectionParams.CollectionAction.BACKUP,
            DEFAULT_COLLECTION_OP_TIMEOUT);
    if (remoteResponse.getException() != null) {
      throw remoteResponse.getException();
    }

    final SolrJerseyResponse response =
        objectMapper.convertValue(
            remoteResponse.getResponse(), CreateCollectionBackupResponseBody.class);

    return response;
  }

  public static ZkNodeProps createRemoteMessage(
      String collectionName, String backupName, CreateCollectionBackupRequestBody requestBody) {
    final Map<String, Object> remoteMessage = requestBody.toMap(new HashMap<>());
    remoteMessage.put(QUEUE_OPERATION, CollectionParams.CollectionAction.BACKUP.toLower());
    remoteMessage.put(COLLECTION_PROP, collectionName);
    remoteMessage.put(NAME, backupName);
    if (!StringUtils.isBlank(requestBody.backupStrategy)) {
      remoteMessage.put(INDEX_BACKUP_STRATEGY, remoteMessage.remove("backupStrategy"));
    }
    if (!StringUtils.isBlank(requestBody.snapshotName)) {
      remoteMessage.put(COMMIT_NAME, remoteMessage.remove("snapshotName"));
    }
    return new ZkNodeProps(remoteMessage);
  }

  public static CreateCollectionBackupRequestBody createRequestBodyFromV1Params(SolrParams params) {
    var requestBody = new CreateCollectionBackupAPI.CreateCollectionBackupRequestBody();

    requestBody.location = params.get(BACKUP_LOCATION);
    requestBody.repository = params.get(BACKUP_REPOSITORY);
    requestBody.followAliases = params.getBool(FOLLOW_ALIASES);
    requestBody.backupStrategy = params.get(INDEX_BACKUP_STRATEGY);
    requestBody.snapshotName = params.get(COMMIT_NAME);
    requestBody.incremental = params.getBool(BACKUP_INCREMENTAL);
    requestBody.backupConfigset = params.getBool(BACKUP_CONFIGSET);
    requestBody.maxNumBackupPoints = params.getInt(MAX_NUM_BACKUP_POINTS);
    requestBody.extraProperties =
        copyPrefixedPropertiesWithoutPrefix(params, new HashMap<>(), PROPERTY_PREFIX);

    requestBody.async = params.get(ASYNC);

    return requestBody;
  }

  public static SolrJerseyResponse invokeFromV1Params(
      SolrQueryRequest req, SolrQueryResponse rsp, CoreContainer coreContainer) throws Exception {
    req.getParams().required().check(NAME, COLLECTION_PROP);
    final var collectionName = req.getParams().get(COLLECTION_PROP);
    final var backupName = req.getParams().get(NAME);
    final var requestBody = createRequestBodyFromV1Params(req.getParams());

    final var createBackupApi = new CreateCollectionBackupAPI(coreContainer, req, rsp);
    return createBackupApi.createCollectionBackup(collectionName, backupName, requestBody);
  }

  public static class CreateCollectionBackupRequestBody implements JacksonReflectMapWriter {
    @JsonProperty public String location;
    @JsonProperty public String repository;
    @JsonProperty public Boolean followAliases;
    @JsonProperty public String backupStrategy;
    @JsonProperty public String snapshotName;
    @JsonProperty public Boolean incremental;
    @JsonProperty public Boolean backupConfigset;
    @JsonProperty public Integer maxNumBackupPoints;
    @JsonProperty public String async;
    @JsonProperty public Map<String, String> extraProperties;
  }

  public static class CreateCollectionBackupResponseBody
      extends SubResponseAccumulatingJerseyResponse {
    @JsonProperty("response")
    public CollectionBackupDetails backupDataResponse;

    @JsonProperty("deleted")
    public List<BackupDeletionData> deleted;

    @JsonProperty public String collection;
  }

  public static class CollectionBackupDetails implements JacksonReflectMapWriter {
    @JsonProperty public String collection;
    @JsonProperty public Integer numShards;
    @JsonProperty public Integer backupId;
    @JsonProperty public String indexVersion;
    @JsonProperty public String startTime;
    @JsonProperty public String endTime;
    @JsonProperty public Integer indexFileCount;
    @JsonProperty public Integer uploadedIndexFileCount;
    @JsonProperty public Double indexSizeMB;
    @JsonProperty public Map<String, String> extraProperties;

    @JsonProperty("uploadedIndexFileMB")
    public Double uploadedIndexSizeMB;

    @JsonProperty public List<String> shardBackupIds;
  }
}
