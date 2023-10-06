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

import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import java.net.URI;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.RestoreCoreApi;
import org.apache.solr.client.api.model.RestoreCoreRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.UpdateLog;

/**
 * V2 API implementation for restoring a previously taken backup to a core
 *
 * <p>Only valid in SolrCloud mode. This API (POST /api/cores/coreName/restore {}) is analogous to
 * the v1 GET /solr/admin/cores?action=RESTORECORE command.
 */
public class RestoreCore extends CoreAdminAPIBase implements RestoreCoreApi {

  @Inject
  public RestoreCore(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @Override
  boolean isExpensive() {
    return true;
  }

  @Override
  @PermissionName(CORE_EDIT_PERM)
  public SolrJerseyResponse restoreCore(String coreName, RestoreCoreRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    ensureRequiredParameterProvided("coreName", coreName);
    if (requestBody == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Missing required request body");
    }
    validateRequestBody(requestBody);
    AdminAPIBase.validateZooKeeperAwareCoreContainer(coreContainer);
    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        requestBody.async,
        "restoreCore",
        () -> {
          try {
            doRestore(coreName, requestBody);
            return response;
          } catch (Exception e) {
            throw new CoreAdminAPIBaseException(e);
          }
        });
  }

  private void doRestore(String coreName, RestoreCoreRequestBody requestBody) throws Exception {
    try (BackupRepository repository =
            coreContainer.newBackupRepository(requestBody.backupRepository);
        SolrCore core = coreContainer.getCore(coreName)) {

      String location = repository.getBackupLocation(requestBody.location);
      if (location == null) {
        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "'location' is not specified as a query"
                + " parameter or as a default repository property");
      }

      URI locationUri = repository.createDirectoryURI(location);
      CloudDescriptor cd = core.getCoreDescriptor().getCloudDescriptor();
      // this core must be the only replica in its shard otherwise
      // we cannot guarantee consistency between replicas because when we add data (or restore
      // index) to this replica
      Slice slice =
          coreContainer
              .getZkController()
              .getClusterState()
              .getCollection(cd.getCollectionName())
              .getSlice(cd.getShardId());
      if (slice.getReplicas().size() != 1 && !core.readOnly) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Failed to restore core="
                + core.getName()
                + ", the core must be the only replica in its shard or it must be read only");
      }

      org.apache.solr.handler.RestoreCore restoreCore;
      if (requestBody.shardBackupId != null) {
        final ShardBackupId shardBackupId = ShardBackupId.from(requestBody.shardBackupId);
        restoreCore =
            org.apache.solr.handler.RestoreCore.createWithMetaFile(
                repository, core, locationUri, shardBackupId);
      } else {
        restoreCore =
            org.apache.solr.handler.RestoreCore.create(
                repository, core, locationUri, requestBody.name);
      }
      boolean success = restoreCore.doRestore();
      if (!success) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Failed to restore core=" + core.getName());
      }
      // other replicas to-be-created will know that they are out of date by
      // looking at their term : 0 compare to term of this core : 1
      coreContainer
          .getZkController()
          .getShardTerms(cd.getCollectionName(), cd.getShardId())
          .ensureHighestTermsAreNotZero();

      // transitions state of update log to ACTIVE
      UpdateLog updateLog = core.getUpdateHandler().getUpdateLog();
      if (updateLog != null) {
        updateLog.applyBufferedUpdates();
      }
    }
  }

  public static void validateRequestBody(RestoreCoreRequestBody requestBody) {
    if (requestBody.shardBackupId == null && requestBody.name == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Either 'name' or 'shardBackupId' must be specified");
    }
  }
}
