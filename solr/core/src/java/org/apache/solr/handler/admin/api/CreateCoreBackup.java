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

import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import java.net.URI;
import java.nio.file.Paths;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.solr.client.api.endpoint.CreateCoreBackupApi;
import org.apache.solr.client.api.model.CreateCoreBackupRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.IncrementalShardBackup;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

public class CreateCoreBackup extends CoreAdminAPIBase implements CreateCoreBackupApi {
  @Inject
  public CreateCoreBackup(
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
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createBackup(
      String coreName, CreateCoreBackupRequestBody backupCoreRequestBody) throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    return handlePotentiallyAsynchronousTask(
        null,
        coreName,
        backupCoreRequestBody.async,
        "backup",
        () -> {
          try (BackupRepository repository =
                  coreContainer.newBackupRepository(backupCoreRequestBody.repository);
              SolrCore core = coreContainer.getCore(coreName)) {
            String location = repository.getBackupLocation(backupCoreRequestBody.location);
            if (location == null) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST,
                  "'location' parameter is not specified in the request body or as a default repository property");
            }
            URI locationUri = repository.createDirectoryURI(location);
            repository.createDirectory(locationUri);

            if (Boolean.TRUE.equals(backupCoreRequestBody.incremental)) {
              if ("file".equals(locationUri.getScheme())) {
                core.getCoreContainer().assertPathAllowed(Paths.get(locationUri));
              }
              ensureRequiredParameterProvided("shardBackupId", backupCoreRequestBody.shardBackupId);
              final ShardBackupId shardBackupId =
                  ShardBackupId.from(backupCoreRequestBody.shardBackupId);
              final ShardBackupId prevShardBackupId =
                  backupCoreRequestBody.prevShardBackupId != null
                      ? ShardBackupId.from(backupCoreRequestBody.prevShardBackupId)
                      : null;
              BackupFilePaths incBackupFiles = new BackupFilePaths(repository, locationUri);
              IncrementalShardBackup incSnapShooter =
                  new IncrementalShardBackup(
                      repository,
                      core,
                      incBackupFiles,
                      prevShardBackupId,
                      shardBackupId,
                      Optional.ofNullable(backupCoreRequestBody.commitName));
              return incSnapShooter.backup();
            } else {
              SnapShooter snapShooter =
                  new SnapShooter(
                      repository,
                      core,
                      locationUri,
                      backupCoreRequestBody.backupName,
                      backupCoreRequestBody.commitName);
              // validateCreateSnapshot will create parent dirs instead of throw; that choice is
              // dubious.
              // But we want to throw. One reason is that this dir really should, in fact must,
              // already
              // exist here if triggered via a collection backup on a shared file system. Otherwise,
              // perhaps the FS location isn't shared -- we want an error.
              if (!snapShooter.getBackupRepository().exists(snapShooter.getLocation())) {
                throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "Directory to contain snapshots doesn't exist: "
                        + snapShooter.getLocation()
                        + ". "
                        + "Note that Backup/Restore of a SolrCloud collection "
                        + "requires a shared file system mounted at the same path on all nodes!");
              }
              snapShooter.validateCreateSnapshot();
              return snapShooter.createSnapshot();
            }
          } catch (Exception exp) {
            throw new SolrException(
                SolrException.ErrorCode.SERVER_ERROR,
                "Failed to backup core=" + coreName + " because " + exp,
                exp);
          }
        });
  }
}
