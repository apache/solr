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
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.client.api.endpoint.CoreSnapshotApi;
import org.apache.solr.client.api.model.CreateCoreSnapshotResponse;
import org.apache.solr.client.api.model.DeleteSnapshotResponse;
import org.apache.solr.client.api.model.ListCoreSnapshotsResponse;
import org.apache.solr.client.api.model.SnapshotInformation;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API for Creating, Listing, and Deleting Core Snapshots. */
public class CoreSnapshot extends CoreAdminAPIBase implements CoreSnapshotApi {

  @Inject
  public CoreSnapshot(
      SolrQueryRequest request,
      SolrQueryResponse response,
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker) {
    super(coreContainer, coreAdminAsyncTracker, request, response);
  }

  /** This API is analogous to V1 (POST /solr/admin/cores?action=CREATESNAPSHOT) */
  @Override
  @PermissionName(CORE_EDIT_PERM)
  public CreateCoreSnapshotResponse createSnapshot(
      String coreName, String snapshotName, String taskId) throws Exception {
    final CreateCoreSnapshotResponse response =
        instantiateJerseyResponse(CreateCoreSnapshotResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        taskId,
        "createSnapshot",
        () -> {
          try (SolrCore core = coreContainer.getCore(coreName)) {
            if (core == null) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
            }

            final String indexDirPath = core.getIndexDir();
            final IndexDeletionPolicyWrapper delPol = core.getDeletionPolicy();
            final IndexCommit ic = delPol.getAndSaveLatestCommit();
            try {
              if (null == ic) {
                throw new SolrException(
                    SolrException.ErrorCode.BAD_REQUEST,
                    "No index commits to snapshot in core " + coreName);
              }
              final SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();
              mgr.snapshot(snapshotName, indexDirPath, ic.getGeneration());

              response.core = core.getName();
              response.commitName = snapshotName;
              response.indexDirPath = indexDirPath;
              response.generation = ic.getGeneration();
              response.files = ic.getFileNames();
            } catch (IOException e) {
              throw new CoreAdminAPIBaseException(e);
            } finally {
              delPol.releaseCommitPoint(ic);
            }
          }

          return response;
        });
  }

  /** This API is analogous to V1 (GET /solr/admin/cores?action=LISTSNAPSHOTS) */
  @Override
  @PermissionName(CORE_READ_PERM)
  public ListCoreSnapshotsResponse listSnapshots(String coreName) throws Exception {
    final ListCoreSnapshotsResponse response =
        instantiateJerseyResponse(ListCoreSnapshotsResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        null, // 'list' operations are never asynchronous
        "listSnapshots",
        () -> {
          try (SolrCore core = coreContainer.getCore(coreName)) {
            if (core == null) {
              throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
            }

            SolrSnapshotMetaDataManager mgr = core.getSnapshotMetaDataManager();

            final Map<String, SnapshotInformation> result = new HashMap<>();
            for (String name : mgr.listSnapshots()) {
              Optional<SolrSnapshotMetaDataManager.SnapshotMetaData> metadata =
                  mgr.getSnapshotMetaData(name);
              if (metadata.isPresent()) {
                final SnapshotInformation snapshotInformation =
                    new SnapshotInformation(
                        metadata.get().getGenerationNumber(), metadata.get().getIndexDirPath());
                result.put(name, snapshotInformation);
              }
            }

            response.snapshots = result;
          }

          return response;
        });
  }

  /** This API is analogous to V1 (DELETE /solr/admin/cores?action=DELETESNAPSHOT) */
  @Override
  @PermissionName(CORE_EDIT_PERM)
  public DeleteSnapshotResponse deleteSnapshot(String coreName, String snapshotName, String taskId)
      throws Exception {
    final DeleteSnapshotResponse response = instantiateJerseyResponse(DeleteSnapshotResponse.class);

    return handlePotentiallyAsynchronousTask(
        response,
        coreName,
        taskId,
        "deleteSnapshot",
        () -> {
          final SolrCore core = coreContainer.getCore(coreName);
          if (core == null) {
            throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
          }

          try {
            try {
              core.deleteNamedSnapshot(snapshotName);
            } catch (IOException e) {
              throw new CoreAdminAPIBaseException(e);
            }

            // Ideally we shouldn't need this. This is added since the RPC logic in
            // OverseerCollectionMessageHandler can not provide the coreName as part of the result.
            response.coreName = coreName;
            response.commitName = snapshotName;
          } finally {
            core.close();
          }

          return response;
        });
  }
}
