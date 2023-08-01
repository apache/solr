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
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API for Creating, Listing, and Deleting Core Snapshots. */
@Path("/cores/{coreName}/snapshots")
public class CoreSnapshotAPI extends CoreAdminAPIBase {

  @Inject
  public CoreSnapshotAPI(
      SolrQueryRequest request,
      SolrQueryResponse response,
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker) {
    super(coreContainer, coreAdminAsyncTracker, request, response);
  }

  /** This API is analogous to V1 (POST /solr/admin/cores?action=CREATESNAPSHOT) */
  @POST
  @Path("/{snapshotName}")
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_EDIT_PERM)
  public CreateSnapshotResponse createSnapshot(
      @Parameter(description = "The name of the core to snapshot.", required = true)
          @PathParam("coreName")
          String coreName,
      @Parameter(description = "The name to associate with the core snapshot.", required = true)
          @PathParam("snapshotName")
          String snapshotName,
      @Parameter(description = "The id to associate with the async task.") @QueryParam("async")
          String taskId)
      throws Exception {
    final CreateSnapshotResponse response = instantiateJerseyResponse(CreateSnapshotResponse.class);

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

  /** The Response for {@link CoreSnapshotAPI}'s {@link #createSnapshot(String, String, String)} */
  public static class CreateSnapshotResponse extends SolrJerseyResponse {
    @Schema(description = "The name of the core.")
    @JsonProperty(CoreAdminParams.CORE)
    public String core;

    @Schema(description = "The name of the created snapshot.")
    @JsonProperty(CoreAdminParams.SNAPSHOT_NAME)
    public String commitName;

    @Schema(description = "The path to the directory containing the index files.")
    @JsonProperty(SolrSnapshotManager.INDEX_DIR_PATH)
    public String indexDirPath;

    @Schema(description = "The generation value for the created snapshot.")
    @JsonProperty(SolrSnapshotManager.GENERATION_NUM)
    public Long generation;

    @Schema(description = "The list of index filenames contained within the created snapshot.")
    @JsonProperty(SolrSnapshotManager.FILE_LIST)
    public Collection<String> files;
  }

  /** This API is analogous to V1 (GET /solr/admin/cores?action=LISTSNAPSHOTS) */
  @GET
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_READ_PERM)
  public ListSnapshotsResponse listSnapshots(
      @Parameter(
              description = "The name of the core for which to retrieve snapshots.",
              required = true)
          @PathParam("coreName")
          String coreName)
      throws Exception {
    final ListSnapshotsResponse response = instantiateJerseyResponse(ListSnapshotsResponse.class);

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

  /** The Response for {@link CoreSnapshotAPI}'s {@link #listSnapshots(String)} */
  public static class ListSnapshotsResponse extends SolrJerseyResponse {
    @Schema(description = "The collection of snapshots found for the requested core.")
    @JsonProperty(SolrSnapshotManager.SNAPSHOTS_INFO)
    public Map<String, SnapshotInformation> snapshots;
  }

  /**
   * Contained in {@link ListSnapshotsResponse}, this holds information for a given core's Snapshot
   */
  public static class SnapshotInformation implements JacksonReflectMapWriter {
    @Schema(description = "The generation value for the snapshot.")
    @JsonProperty(SolrSnapshotManager.GENERATION_NUM)
    public final long generationNumber;

    @Schema(description = "The path to the directory containing the index files.")
    @JsonProperty(SolrSnapshotManager.INDEX_DIR_PATH)
    public final String indexDirPath;

    public SnapshotInformation(long generationNumber, String indexDirPath) {
      this.generationNumber = generationNumber;
      this.indexDirPath = indexDirPath;
    }
  }

  /** This API is analogous to V1 (DELETE /solr/admin/cores?action=DELETESNAPSHOT) */
  @DELETE
  @Path("/{snapshotName}")
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_EDIT_PERM)
  public DeleteSnapshotResponse deleteSnapshot(
      @Parameter(
              description = "The name of the core for which to delete a snapshot.",
              required = true)
          @PathParam("coreName")
          String coreName,
      @Parameter(description = "The name of the core snapshot to delete.", required = true)
          @PathParam("snapshotName")
          String snapshotName,
      @Parameter(description = "The id to associate with the async task.") @QueryParam("async")
          String taskId)
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

  /** The Response for {@link CoreSnapshotAPI}'s {@link #deleteSnapshot(String, String, String)} */
  public static class DeleteSnapshotResponse extends SolrJerseyResponse {
    @Schema(description = "The name of the core.")
    @JsonProperty(CoreAdminParams.CORE)
    public String coreName;

    @Schema(description = "The name of the deleted snapshot.")
    @JsonProperty(CoreAdminParams.SNAPSHOT_NAME)
    public String commitName;
  }
}
