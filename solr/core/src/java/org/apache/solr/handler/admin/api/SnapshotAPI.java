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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.Parameter;
import org.apache.lucene.index.IndexCommit;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.IndexDeletionPolicyWrapper;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.snapshots.SolrSnapshotManager;
import org.apache.solr.core.snapshots.SolrSnapshotMetaDataManager;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.solr.client.solrj.impl.BinaryResponseParser.BINARY_CONTENT_TYPE_V2;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

@Path("/cores/{coreName}/snapshots")
public class SnapshotAPI extends JerseyResource {

  private final CoreContainer coreContainer;

  @Inject
  public SnapshotAPI(CoreContainer coreContainer) {
    this.coreContainer = coreContainer;
  }

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
          String snapshotName)
      throws Exception {
    final CreateSnapshotResponse response = instantiateJerseyResponse(CreateSnapshotResponse.class);

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
      } finally {
        delPol.releaseCommitPoint(ic);
      }
    }

    return response;
  }

  public static class CreateSnapshotResponse extends SolrJerseyResponse {
    @JsonProperty(CoreAdminParams.CORE)
    public String core;

    @JsonProperty(CoreAdminParams.SNAPSHOT_NAME)
    public String commitName;

    @JsonProperty(SolrSnapshotManager.INDEX_DIR_PATH)
    public String indexDirPath;

    @JsonProperty(SolrSnapshotManager.GENERATION_NUM)
    public long generation;

    @JsonProperty(SolrSnapshotManager.FILE_LIST)
    public Collection<String> files;
  }

  @GET
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_READ_PERM)
  public ListSnapshotsResponse listSnapshots(
      @Parameter(
              description = "The name of the core for which to retrieve snapshots.",
              required = true)
          @PathParam("coreName")
          String coreName) {
    final ListSnapshotsResponse response = instantiateJerseyResponse(ListSnapshotsResponse.class);

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
  }

  public static class ListSnapshotsResponse extends SolrJerseyResponse {
    @JsonProperty(SolrSnapshotManager.SNAPSHOTS_INFO)
    public Map<String, SnapshotInformation> snapshots;
  }

  public static class SnapshotInformation {
    @JsonProperty(SolrSnapshotManager.GENERATION_NUM)
    public final long generationNumber;

    @JsonProperty(SolrSnapshotManager.INDEX_DIR_PATH)
    public final String indexDirPath;

    public SnapshotInformation(long generationNumber, String indexDirPath) {
      this.generationNumber = generationNumber;
      this.indexDirPath = indexDirPath;
    }
  }

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
          String snapshotName)
      throws Exception {
    final DeleteSnapshotResponse response = instantiateJerseyResponse(DeleteSnapshotResponse.class);

    final SolrCore core = coreContainer.getCore(coreName);
    if (core == null) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "Unable to locate core " + coreName);
    }

    try {
      core.deleteNamedSnapshot(snapshotName);
      // Ideally we shouldn't need this. This is added since the RPC logic in
      // OverseerCollectionMessageHandler can not provide the coreName as part of the result.
      response.coreName = coreName;
      response.commitName = snapshotName;
    } finally {
      core.close();
    }

    return response;
  }

  public static class DeleteSnapshotResponse extends SolrJerseyResponse {
    @JsonProperty(CoreAdminParams.CORE)
    public String coreName;

    @JsonProperty(CoreAdminParams.SNAPSHOT_NAME)
    public String commitName;
  }
}
