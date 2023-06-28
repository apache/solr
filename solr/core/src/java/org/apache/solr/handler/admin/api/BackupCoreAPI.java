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
import static org.apache.solr.security.PermissionNameProvider.Name.COLL_EDIT_PERM;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Path;
import org.apache.solr.common.SolrException;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.backup.BackupFilePaths;
import org.apache.solr.core.backup.ShardBackupId;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.handler.IncrementalShardBackup;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

@Path(("/cores/{coreName}/backup/{name}"))
public class BackupCoreAPI extends CoreAdminAPIBase {
  @Inject
  public BackupCoreAPI(
          CoreContainer coreContainer,
          SolrQueryRequest solrQueryRequest,
          SolrQueryResponse solrQueryResponse,
          CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker) {
    super(coreContainer, coreAdminAsyncTracker, solrQueryRequest, solrQueryResponse);
  }

  @POST
  @Produces({"application/json", "application/xml", BINARY_CONTENT_TYPE_V2})
  @PermissionName(COLL_EDIT_PERM)
  public SolrJerseyResponse createBackup(
          @Schema(description = "The name of the core.") @PathParam("core")  String coreName,
          @Schema(description = "The backup will be created in a directory called snapshot.<name>")
          @PathParam("name")
          String name,
          @Schema(description = "The POJO for representing additional backup params.") @RequestBody
          BackupCoreRequestBody backupCoreRequestBody,
          @Parameter(description = "The id to associate with the async task.") @QueryParam("async")
          String taskId)
          throws Exception {
    if(coreName == null)
      throw new SolrException(
              SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: " + CoreAdminParams.CORE);
    return handlePotentiallyAsynchronousTask
            (null
                    ,coreName
                    ,taskId
                    ,"backup"
                    , () -> {
    try (BackupRepository repository =
                 coreContainer.newBackupRepository(backupCoreRequestBody.repository);
         SolrCore core = coreContainer.getCore(coreName)) {
      String location = repository.getBackupLocation(backupCoreRequestBody.location);
      if (location == null) {
        throw new SolrException(
                SolrException.ErrorCode.BAD_REQUEST,
                "'location' is not specified as a query"
                        + " parameter or as a default repository property");
      }
      URI locationUri = repository.createDirectoryURI(location);
      repository.createDirectory(locationUri);

      if(backupCoreRequestBody.incremental){
        if ("file".equals(locationUri.getScheme())) {
          core.getCoreContainer().assertPathAllowed(Paths.get(locationUri));
        }
        if (backupCoreRequestBody.shardBackupId == null) {
          throw new SolrException(
                  SolrException.ErrorCode.BAD_REQUEST, "Missing required parameter: shardBackupId");
        }
        final ShardBackupId shardBackupId = ShardBackupId.from(backupCoreRequestBody.shardBackupId);
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
        NamedList<Object> rsp = incSnapShooter.backup();
        return IncrementalBackupCoreResponse.getObjectFromNamedList(rsp);

      }else {
        SnapShooterBackupCoreResponse snapShooterBackupCoreResponse = new SnapShooterBackupCoreResponse();
        SnapShooter snapShooter =
                new SnapShooter(repository, core, locationUri, name, backupCoreRequestBody.commitName);
        // validateCreateSnapshot will create parent dirs instead of throw; that choice is dubious.
        // But we want to throw. One reason is that this dir really should, in fact must, already
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
        NamedList<Object> rsp = snapShooter.createSnapshot();
        return SnapShooterBackupCoreResponse.getObjectFromNamedList(rsp);
      }
    }catch(Exception exp){
      throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Failed to backup core=" + coreName + " because " + exp,
              exp);
    }});
  }
  public static class BackupCoreRequestBody extends SolrJerseyResponse {

    @Schema(description = "The name of the repository to be used for backup.")
    @JsonProperty("repository")
    public String repository;

    @Schema(description = "The path where the backup will be created")
    @JsonProperty("location")
    public String location;

    @JsonProperty("shardBackupId")
    public String shardBackupId;

    @JsonProperty("prevShardBackupId")
    public String prevShardBackupId;

    @Schema(
            description =
                    "The name of the commit which was used while taking a snapshot using the CREATESNAPSHOT command.")
    @JsonProperty("commitName")
    public String commitName;

    @Schema(
            description = "To turn on incremental backup feature"
    )
    @JsonProperty("incremental")
    public boolean incremental;
  }
  public static class IncrementalBackupCoreResponse extends SolrJerseyResponse {
    //public NamedList<Object> response;
    @Schema(description = "The time at which backup snapshot started at.")
    @JsonProperty("startTime")
    public String startTime;

    @Schema(description = "The count of index files in the snapshot.")
    @JsonProperty("indexFileCount")
    public int indexFileCount;

    @Schema(description = "The count of uploaded index files.")
    @JsonProperty("uploadedIndexFileCount")
    public int uploadedIndexFileCount;

    @Schema(description = "The size of index in MB.")
    @JsonProperty("indexSizeMB")
    public double indexSizeMB;

    @Schema(description = "The size of uploaded index in MB.")
    @JsonProperty("uploadedIndexFileMB")
    public double uploadedIndexFileMB;

    @Schema(description = "Shard Id.")
    @JsonProperty("shard")
    public String shard;

    @Schema(description = "The time at which backup snapshot completed at.")
    @JsonProperty("endTime")
    public String endTime;

    @Schema(description = "ShardId of shard to which core belongs to.")
    @JsonProperty("shardBackupId")
    public String shardBackupId;

    public static IncrementalBackupCoreResponse getObjectFromNamedList(NamedList<Object> nl){
      IncrementalBackupCoreResponse incrementalBackupCoreResponse = new IncrementalBackupCoreResponse();
      incrementalBackupCoreResponse.endTime = nl.get("endTime").toString();
      incrementalBackupCoreResponse.indexSizeMB = Double.parseDouble(nl.get("indexSizeMB").toString());
      incrementalBackupCoreResponse.indexFileCount = Integer.parseInt(nl.get("indexFileCount").toString());
      incrementalBackupCoreResponse.startTime = nl.get("startTime").toString();
      incrementalBackupCoreResponse.shardBackupId = nl.get("shardBackupId").toString();
      incrementalBackupCoreResponse.uploadedIndexFileCount =
              Integer.parseInt(nl.get("uploadedIndexFileCount").toString());
      incrementalBackupCoreResponse.uploadedIndexFileMB =
              Double.parseDouble(nl.get("uploadedIndexFileMB").toString());
      if (nl.get("shard") != null) incrementalBackupCoreResponse.shard = nl.get("shard").toString();
      return incrementalBackupCoreResponse;
    }
  }

  public static class SnapShooterBackupCoreResponse extends SolrJerseyResponse {

    @Schema(description = "The time at which snapshot started at.")
    @JsonProperty("startTime")
    public String startTime;
    @Schema(description = "The number of files in the snapshot.")
    @JsonProperty("fileCount")
    public int fileCount;

    @Schema(description = "The number of index files in the snapshot.")
    @JsonProperty("indexFileCount")
    public int indexFileCount;

    @Schema(description = "The status of the snapshot")
    @JsonProperty("status")
    public String status;

    @Schema(description = "The time at which snapshot completed at.")
    @JsonProperty("snapshotCompletedAt")
    public String snapshotCompletedAt;

    @Schema(description = "The time at which snapshot completed at.")
    @JsonProperty("endTime")
    public String endTime;

    @Schema(description = "The name of the snapshot")
    @JsonProperty("snapshotName")
    public String snapshotName;

    @Schema(description = "The name of the directory where snapshot created.")
    @JsonProperty("directoryName")
    public String directoryName;

    public static SnapShooterBackupCoreResponse getObjectFromNamedList(NamedList<Object> nl){
      SnapShooterBackupCoreResponse snapShooterBackupCoreResponse = new SnapShooterBackupCoreResponse();
      snapShooterBackupCoreResponse.directoryName = nl.get("directoryName").toString();
      snapShooterBackupCoreResponse.endTime = nl.get("endTime").toString();
      snapShooterBackupCoreResponse.indexFileCount = Integer.parseInt(nl.get("indexFileCount").toString());
      snapShooterBackupCoreResponse.snapshotName = nl.get("snapshotName").toString();
      snapShooterBackupCoreResponse.status = nl.get("status").toString();
      snapShooterBackupCoreResponse.startTime = nl.get("startTime").toString();
      snapShooterBackupCoreResponse.fileCount = Integer.parseInt(nl.get("fileCount").toString());
      snapShooterBackupCoreResponse.snapshotCompletedAt = nl.get("snapshotCompletedAt").toString();
      return snapShooterBackupCoreResponse;
    }
  }
}