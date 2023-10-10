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
import static org.apache.solr.handler.ReplicationHandler.ERR_STATUS;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.ReplicationHandler.ReplicationHandlerConfig;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** V2 endpoint for Backup API used for User-Managed clusters and Single-Node Installation. */
@Path("/cores/{coreName}/replication/backups")
public class SnapshotBackupAPI extends JerseyResource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SolrCore solrCore;
  private final ReplicationHandlerConfig replicationHandlerConfig;

  @Inject
  public SnapshotBackupAPI(SolrCore solrCore, ReplicationHandlerConfig replicationHandlerConfig) {
    this.solrCore = solrCore;
    this.replicationHandlerConfig = replicationHandlerConfig;
  }

  /**
   * This API (POST /api/cores/coreName/replication/backups {...}) is analogous to the v1
   * /solr/coreName/replication?command=backup
   */
  @POST
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @Operation(summary = "Backup command using ReplicationHandler")
  @PermissionName(CORE_EDIT_PERM)
  public BackupReplicationResponse createBackup(
      @RequestBody BackupReplicationRequestBody backupReplicationPayload) throws Exception {
    ensureRequiredRequestBodyProvided(backupReplicationPayload);
    ReplicationHandler replicationHandler =
        (ReplicationHandler) solrCore.getRequestHandler(ReplicationHandler.PATH);
    return doBackup(replicationHandler, backupReplicationPayload);
  }

  private BackupReplicationResponse doBackup(
      ReplicationHandler replicationHandler,
      BackupReplicationRequestBody backupReplicationPayload) {
    BackupReplicationResponse response = instantiateJerseyResponse(BackupReplicationResponse.class);
    int numberToKeep = backupReplicationPayload.numberToKeep;
    int numberBackupsToKeep = replicationHandlerConfig.getNumberBackupsToKeep();
    String location = backupReplicationPayload.location;
    String repoName = backupReplicationPayload.repository;
    String commitName = backupReplicationPayload.commitName;
    String name = backupReplicationPayload.name;
    Consumer<NamedList<?>> resultConsumer = result -> response.result = result;
    try {
      doSnapShoot(
          numberToKeep,
          numberBackupsToKeep,
          location,
          repoName,
          commitName,
          name,
          solrCore,
          resultConsumer);
      response.status = ReplicationHandler.OK_STATUS;
    } catch (SolrException e) {
      throw e;
    } catch (Exception e) {
      log.error("Exception while creating a snapshot", e);
      reportErrorOnResponse(
          response, "Error encountered while creating a snapshot: " + e.getMessage(), e);
    }
    return response;
  }

  /** Separate method helps with testing */
  protected void doSnapShoot(
      int numberToKeep,
      int numberBackupsToKeep,
      String location,
      String repoName,
      String commitName,
      String name,
      SolrCore solrCore,
      Consumer<NamedList<?>> resultConsumer)
      throws IOException {
    ReplicationHandler.doSnapShoot(
        numberToKeep,
        numberBackupsToKeep,
        location,
        repoName,
        commitName,
        name,
        solrCore,
        resultConsumer);
  }

  /* POJO for v2 endpoints request body. */
  public static class BackupReplicationRequestBody implements JacksonReflectMapWriter {

    public BackupReplicationRequestBody() {}

    public BackupReplicationRequestBody(
        String location, String name, int numberToKeep, String repository, String commitName) {
      this.location = location;
      this.name = name;
      this.numberToKeep = numberToKeep;
      this.repository = repository;
      this.commitName = commitName;
    }

    @Schema(description = "The path where the backup will be created")
    @JsonProperty
    public String location;

    @Schema(description = "The backup will be created in a directory called snapshot.<name>")
    @JsonProperty
    public String name;

    @Schema(description = "The number of backups to keep.")
    @JsonProperty
    public int numberToKeep;

    @Schema(description = "The name of the repository to be used for e backup.")
    @JsonProperty
    public String repository;

    @Schema(
        description =
            "The name of the commit which was used while taking a snapshot using the CREATESNAPSHOT command.")
    @JsonProperty
    public String commitName;
  }

  /** Response for {@link SnapshotBackupAPI#createBackup(BackupReplicationRequestBody)}. */
  public static class BackupReplicationResponse extends SolrJerseyResponse {

    @JsonProperty("result")
    public NamedList<?> result;

    @JsonProperty("status")
    public String status;

    @JsonProperty("message")
    public String message;

    @JsonProperty("exception")
    public Exception exception;

    public BackupReplicationResponse() {}
  }

  private static void reportErrorOnResponse(
      BackupReplicationResponse response, String message, Exception e) {
    response.status = ERR_STATUS;
    response.message = message;
    if (e != null) {
      response.exception = e;
    }
  }
}
