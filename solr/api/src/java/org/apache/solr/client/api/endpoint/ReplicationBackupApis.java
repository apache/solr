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
package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ReplicationBackupRequestBody;
import org.apache.solr.client.api.model.ReplicationBackupResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.util.CoreApiParameters;

/**
 * V2 endpoint for Backup API used for User-Managed clusters and Single-Node Installation.
 *
 * @see ReplicationApis
 */
@Path("/cores/{coreName}/replication")
public interface ReplicationBackupApis {

  @POST
  @CoreApiParameters
  @Path("/backups")
  @Operation(
      summary = "Create a backup of a single core using Solr's 'Replication Handler'",
      tags = {"replication-backups"})
  ReplicationBackupResponse createBackup(
      @RequestBody ReplicationBackupRequestBody backupReplicationPayload);

  @DELETE
  @CoreApiParameters
  @Path("/backups/{name}")
  @Operation(
      summary = "Delete a backup which was taken via Solr's 'Replication Handler'",
      tags = {"replication-backups"})
  SolrJerseyResponse deleteBackup(
      @Parameter(description = "Name of the backup to delete", required = true) @PathParam("name")
          String name,
      @Parameter(description = "location of the backup") @QueryParam("location") String location,
      @Parameter(description = "name of the backup repository") @QueryParam("repository")
          String repository);
}
