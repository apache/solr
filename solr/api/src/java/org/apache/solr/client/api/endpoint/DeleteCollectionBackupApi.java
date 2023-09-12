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

import static org.apache.solr.client.api.model.Constants.ASYNC;
import static org.apache.solr.client.api.model.Constants.BACKUP_LOCATION;
import static org.apache.solr.client.api.model.Constants.BACKUP_REPOSITORY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.BackupDeletionResponseBody;
import org.apache.solr.client.api.model.PurgeUnusedFilesRequestBody;
import org.apache.solr.client.api.model.PurgeUnusedResponse;

@Path("/backups/{backupName}")
public interface DeleteCollectionBackupApi {

  @Path("/versions/{backupId}")
  @DELETE
  @Operation(
      summary = "Delete incremental backup point by ID",
      tags = {"collection-backups"})
  BackupDeletionResponseBody deleteSingleBackupById(
      @PathParam("backupName") String backupName,
      @PathParam("backupId") String backupId,
      // Optional parameters below
      @QueryParam(BACKUP_LOCATION) String location,
      @QueryParam(BACKUP_REPOSITORY) String repositoryName,
      @QueryParam(ASYNC) String asyncId)
      throws Exception;

  @Path("/versions")
  @DELETE
  @Operation(
      summary = "Delete all incremental backup points older than the most recent N",
      tags = {"collection-backups"})
  BackupDeletionResponseBody deleteMultipleBackupsByRecency(
      @PathParam("backupName") String backupName,
      @QueryParam("retainLatest") Integer versionsToRetain,
      // Optional parameters below
      @QueryParam(BACKUP_LOCATION) String location,
      @QueryParam(BACKUP_REPOSITORY) String repositoryName,
      @QueryParam(ASYNC) String asyncId)
      throws Exception;

  @Path("/purgeUnused")
  @PUT
  @Operation(
      summary = "Garbage collect orphaned incremental backup files",
      tags = {"collection-backups"})
  PurgeUnusedResponse garbageCollectUnusedBackupFiles(
      @PathParam("backupName") String backupName,
      @RequestBody(
              description = "Request body parameters for the orphaned file cleanup",
              required = false)
          PurgeUnusedFilesRequestBody requestBody)
      throws Exception;
}
