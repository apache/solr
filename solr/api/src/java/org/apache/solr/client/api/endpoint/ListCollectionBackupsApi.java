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

import static org.apache.solr.client.api.model.Constants.BACKUP_LOCATION;
import static org.apache.solr.client.api.model.Constants.BACKUP_REPOSITORY;

import io.swagger.v3.oas.annotations.Operation;
import java.io.IOException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ListCollectionBackupsResponse;

/** V2 API definitions for collection-backup "listing". */
@Path("/backups/{backupName}/versions")
public interface ListCollectionBackupsApi {

  @GET
  @Operation(
      summary = "List existing incremental backups at the specified location.",
      tags = {"collection-backups"})
  ListCollectionBackupsResponse listBackupsAtLocation(
      @PathParam("backupName") String backupName,
      @QueryParam(BACKUP_LOCATION) String location,
      @QueryParam(BACKUP_REPOSITORY) String repositoryName)
      throws IOException;
}
