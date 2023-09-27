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
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.apache.solr.client.api.model.CreateCoreSnapshotResponse;
import org.apache.solr.client.api.model.DeleteSnapshotResponse;
import org.apache.solr.client.api.model.ListCoreSnapshotsResponse;

/** V2 API definitions for Creating, Listing, and Deleting Core Snapshots. */
@Path("/cores/{coreName}/snapshots")
public interface CoreSnapshotApi {
  @POST
  @Path("/{snapshotName}")
  @Operation(
      summary = "Create a new snapshot of the specified core.",
      tags = {"core-snapshots"})
  CreateCoreSnapshotResponse createSnapshot(
      @Parameter(description = "The name of the core to snapshot.", required = true)
          @PathParam("coreName")
          String coreName,
      @Parameter(description = "The name to associate with the core snapshot.", required = true)
          @PathParam("snapshotName")
          String snapshotName,
      @Parameter(description = "The id to associate with the async task.") @QueryParam("async")
          String taskId)
      throws Exception;

  @GET
  @Operation(
      summary = "List existing snapshots for the specified core.",
      tags = {"core-snapshots"})
  ListCoreSnapshotsResponse listSnapshots(
      @Parameter(
              description = "The name of the core for which to retrieve snapshots.",
              required = true)
          @PathParam("coreName")
          String coreName)
      throws Exception;

  @DELETE
  @Path("/{snapshotName}")
  @Operation(
      summary = "Delete a single snapshot from the specified core.",
      tags = {"core-snapshots"})
  DeleteSnapshotResponse deleteSnapshot(
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
      throws Exception;
}
