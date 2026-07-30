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
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.CreateCollectionSnapshotRequestBody;
import org.apache.solr.client.api.model.CreateCollectionSnapshotResponse;
import org.apache.solr.client.api.model.DeleteCollectionSnapshotResponse;
import org.apache.solr.client.api.model.ListCollectionSnapshotsResponse;

/** V2 API definitions for creating, accessing, and deleting collection-level snapshots. */
public interface CollectionSnapshotApis {

  @Path("/collections/{collName}/snapshots")
  interface Create {
    @POST
    @Path("/{snapshotName}")
    @Operation(
        summary = "Creates a new snapshot of the specified collection.",
        tags = {"collection-snapshots"})
    CreateCollectionSnapshotResponse createCollectionSnapshot(
        @Parameter(description = "The name of the collection.", required = true)
            @PathParam("collName")
            String collName,
        @Parameter(description = "The name of the snapshot to be created.", required = true)
            @PathParam("snapshotName")
            String snapshotName,
        @RequestBody(description = "Contains user provided parameters", required = true)
            CreateCollectionSnapshotRequestBody requestBody)
        throws Exception;
  }

  @Path("/collections/{collName}/snapshots/{snapshotName}")
  interface Delete {
    @DELETE
    @Operation(
        summary = "Delete an existing collection-snapshot by name.",
        tags = {"collection-snapshots"})
    DeleteCollectionSnapshotResponse deleteCollectionSnapshot(
        @Parameter(description = "The name of the collection.", required = true)
            @PathParam("collName")
            String collName,
        @Parameter(description = "The name of the snapshot to be deleted.", required = true)
            @PathParam("snapshotName")
            String snapshotName,
        @Parameter(description = "A flag that treats the collName parameter as a collection alias.")
            @DefaultValue("false")
            @QueryParam("followAliases")
            boolean followAliases,
        @QueryParam("async") String asyncId)
        throws Exception;
  }

  @Path("/collections/{collName}/snapshots")
  interface List {
    @GET
    @Operation(
        summary = "List the snapshots available for a specified collection.",
        tags = {"collection-snapshots"})
    ListCollectionSnapshotsResponse listSnapshots(
        @Parameter(description = "The name of the collection.", required = true)
            @PathParam("collName")
            String collName)
        throws Exception;
  }
}
