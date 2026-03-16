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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.AddPackageVersionRequestBody;
import org.apache.solr.client.api.model.PackagesResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/** V2 API definitions for managing Solr packages. */
@Path("/cluster/package")
public interface PackageApis {

  @GET
  @Operation(
      summary = "List all packages registered in this Solr cluster.",
      tags = {"package"})
  PackagesResponse listPackages(
      @Parameter(description = "If provided, the named package is refreshed on this node.")
          @QueryParam("refreshPackage")
          String refreshPackage,
      @Parameter(
              description =
                  "If provided, the node waits until its package data matches this ZooKeeper version.")
          @QueryParam("expectedVersion")
          Integer expectedVersion);

  @GET
  @Path("/{packageName}")
  @Operation(
      summary = "Get information about a specific package in this Solr cluster.",
      tags = {"package"})
  PackagesResponse getPackage(
      @Parameter(description = "The name of the package.", required = true)
          @PathParam("packageName")
          String packageName);

  @POST
  @Path("/{packageName}/versions")
  @Operation(
      summary = "Add a version of a package to this Solr cluster.",
      tags = {"package"})
  SolrJerseyResponse addPackageVersion(
      @Parameter(description = "The name of the package.", required = true)
          @PathParam("packageName")
          String packageName,
      @RequestBody(description = "Details of the package version to add.", required = true)
          AddPackageVersionRequestBody requestBody);

  @DELETE
  @Path("/{packageName}/versions/{version}")
  @Operation(
      summary = "Delete a specific version of a package from this Solr cluster.",
      tags = {"package"})
  SolrJerseyResponse deletePackageVersion(
      @Parameter(description = "The name of the package.", required = true)
          @PathParam("packageName")
          String packageName,
      @Parameter(description = "The version of the package to delete.", required = true)
          @PathParam("version")
          String version);

  @POST
  @Path("/{packageName}/refresh")
  @Operation(
      summary = "Refresh a package on all nodes in this Solr cluster.",
      tags = {"package"})
  SolrJerseyResponse refreshPackage(
      @Parameter(description = "The name of the package to refresh.", required = true)
          @PathParam("packageName")
          String packageName);
}
