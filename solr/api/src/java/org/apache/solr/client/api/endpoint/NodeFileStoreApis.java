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

import static org.apache.solr.client.api.util.Constants.OMIT_FROM_CODEGEN_PROPERTY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.SolrJerseyResponse;

/**
 * V2 APIs for fetching filestore files, syncing them across nodes, or fetching related metadata.
 */
@Path("/node")
public interface NodeFileStoreApis {
  @GET
  @Operation(
      summary = "Retrieve file contents or metadata from the filestore.",
      tags = {"file-store"},
      // The response of this v2 API is highly variable based on the parameters specified.  It can
      // return raw (potentially binary) file data, a JSON-ified representation of that file data,
      // metadata regarding one or multiple file store entries, etc.  This variability can be
      // handled on the Jersey server side, but would be prohibitively difficult to accommodate in
      // our code-generation templates.  Ideally, cosmetic improvements (e.g. splitting it up into
      // multiple endpoints) will make this unnecessary in the future.  But for now, the extension
      // property below ensures that this endpoint is ignored entirely when doing code generation.
      extensions = {
        @Extension(
            properties = {@ExtensionProperty(name = OMIT_FROM_CODEGEN_PROPERTY, value = "true")})
      })
  @Path("/files{path:.+}")
  SolrJerseyResponse getFile(
      @Parameter(description = "Path to a file or directory within the filestore")
          @PathParam("path")
          String path,
      @Parameter(
              description =
                  "If true, triggers syncing for this file across all nodes in the filestore")
          @QueryParam("sync")
          Boolean sync,
      @Parameter(description = "An optional Solr node name to fetch the file from")
          @QueryParam("getFrom")
          String getFrom,
      @Parameter(description = "Indicates that (only) file metadata should be fetched")
          @QueryParam("meta")
          Boolean meta);
}
