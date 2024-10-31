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

import static org.apache.solr.client.api.util.Constants.BINARY_CONTENT_TYPE_V2;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import org.apache.solr.client.api.model.SolrJerseyResponse;

@Path("/cores/{coreName}/replication")
public interface CoreReplicationApi {
  @GET
  @Operation(
      summary = "Get the index version and generation of a core",
      tags = {"core-replication"})
  @Path("/indexversion")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  public SolrJerseyResponse fetchIndexVersion(
      @Parameter(description = "The name of the core to fetch index version.", required = true)
          @PathParam("coreName")
          String coreName)
      throws IOException;

  @GET
  @Operation(
      summary = "Get a list of lucene or config files of a core",
      tags = {"core-replication"})
  @Path("/files")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  public SolrJerseyResponse fetchFileList(
      @Parameter(description = "The name of the core to fetch the file list", required = true)
          @PathParam("coreName")
          String coreName,
      @Parameter(description = "The generation number of the index", required = true)
          @QueryParam("generation")
          long gen);

  @GET
  @Operation(
      summary = "Get a stream of a specific file path of a core",
      tags = {"core-replication"})
  @Path("/files/{filePath}")
  @Produces({MediaType.TEXT_PLAIN, BINARY_CONTENT_TYPE_V2})
  public SolrJerseyResponse fetchFile(
      @Parameter(description = "The name of the core to fetch the file", required = true)
          @PathParam("coreName")
          String coreName,
      @PathParam("filePath") String filePath,
      @Parameter(
              description =
                  "Directory type for specific filePath (cf or tlogFile). Defaults to Lucene index (file) directory if empty",
              required = true)
          @QueryParam("dirType")
          String dirType,
      @Parameter(description = "Output stream read/write offset", required = false)
          @QueryParam("offset")
          String offset,
      @Parameter(required = false) @QueryParam("len") String len,
      @Parameter(description = "Compress file output", required = false)
          @QueryParam("compression")
          @DefaultValue("false")
          Boolean compression,
      @Parameter(description = "Write checksum with output stream", required = false)
          @QueryParam("checksum")
          @DefaultValue("false")
          Boolean checksum,
      @Parameter(
              description = "Limit data write per seconds. Defaults to no throttling",
              required = false)
          @QueryParam("maxWriteMBPerSec")
          double maxWriteMBPerSec,
      @Parameter(description = "The generation number of the index", required = false)
          @QueryParam("generation")
          Long gen)
      throws IOException;
}
