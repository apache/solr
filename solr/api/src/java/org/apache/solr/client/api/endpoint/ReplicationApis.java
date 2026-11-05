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

import static org.apache.solr.client.api.util.Constants.RAW_OUTPUT_PROPERTY;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.extensions.Extension;
import io.swagger.v3.oas.annotations.extensions.ExtensionProperty;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import org.apache.solr.client.api.model.CommitsResponse;
import org.apache.solr.client.api.model.FileListResponse;
import org.apache.solr.client.api.model.IndexVersionResponse;
import org.apache.solr.client.api.model.ReplicationDetailsResponse;
import org.apache.solr.client.api.model.ReplicationEnabledRequestBody;
import org.apache.solr.client.api.model.ReplicationPollingRequestBody;
import org.apache.solr.client.api.model.RestoreStatusResponse;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.util.CoreApiParameters;

@Path("/cores/{coreName}/replication")
public interface ReplicationApis {

  @GET
  @CoreApiParameters
  @Operation(
      summary = "Return replication configuration and current status.",
      tags = {"replication"})
  ReplicationDetailsResponse fetchReplicationDetails(
      @Parameter(
              description = "When true on a follower, include nested details fetched from leader",
              required = false)
          @QueryParam("follower")
          @DefaultValue("true")
          Boolean follower);

  @GET
  @CoreApiParameters
  @Path("/restore/status")
  @Operation(
      summary = "Return the status of the most recent restore operation.",
      tags = {"replication"})
  RestoreStatusResponse fetchRestoreStatus();

  @GET
  @CoreApiParameters
  @Path("/commits")
  @Operation(
      summary = "Return the index commit points retained by the specified core.",
      tags = {"replication"})
  CommitsResponse fetchCommits();

  @GET
  @CoreApiParameters
  @Path("/indexversion")
  @Operation(
      summary = "Return the index version of the specified core.",
      tags = {"replication"})
  IndexVersionResponse fetchIndexVersion() throws IOException;

  @GET
  @CoreApiParameters
  @Path("/files")
  @Operation(
      summary = "Return the list of index file that make up the specified core.",
      tags = {"replication"})
  FileListResponse fetchFileList(
      @Parameter(description = "The generation number of the index", required = true)
          @QueryParam("generation")
          long gen);

  @GET
  @CoreApiParameters
  @Operation(
      summary = "Get a stream of a specific file path of a core",
      tags = {"replication"},
      extensions = {
        @Extension(properties = {@ExtensionProperty(name = RAW_OUTPUT_PROPERTY, value = "true")})
      })
  @Path("/files/{filePath}")
  StreamingOutput fetchFile(
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
          Long gen);

  @PUT
  @CoreApiParameters
  @Operation(
      summary = "Enable or disable replication.",
      tags = {"replication"})
  SolrJerseyResponse setReplicationEnabled(
      @RequestBody(required = true) ReplicationEnabledRequestBody requestBody);

  @PUT
  @CoreApiParameters
  @Path("/polling")
  @Operation(
      summary = "Enable or disable polling of the leader by a follower.",
      tags = {"replication"})
  SolrJerseyResponse setReplicationPolling(
      @RequestBody(required = true) ReplicationPollingRequestBody requestBody);
}
