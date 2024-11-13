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
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_READ_PERM;

import io.swagger.v3.oas.annotations.Parameter;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import org.apache.solr.client.api.endpoint.ReplicationApis;
import org.apache.solr.client.api.model.FileListResponse;
import org.apache.solr.client.api.model.IndexVersionResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API implementation of {@link ReplicationApis}
 *
 * <p>These APIs are analogous to the v1 /coreName/replication APIs.
 */
public class CoreReplication extends ReplicationAPIBase implements ReplicationApis {

  @Inject
  public CoreReplication(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(solrCore, req, rsp);
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public IndexVersionResponse fetchIndexVersion() throws IOException {
    return doFetchIndexVersion();
  }

  @Override
  @PermissionName(CORE_READ_PERM)
  public FileListResponse fetchFileList(long gen) {
    return doFetchFileList(gen);
  }

  @GET
  @Path("/files/{filePath}")
  @Produces({MediaType.TEXT_PLAIN, BINARY_CONTENT_TYPE_V2})
  public StreamingOutput fetchFile(
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
      throws IOException {
    if (dirType == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Must provide a dirType ");
    }
    return doFetchFile(
        filePath, dirType, offset, len, compression, checksum, maxWriteMBPerSec, gen);
  }
}
