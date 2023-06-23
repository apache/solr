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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import io.swagger.v3.oas.annotations.Parameter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.solr.core.SolrCore;
import org.apache.solr.jersey.JacksonReflectMapWriter;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.jersey.SolrJerseyResponse;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 APIs for inspecting and replicating indices
 *
 * <p>These APIs are analogous to the v1 /coreName/replication APIs.
 */
@Path("/cores/{coreName}/replication")
public class CoreReplicationAPI extends ReplicationAPIBase {

  @Inject
  public CoreReplicationAPI(SolrCore solrCore, SolrQueryRequest req, SolrQueryResponse rsp) {
    super(solrCore, req, rsp);
  }

  @GET
  @Path("/indexversion")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_READ_PERM)
  public IndexVersionResponse fetchIndexVersion() throws IOException {
    return doFetchIndexVersion();
  }

  @GET
  @Path("/files/generation/{gen}")
  @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, BINARY_CONTENT_TYPE_V2})
  @PermissionName(CORE_READ_PERM)
  public FileListResponse fetchFileList(
      @Parameter(description = "The generation number of the index", required = true)
          @PathParam("gen")
          long gen) {
    FileListResponse response = doFetchFileList(gen);
    return doFetchFileList(gen);
  }

  /** Response for {@link CoreReplicationAPI#fetchIndexVersion()}. */
  public static class IndexVersionResponse extends SolrJerseyResponse {

    @JsonProperty("indexversion")
    public Long indexVersion;

    @JsonProperty("generation")
    public Long generation;

    @JsonProperty("status")
    public String status;

    public IndexVersionResponse() {}

    public IndexVersionResponse(Long indexVersion, Long generation, String status) {
      this.indexVersion = indexVersion;
      this.generation = generation;
      this.status = status;
    }
  }

  /** Base Response for {@link CoreReplicationAPI#fetchFileList(long)}. */
  public interface FileListResponseBase {}

  /** Response for {@link CoreReplicationAPI#fetchFileList(long)}. */
  public static class FileListResponse extends SolrJerseyResponse {

    @JsonValue
    @JsonProperty("response")
    public FileListResponseBase response;

    public FileListResponse(FileListResponseBase repsonse) {
      this.response = repsonse;
    }

    public void setResponse(FileListResponseBase response) {
      this.response = response;
    }

    public FileListResponseBase getResponse() {
      return this.response;
    }
  }

  public static class FileList implements JacksonReflectMapWriter, FileListResponseBase {
    @JsonProperty("filelist")
    public List<FileMetaData> fileList;

    @JsonProperty("confFiles")
    public List<FileMetaData> confFiles;

    public String status;

    public FileList() {
      this.fileList = new ArrayList<>();
    }

    public void addToFileList(List<FileMetaData> fileMetaData) {
      if (fileList == null) {
        fileList = new ArrayList<>();
      }
      fileList.addAll(fileMetaData);
    }

    public void addToConfFiles(List<FileMetaData> confFilesMetaData) {
      if (confFiles == null) {
        confFiles = new ArrayList<>();
      }
      confFiles.addAll(confFilesMetaData);
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }

  public static class Status implements JacksonReflectMapWriter, FileListResponseBase {

    @JsonProperty("status")
    public String status;

    @JsonProperty("message")
    public String message;

    @JsonProperty("exception")
    public Exception exception;

    public Status() {}

    public void setStatus(String status) {
      this.status = status;
    }

    public void setMessage(String message) {
      this.message = message;
    }

    public void setException(Exception exception) {
      this.exception = exception;
    }
  }

  /**
   * Contained in {@link CoreReplicationAPI.FileListResponse}, this holds metadata from a file for
   * an index
   */
  public static class FileMetaData implements JacksonReflectMapWriter {

    @JsonProperty("size")
    public long size;

    @JsonProperty("name")
    public String name;

    @JsonProperty("checksum")
    public long checksum;

    @JsonProperty("alias")
    public String alias;

    public FileMetaData() {}

    public FileMetaData(long size, String name, long checksum) {
      this.size = size;
      this.name = name;
      this.checksum = checksum;
    }

    public void setSize(long size) {
      this.size = size;
    }

    public void setName(String name) {
      this.name = name;
    }

    public void setChecksum(long checksum) {
      this.checksum = checksum;
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }

    public long getSize() {
      return size;
    }

    public String getName() {
      return name;
    }

    public long getChecksum() {
      return checksum;
    }

    public String getAlias() {
      return alias;
    }
  }
}
