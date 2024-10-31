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
package org.apache.solr.client.api.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;

public class ReplicationFileListResponse extends SolrJerseyResponse {
  public ReplicationFileListResponse() {}

  @Schema(description = "List of lucene files", name = "filelist")
  @JsonProperty("filelist")
  public List<FileMetaData> fileList;

  @Schema(description = "List of configuration files", name = "confFiles")
  @JsonProperty("confFiles")
  public List<FileMetaData> confFiles;

  @Schema(description = "Status of response", name = "status")
  @JsonProperty("status")
  public String status;

  @Schema(description = "Response message", name = "message")
  @JsonProperty("message")
  public String message;

  @Schema(description = "Response exception", name = "exception")
  @JsonProperty("exception")
  public Exception exception;

  public static class FileMetaData {

    @Schema(description = "Size of file in bytes", name = "size")
    @JsonProperty("size")
    public long size;

    @Schema(description = "Name of file", name = "name")
    @JsonProperty("name")
    public String name;

    @Schema(description = "Files checksum value", name = "checksum")
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
  }
}
