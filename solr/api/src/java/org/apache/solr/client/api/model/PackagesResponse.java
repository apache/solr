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
import java.util.Map;

/** Response for the package listing API. */
public class PackagesResponse extends SolrJerseyResponse {

  @JsonProperty("result")
  @Schema(description = "The package data including znode version and package definitions.")
  public PackageData result;

  /** Package data returned by the package API. */
  public static class PackageData {
    @JsonProperty("znodeVersion")
    @Schema(description = "The ZooKeeper version of the packages.json node.")
    public int znodeVersion;

    @JsonProperty("packages")
    @Schema(description = "Map from package name to list of package versions.")
    public Map<String, List<PackageVersion>> packages;
  }

  /** Describes a single version of a package. */
  public static class PackageVersion {
    @JsonProperty("package")
    @Schema(description = "The package name.")
    public String pkg;

    @JsonProperty("version")
    @Schema(description = "The version string.")
    public String version;

    @JsonProperty("files")
    @Schema(description = "List of file paths from the file store included in this version.")
    public List<String> files;

    @JsonProperty("manifest")
    @Schema(description = "Optional manifest reference.")
    public String manifest;

    @JsonProperty("manifestSHA512")
    @Schema(description = "Optional SHA-512 hash of the manifest.")
    public String manifestSHA512;
  }
}
