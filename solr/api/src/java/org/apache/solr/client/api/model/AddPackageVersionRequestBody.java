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

/** Request body for adding a version of a package. */
public class AddPackageVersionRequestBody {

  @JsonProperty("version")
  @Schema(description = "The version string for this package version.", required = true)
  public String version;

  @JsonProperty("files")
  @Schema(
      description = "File paths from the file store to include in this version.",
      required = true)
  public List<String> files;

  @JsonProperty("manifest")
  @Schema(description = "Optional path to a manifest file in the file store.")
  public String manifest;

  @JsonProperty("manifestSHA512")
  @Schema(description = "Optional SHA-512 hash of the manifest file.")
  public String manifestSHA512;
}
