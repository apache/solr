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

/** Response for the get-a-specific-package API. */
public class GetPackageResponse extends SolrJerseyResponse {

  @JsonProperty("result")
  @Schema(description = "The requested package's data.")
  public PackageInfo result;

  /** Data for a single package, returned by the get-package API. */
  public static class PackageInfo {
    @JsonProperty("znodeVersion")
    @Schema(description = "The ZooKeeper version of the packages.json node.")
    public int znodeVersion;

    @JsonProperty("name")
    @Schema(description = "The package name.")
    public String name;

    @JsonProperty("versions")
    @Schema(description = "The versions registered for this package.")
    public List<ListPackagesResponse.PackageVersion> versions;
  }
}
