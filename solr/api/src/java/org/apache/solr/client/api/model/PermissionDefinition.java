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

/** The fields of a Rule-Based Authorization permission, as created or updated by a caller. */
public class PermissionDefinition {
  @Schema(description = "The name of a predefined permission, e.g. 'read', 'update', 'all'.")
  @JsonProperty("name")
  public String name;

  @Schema(description = "The role(s) this permission is granted to.")
  @JsonProperty("role")
  public List<String> role;

  @Schema(
      description =
          "The collection(s) this permission applies to. Omit for collection-agnostic requests"
              + " (e.g. the Collections API); use an explicit null element to mean 'no"
              + " collection'.")
  @JsonProperty("collection")
  public List<String> collection;

  @Schema(description = "The request path(s) this permission applies to.")
  @JsonProperty("path")
  public List<String> path;

  @Schema(description = "The HTTP method(s) this permission applies to.")
  @JsonProperty("method")
  public List<String> method;

  @Schema(description = "Request parameter values this permission is restricted to matching.")
  @JsonProperty("params")
  public Map<String, Object> params;

  @Schema(
      description =
          "On creation only: place the new permission immediately before the permission "
              + "currently at this index, instead of appending it at the end.")
  @JsonProperty("before")
  public Integer before;
}
