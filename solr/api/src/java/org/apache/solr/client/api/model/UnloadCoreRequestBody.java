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

public class UnloadCoreRequestBody {
  @Schema(description = "If true, will remove the index when unloading the core.")
  @JsonProperty
  public Boolean deleteIndex;

  @Schema(description = "If true, removes the data directory and all sub-directories.")
  @JsonProperty
  public Boolean deleteDataDir;

  @Schema(
      description =
          "If true, removes everything related to the core, including the index directory, configuration files and other related files.")
  @JsonProperty
  public Boolean deleteInstanceDir;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty
  public String async;
}
