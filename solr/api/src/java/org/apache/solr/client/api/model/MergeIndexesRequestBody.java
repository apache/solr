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

/** Request body for endpoints {@link org.apache.solr.client.api.endpoint.MergeIndexesApi} */
public class MergeIndexesRequestBody {
  @Schema(description = "Multi-valued, directories that would be merged.", name = "indexDirs")
  @JsonProperty
  public List<String> indexDirs;

  @Schema(description = "Multi-valued, source cores that would be merged.", name = "srcCores")
  @JsonProperty
  public List<String> srcCores;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty
  public String async;

  @JsonProperty public String updateChain;
}
