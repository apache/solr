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

/**
 * Request body for splitting a Solr core via {@link
 * org.apache.solr.client.api.endpoint.SplitCoreApi}
 */
public class SplitCoreRequestBody {
  @Schema(description = "Multi-valued, file system paths to write the split pieces to.")
  @JsonProperty
  public List<String> path;

  @Schema(description = "Multi-valued, names of the cores to split the index into.")
  @JsonProperty
  public List<String> targetCore;

  @Schema(description = "A route key to be used for splitting the index.")
  @JsonProperty
  public String splitKey;

  @Schema(description = "The method to use for splitting the index (e.g. 'rewrite' or 'link').")
  @JsonProperty
  public String splitMethod;

  @Schema(description = "When true, returns the range recommendations for splitting the index.")
  @JsonProperty
  public Boolean getRanges;

  @Schema(description = "Comma-separated hash ranges to split the index into (e.g. 'a-b,c-d').")
  @JsonProperty
  public String ranges;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty
  public String async;
}
