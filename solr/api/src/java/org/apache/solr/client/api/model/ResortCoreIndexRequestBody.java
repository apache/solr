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

/** Request body for the {@code RESORTINDEX} core-admin action. */
public class ResortCoreIndexRequestBody {

  @Schema(
      description =
          "The target index sort, in Solr sort syntax (e.g. 'timestamp desc'). If omitted, the "
              + "sort configured for the core is used: the <indexSort> element if present, "
              + "otherwise a (deprecated) SortingMergePolicy sort. Fields must have docValues.")
  @JsonProperty
  public String sort;

  @Schema(description = "Request ID to track this action which will be processed asynchronously.")
  @JsonProperty
  public String async;
}
