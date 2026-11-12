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

/**
 * A single add/delete/delete-by-query that failed but was tolerated because the request's {@code
 * maxErrors} allowed it.
 */
public class ToleratedUpdateError {

  @JsonProperty("type")
  @Schema(description = "The kind of command that failed, e.g. 'ADD', 'DELID', or 'DELQ'.")
  public String type;

  @JsonProperty("id")
  @Schema(description = "The document id or delete-by-query expression that failed.")
  public String id;

  @JsonProperty("message")
  @Schema(description = "The error message describing why the command failed.")
  public String message;
}
