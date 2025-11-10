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

import static org.apache.solr.client.api.model.Constants.SNAPSHOT_NAME;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;

public class CreateCoreSnapshotResponse extends SolrJerseyResponse {
  @Schema(description = "The name of the core.")
  @JsonProperty
  public String core;

  @Schema(name = "commitName", description = "The name of the created snapshot.")
  @JsonProperty(SNAPSHOT_NAME)
  public String commitName;

  @Schema(description = "The path to the directory containing the index files.")
  @JsonProperty
  public String indexDirPath;

  @Schema(description = "The generation value for the created snapshot.")
  @JsonProperty
  public Long generation;

  @Schema(description = "The list of index filenames contained within the created snapshot.")
  @JsonProperty
  public Collection<String> files;
}
