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

/** Version information returned by an update request with {@code versions=true}. */
public class UpdateResponse extends SolrJerseyResponse {

  @JsonProperty("adds")
  @Schema(description = "Documents added and the versions assigned to them.")
  public List<VersionedDocument> adds;

  @JsonProperty("deletes")
  @Schema(description = "Documents deleted and the versions assigned to their delete operations.")
  public List<VersionedDocument> deletes;

  @JsonProperty("deleteByQuery")
  @Schema(description = "Delete-by-query operations and the versions assigned to them.")
  public List<VersionedQuery> deleteByQuery;
}
