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

import static org.apache.solr.client.api.model.Constants.ASYNC;
import static org.apache.solr.client.api.model.Constants.COUNT_PROP;
import static org.apache.solr.client.api.model.Constants.DELETE_DATA_DIR;
import static org.apache.solr.client.api.model.Constants.DELETE_INDEX;
import static org.apache.solr.client.api.model.Constants.DELETE_INSTANCE_DIR;
import static org.apache.solr.client.api.model.Constants.FOLLOW_ALIASES;
import static org.apache.solr.client.api.model.Constants.ONLY_IF_DOWN;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

/** Request body used by {@link org.apache.solr.client.api.endpoint.DeleteReplicaApi} */
public class ScaleCollectionRequestBody {
  public @JsonProperty(value = COUNT_PROP, required = true) @Schema(name = "numToDelete") Integer
      numToDelete;
  public @JsonProperty(FOLLOW_ALIASES) Boolean followAliases;
  public @JsonProperty(DELETE_INSTANCE_DIR) Boolean deleteInstanceDir;
  public @JsonProperty(DELETE_DATA_DIR) Boolean deleteDataDir;
  public @JsonProperty(DELETE_INDEX) Boolean deleteIndex;
  public @JsonProperty(ONLY_IF_DOWN) Boolean onlyIfDown;
  public @JsonProperty(ASYNC) String async;
}
