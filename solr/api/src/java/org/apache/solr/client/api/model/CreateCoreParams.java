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
import java.util.Map;

public class CreateCoreParams {
  @JsonProperty(required = true)
  public String name;

  @JsonProperty public String instanceDir;

  @JsonProperty public String dataDir;

  @JsonProperty public String ulogDir;

  @JsonProperty public String schema;

  @JsonProperty public String config;

  @JsonProperty public String configSet;

  @JsonProperty public Boolean loadOnStartup;

  @Schema(name = "isTransient")
  @JsonProperty("transient")
  public Boolean isTransient;

  @JsonProperty public String shard;

  @JsonProperty public String collection;

  @JsonProperty public String replicaType;

  @JsonProperty public Map<String, String> properties;

  @JsonProperty public Map<String, String> collectionProperties;

  @JsonProperty public String coreNodeName;

  @JsonProperty public Integer numShards;

  @JsonProperty public Boolean newCollection;

  @JsonProperty public String async;
}
