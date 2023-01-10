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

package org.apache.solr.client.solrj.request.beans;

import java.util.List;
import java.util.Map;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class CreateCorePayload implements ReflectMapWriter {
  @JsonProperty(required = true)
  public String name;

  @JsonProperty public String instanceDir;

  @JsonProperty public String dataDir;

  @JsonProperty public String ulogDir;

  @JsonProperty public String schema;

  @JsonProperty public String config;

  @JsonProperty public String configSet;

  @JsonProperty public Boolean loadOnStartup;

  // If our JsonProperty clone was more feature-rich here we could specify the property be called
  // 'transient', but without that support it needs to be named something else to avoid conflicting
  // with the 'transient' keyword in Java
  @JsonProperty public Boolean isTransient;

  @JsonProperty public String shard;

  @JsonProperty public String collection;

  // TODO - what type is 'roles' expected to be?
  @JsonProperty public List<String> roles;

  @JsonProperty public String replicaType;

  @JsonProperty public Map<String, Object> properties;

  @JsonProperty public String coreNodeName;

  @JsonProperty public Integer numShards;

  @JsonProperty public Boolean newCollection;

  @JsonProperty public String async;
}
