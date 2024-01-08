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
import java.util.List;
import java.util.Map;

public class CreateReplicaRequestBody {
  @JsonProperty public String name;
  @JsonProperty public String type; // TODO Make this an enum - see SOLR-15796
  @JsonProperty public String instanceDir;
  @JsonProperty public String dataDir;
  @JsonProperty public String ulogDir;
  @JsonProperty public String route;
  @JsonProperty public Integer nrtReplicas;
  @JsonProperty public Integer tlogReplicas;
  @JsonProperty public Integer pullReplicas;
  @JsonProperty public Boolean waitForFinalState;
  @JsonProperty public Boolean followAliases;

  @JsonProperty public String async;

  // TODO This cluster of properties could probably be simplified down to just "nodeSet".  See
  // SOLR-15542
  @JsonProperty public String node;

  @JsonProperty("nodeSet")
  public List<String> nodeSet;

  @JsonProperty public Boolean skipNodeAssignment;

  @JsonProperty public Map<String, String> properties;
}
