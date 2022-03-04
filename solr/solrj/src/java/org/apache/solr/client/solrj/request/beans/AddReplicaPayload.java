/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

public class AddReplicaPayload implements ReflectMapWriter {
  @JsonProperty public String shard;

  @JsonProperty public String _route_;

  // TODO Remove in favor of a createNodeSet/nodeSet param with size=1 (see SOLR-15542)
  @JsonProperty public String node;

  // TODO Rename to 'nodeSet' to match the name used by create-shard and other APIs (see SOLR-15542)
  @JsonProperty public List<String> createNodeSet;

  @JsonProperty public String name;

  @JsonProperty public String instanceDir;

  @JsonProperty public String dataDir;

  @JsonProperty public String ulogDir;

  @JsonProperty public Map<String, Object> coreProperties;

  @JsonProperty public String async;

  @JsonProperty public Boolean waitForFinalState;

  @JsonProperty public Boolean followAliases;

  @JsonProperty public Boolean skipNodeAssignment;

  // TODO Make this an enum - see SOLR-15796
  @JsonProperty public String type;
}
