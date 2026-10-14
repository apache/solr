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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Generic logging response that includes the name of the log watcher (e.g. "Log4j2") */
public class LoggingResponse extends SolrJerseyResponse {
  @JsonProperty("watcher")
  public String watcherName;

  /**
   * Per-node results of a request broadcast with the 'nodes' parameter, keyed by node name in "live
   * node" format (e.g. "someHost:8983_solr"). Serialized inline, as top-level fields named by node,
   * mirroring {@link NodeSystemResponse#remoteNodeData}. Empty for requests that were not
   * broadcast.
   */
  // Object, not LoggingResponse, since @JsonAnySetter below also feeds this map raw values.
  public Map<String, Object> remoteNodeData = new LinkedHashMap<>();

  @JsonAnyGetter
  public Map<String, Object> remoteNodeData() {
    return remoteNodeData;
  }

  @JsonAnySetter
  public void setRemoteNodeResponse(String field, Object value) {
    remoteNodeData.put(field, value);
  }

  /**
   * Nodes that were asked to apply a broadcast request (via the 'nodes' parameter) but did not
   * return a response, e.g. because they timed out or errored. Null for requests that were not
   * broadcast, and empty when every requested node responded.
   */
  @JsonProperty("failedNodes")
  public List<String> failedNodes;
}
