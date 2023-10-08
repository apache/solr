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

public class UpdateRuleBasedAuthPermissionPayload implements ReflectMapWriter {
  @JsonProperty public String name;

  // TODO Do we support enum's: only acceptable values here are GET, POST, DELETE, and PUT
  @JsonProperty public String method;

  @JsonProperty public List<String> collection;

  @JsonProperty public List<String> path;

  @JsonProperty(required = true)
  public Integer index;

  @JsonProperty public Integer before;

  @JsonProperty public Map<String, Object> params;

  @JsonProperty(required = true)
  public List<String> role;
}
