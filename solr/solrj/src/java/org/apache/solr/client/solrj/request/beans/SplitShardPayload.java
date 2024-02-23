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

import java.util.Map;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class SplitShardPayload implements ReflectMapWriter {
  @JsonProperty public String shard;

  @JsonProperty public String ranges;

  @JsonProperty public String splitKey;

  @JsonProperty public Integer numSubShards;

  @JsonProperty public String splitFuzz;

  @JsonProperty public Boolean timing;

  @JsonProperty public Boolean splitByPrefix;

  @JsonProperty public Boolean followAliases;

  // TODO Should/can this be an enum?  Does the annotation framework have support for enums like
  // apispec files did?
  @JsonProperty public String splitMethod;

  @JsonProperty public Map<String, Object> coreProperties;

  @JsonProperty public String async;

  @JsonProperty public Boolean waitForFinalState;
}
