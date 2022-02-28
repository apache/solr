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

import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.ReflectMapWriter;

public class RejoinLeaderElectionPayload implements ReflectMapWriter {

  // TODO It seems like most of these properties should be required, but it's hard to tell which
  // ones are meant to be required without that being specified on the v1 API or elsewhere
  @JsonProperty public String collection;

  @JsonProperty public String shard;

  @JsonProperty public String coreNodeName;

  @JsonProperty public String core;

  @JsonProperty public String electionNode;

  @JsonProperty public Boolean rejoinAtHead;
}
