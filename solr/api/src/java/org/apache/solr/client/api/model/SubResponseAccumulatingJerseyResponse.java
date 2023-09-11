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

/**
 * Represents API responses composed of the responses of various sub-requests.
 *
 * <p>Many Solr APIs, particularly those historically reliant on overseer processing, return a
 * response to the user that is composed in large part of the responses from all sub-requests made
 * during the APIs execution. (e.g. the collection-deletion response itself contains the responses
 * from the 'UNLOAD' call send to each core.) This class encapsulates those responses as possible.
 */
public class SubResponseAccumulatingJerseyResponse extends AsyncJerseyResponse {

  // TODO The 'Object' value in this and the failure prop below have a more defined structure.
  //  Specifically, each value is a map whose keys are node names and whose values are full
  //  responses (in NamedList form) of all shard or replica requests made to that node by the
  //  overseer.  We've skipped being more explicit here, type-wise, for a few reasons:
  //  1. While the overseer response comes back as a raw NamedList, there's no good way to
  //     serialize it into a more strongly-typed response without some ugly NL inspection code
  //  2. The overseer response can include duplicate keys when multiple replica-requests are sent
  //     by the overseer to the same node.  This makes the overseer response invalid JSON, and
  //     prevents utilizing Jackson for serde.
  //  3. This would still all be surmountable if the user response for overseer-based APIs was
  //     especially worth preserving, but it's not.  We should rework this response format to be
  //     less verbose in the successful case and to be more explicit in the failure case about
  //     which internal replica requests failed.
  //  We should either change this response format to be more helpful, or add stronger typing to
  //  overseer responses so that being more type-explicit here is feasible.
  @JsonProperty("success")
  public Object successfulSubResponsesByNodeName;

  @JsonProperty("failure")
  public Object failedSubResponsesByNodeName;

  @JsonProperty("warning")
  public String warning;
}
