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

/** The Response for the v2 "list collection snapshots" API */
public class ListCollectionSnapshotsResponse extends AsyncJerseyResponse {

  // TODO In practice, map values are of the CollectionSnapshotMetaData type, but that cannot be
  // used here until the class is made into more of a POJO and can join the 'api' module here
  @Schema(description = "The snapshots for the collection.")
  @JsonProperty("snapshots")
  public Map<String, Object> snapshots;
}
