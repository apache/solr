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

/** Response body for the Schema Designer get-info endpoint. */
public class SchemaDesignerInfoResponse extends SchemaDesignerSettingsResponse {

  @JsonProperty("configSet")
  public String configSet;

  /** Whether the configSet has a published (live) version. */
  @JsonProperty("published")
  public Boolean published;

  @JsonProperty("schemaVersion")
  public Integer schemaVersion;

  /** Collections currently using this configSet. */
  @JsonProperty("collections")
  public List<String> collections;

  /** Number of sample documents stored for this configSet, if available. */
  @JsonProperty("numDocs")
  public Integer numDocs;
}
