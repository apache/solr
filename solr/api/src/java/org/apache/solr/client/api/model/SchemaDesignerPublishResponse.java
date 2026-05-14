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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Response body for the Schema Designer publish endpoint. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SchemaDesignerPublishResponse extends SolrJerseyResponse {

  @JsonProperty("configSet")
  public String configSet;

  @JsonProperty("schemaVersion")
  public Integer schemaVersion;

  /** The new collection created during publish, if requested. */
  @JsonProperty("newCollection")
  public String newCollection;

  /** Error message if indexing sample docs into the new collection failed. */
  @JsonProperty("updateError")
  public String updateError;

  @JsonProperty("updateErrorCode")
  public Integer updateErrorCode;

  @JsonProperty("errorDetails")
  public Object errorDetails;
}
