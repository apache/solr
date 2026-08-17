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

/**
 * Base response body for Schema Designer endpoints that operate on a full schema: {@code
 * prepNewSchema} returns this directly; {@code updateFileContents}, {@code addSchemaObject}, {@code
 * updateSchemaObject}, and {@code analyze} return the more specific subclasses declared in this
 * package, adding fields particular to each.
 */
public class SchemaDesignerResponse extends SchemaDesignerSettingsResponse {

  // --- core schema identification ---

  @JsonProperty("configSet")
  public String configSet;

  @JsonProperty("schemaVersion")
  public Integer schemaVersion;

  /** The temporary mutable collection used during design (e.g. {@code ._designer_myConfig}). */
  @JsonProperty("tempCollection")
  public String tempCollection;

  /** Active replica core name for the temp collection, used for Luke API calls. */
  @JsonProperty("core")
  public String core;

  @JsonProperty("uniqueKeyField")
  public String uniqueKeyField;

  /** Collections currently using the published version of this configSet. */
  @JsonProperty("collectionsForConfig")
  public List<String> collectionsForConfig;

  // --- schema objects ---

  @JsonProperty("fields")
  public List<Map<String, Object>> fields;

  @JsonProperty("dynamicFields")
  public List<Map<String, Object>> dynamicFields;

  @JsonProperty("fieldTypes")
  public List<Map<String, Object>> fieldTypes;

  /** ConfigSet files available in ZooKeeper (excluding managed-schema and internal files). */
  @JsonProperty("files")
  public List<String> files;

  /** IDs of the first 100 sample documents (present when docs were loaded/analyzed). */
  @JsonProperty("docIds")
  public List<String> docIds;

  /** Total number of sample documents, or -1 when no docs were passed to the endpoint. */
  @JsonProperty("numDocs")
  public Integer numDocs;

  // --- error fields (set when sample-doc indexing fails) ---

  @JsonProperty("updateError")
  public String updateError;

  @JsonProperty("updateErrorCode")
  public Integer updateErrorCode;

  @JsonProperty("errorDetails")
  public Object errorDetails;

  /**
   * Analysis warning when field-type inference produced errors; set by {@code analyze}, {@code
   * updateFileContents}, and {@code updateSchemaObject}.
   */
  @JsonProperty("analysisError")
  public String analysisError;
}
