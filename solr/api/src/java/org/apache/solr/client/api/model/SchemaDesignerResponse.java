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
 * Response body for Schema Designer endpoints that operate on a full schema: {@code prepNewSchema},
 * {@code updateFileContents}, {@code addSchemaObject}, {@code updateSchemaObject}, and {@code
 * analyze}.
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

  // --- endpoint-specific fields ---

  /** Source of the sample documents (e.g. "blob", "request"); set by {@code analyze}. */
  @JsonProperty("sampleSource")
  public String sampleSource;

  /** Analysis warning when field-type inference produced errors; set by {@code analyze}. */
  @JsonProperty("analysisError")
  public String analysisError;

  /**
   * The type of schema object that was updated: {@code "field"} or {@code "type"}; set by {@code
   * updateSchemaObject}.
   */
  @JsonProperty("updateType")
  public String updateType;

  /**
   * The updated field definition map; populated when {@code updateType} is {@code "field"} in
   * {@code updateSchemaObject}, or the field name string when returned by {@code addSchemaObject}.
   */
  @JsonProperty("field")
  public Object field;

  /**
   * The updated field-type definition map; populated when {@code updateType} is {@code "type"} in
   * {@code updateSchemaObject}, or the type name string when returned by {@code addSchemaObject}.
   */
  @JsonProperty("type")
  public Object type;

  /** The added dynamic-field name; set by {@code addSchemaObject} when adding a dynamic field. */
  @JsonProperty("dynamicField")
  public Object dynamicField;

  /** The added field-type name; set by {@code addSchemaObject} when adding a field type. */
  @JsonProperty("fieldType")
  public Object fieldType;

  /**
   * Whether the temp collection needs to be rebuilt after this update; set by {@code
   * updateSchemaObject}.
   */
  @JsonProperty("rebuild")
  public Boolean rebuild;

  /**
   * Error message when a file update (e.g. {@code solrconfig.xml}) fails validation; set by {@code
   * updateFileContents}.
   */
  @JsonProperty("updateFileError")
  public String updateFileError;

  /**
   * The raw file content returned when a file update fails validation; set by {@code
   * updateFileContents} so the UI can display the attempted content alongside the error.
   */
  @JsonProperty("fileContent")
  public String fileContent;
}
