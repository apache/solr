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

/**
 * Request body for the Schema Designer add endpoint. Exactly one of the four fields should be
 * populated; the populated field's name is the action and its value carries the schema-object
 * attributes (e.g. for {@code addField}: {@code name}, {@code type}, {@code stored}, etc.).
 */
public class SchemaDesignerAddRequestBody {

  @Schema(name = "addField")
  @JsonProperty("add-field")
  public Map<String, Object> addField;

  @Schema(name = "addDynamicField")
  @JsonProperty("add-dynamic-field")
  public Map<String, Object> addDynamicField;

  @Schema(name = "addCopyField")
  @JsonProperty("add-copy-field")
  public Map<String, Object> addCopyField;

  @Schema(name = "addFieldType")
  @JsonProperty("add-field-type")
  public Map<String, Object> addFieldType;
}
