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

/** Response body for the Schema Designer {@code addSchemaObject} endpoint. */
public class SchemaDesignerAddResponse extends SchemaDesignerResponse {

  /** Name of the field that was added, when the request added a field. */
  @JsonProperty("field")
  public String field;

  /** Name of the dynamic field that was added, when the request added a dynamic field. */
  @JsonProperty("dynamicField")
  public String dynamicField;

  /** Name of the field type that was added, when the request added a field type. */
  @JsonProperty("fieldType")
  public String fieldType;
}
