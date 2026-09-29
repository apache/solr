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
import java.util.Map;

/** Response body for the Schema Designer {@code updateSchemaObject} endpoint. */
public class SchemaDesignerUpdateResponse extends SchemaDesignerResponse {

  /** The type of schema object that was updated: {@code "field"} or {@code "type"}. */
  @JsonProperty("updateType")
  public String updateType;

  /** The updated field definition; populated when {@code updateType} is {@code "field"}. */
  @JsonProperty("field")
  public Map<String, Object> field;

  /** The updated field-type definition; populated when {@code updateType} is {@code "type"}. */
  @JsonProperty("type")
  public Map<String, Object> type;

  /** Whether the temp collection needed to be rebuilt to apply this update. */
  @JsonProperty("rebuild")
  public Boolean rebuild;
}
