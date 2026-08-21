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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;
import java.util.Map;

/**
 * Request body for the Schema Designer update endpoint: a flat field or field-type definition. The
 * {@code name} property is required; remaining schema attributes (e.g. {@code type}, {@code
 * indexed}, {@code stored}, {@code analyzer}, {@code copyDest}) are captured via the dynamic {@code
 * additionalProperties} map and forwarded to the Schema API.
 */
public class SchemaDesignerUpdateRequestBody {

  @JsonProperty public String name;

  // Non-final + public so the OpenAPI-generated SolrJ client can assign to it directly.
  // Accessed via @JsonAnyGetter / @JsonAnySetter for JSON (de)serialization.
  public Map<String, Object> additionalProperties = new HashMap<>();

  @JsonAnyGetter
  public Map<String, Object> getAdditionalProperties() {
    return additionalProperties;
  }

  @JsonAnySetter
  public void setAdditionalProperty(String key, Object value) {
    additionalProperties.put(key, value);
  }
}
