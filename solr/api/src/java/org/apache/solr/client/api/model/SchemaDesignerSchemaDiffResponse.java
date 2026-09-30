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

/** Response body for the Schema Designer get-schema-diff endpoint. */
public class SchemaDesignerSchemaDiffResponse extends SchemaDesignerSettingsResponse {

  /** The list of field-level differences between the designed schema and the source. */
  @JsonProperty("diff")
  public Map<String, Object> diff;

  /** The configSet used as the diff source (either the published configSet or copyFrom). */
  @JsonProperty("diff-source")
  public String diffSource;
}
