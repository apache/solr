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
import java.util.List;
import java.util.Map;

/** Response body for the Schema Designer list-configs endpoint. */
public class SchemaDesignerConfigsResponse extends SolrJerseyResponse {

  /**
   * Map of configSet name to status: 0 = in-progress (temp only), 1 = disabled, 2 = enabled and
   * published.
   */
  @JsonProperty("configSets")
  public Map<String, Integer> configSets;

  @Schema(
      description = "Language choices supplied by the _multilingual configset, ordered by name.")
  @JsonProperty("availableLanguages")
  public List<Language> availableLanguages;

  /** A language supplied by the multilingual schema. */
  @Schema(name = "SchemaDesignerLanguage")
  public static class Language {
    @JsonProperty("code")
    public String code;

    @JsonProperty("name")
    public String name;
  }
}
