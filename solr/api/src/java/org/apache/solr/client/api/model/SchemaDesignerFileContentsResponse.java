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

/** Response body for the Schema Designer {@code updateFileContents} endpoint. */
public class SchemaDesignerFileContentsResponse extends SchemaDesignerResponse {

  /** Error message when the updated file (e.g. {@code solrconfig.xml}) fails validation. */
  @JsonProperty("updateFileError")
  public String updateFileError;

  /**
   * The raw file content submitted, returned when validation fails so the UI can display the
   * attempted content alongside the error.
   */
  @JsonProperty("fileContent")
  public String fileContent;
}
