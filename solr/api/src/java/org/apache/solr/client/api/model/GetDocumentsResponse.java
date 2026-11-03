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
 * Response for the real-time get API ({@code GET /v2/.../get}).
 *
 * <p>When a single document is requested via the {@code id} parameter, the document is returned in
 * the {@link #doc} field. When multiple documents are requested via the {@code ids} parameter, the
 * results are returned in the {@link #response} field as a {@link DocumentsResult}.
 */
public class GetDocumentsResponse extends SolrJerseyResponse {

  /**
   * The retrieved document, populated when a single document is requested via the {@code id} query
   * parameter. {@code null} if the document does not exist or {@code ids} was used instead.
   */
  @JsonProperty("doc")
  public Map<String, Object> doc;

  /**
   * The retrieved documents, populated when multiple documents are requested via the {@code ids}
   * query parameter. {@code null} when the {@code id} parameter is used instead.
   */
  @JsonProperty("response")
  public DocumentsResult response;

  /** Typed container for a list of retrieved documents with associated metadata. */
  public static class DocumentsResult {

    /** Total number of documents found matching the requested IDs. */
    @JsonProperty("numFound")
    public long numFound;

    /** Offset of the first document in this result set. */
    @JsonProperty("start")
    public long start;

    /**
     * Whether {@link #numFound} is exact ({@code true}) or a lower bound ({@code false}). May be
     * {@code null} if not reported by the server.
     */
    @JsonProperty("numFoundExact")
    public Boolean numFoundExact;

    /**
     * Maximum relevance score among the returned documents, if scores were requested. {@code null}
     * when scoring is not applicable.
     */
    @JsonProperty("maxScore")
    public Float maxScore;

    /**
     * The list of retrieved documents. Each document is represented as a {@code Map} from field
     * name to field value, where multi-valued fields have a {@code List} as their value.
     */
    @JsonProperty("docs")
    public List<Map<String, Object>> docs;
  }
}
