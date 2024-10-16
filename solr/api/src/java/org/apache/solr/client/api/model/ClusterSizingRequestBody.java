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

public class ClusterSizingRequestBody {

  @Schema(
      description =
          "Document size used to compute resource usage. If less than 1, the value will be computed using the content of currently indexed documents.")
  @JsonProperty("avgDocSize")
  public Long avgDocSize;

  @Schema(
      description =
          "Number of documents to use when computing resource usage. If less than 1, actual number of indexed documents will be used. This parameter will be ignored if estimationRatio is specified.")
  @JsonProperty("numDocs")
  public Long numDocs;

  @Schema(
      description =
          "If specified, will be used as number of deleted documents in the index when computing resource usage, otherwise, current number of deleted documents will be used instead.")
  @JsonProperty("deletedDocs")
  public Long deletedDocs;

  @Schema(
      description =
          "Size of the filter cache to use for computing resource usage, if not specified, current filter cache size will be used.")
  @JsonProperty("filterCacheMax")
  public Long filterCacheMax;

  @Schema(
      description =
          "Size of the query result cache to use for computing resource usage, if not specified, current query result cache size will be used.")
  @JsonProperty("queryResultCacheMax")
  public Long queryResultCacheMax;

  @Schema(
      description =
          "Size of the document cache to use for computing resource usage, if not specified, current document cache size will be used.")
  @JsonProperty("documentCacheMax")
  public Long documentCacheMax;

  @Schema(
      description =
          "Maximum number of documents to cache per entry in query result cache to use for computing resource usage, if not specified, current maximum will be used.")
  @JsonProperty("queryResultMaxDocsCached")
  public Long queryResultMaxDocsCached;

  @Schema(
      description =
          "Ratio used for resource usage estimations. If a value greater than 0.0 is specified, the current number of documents will be multiplied by this ratio in order to determine number of documents to be used when computing resource usage.")
  @JsonProperty("estimationRatio")
  public Double estimationRatio;

  @Schema(
      description =
          "If present, values will be output as 'double', according to the chosen size unit. Default behavior, if not present is a human-readable format.  Allowed values are: GB, MB, KB, bytes")
  @JsonProperty("sizeUnit")
  public String sizeUnit;

  public ClusterSizingRequestBody() {}

  public ClusterSizingRequestBody(
      final Long avgDocSize,
      final Long numDocs,
      final Long deletedDocs,
      final Long filterCacheMax,
      final Long queryResultCacheMax,
      final Long documentCacheMax,
      final Long queryResultMaxDocsCached,
      final Double estimationRatio,
      final String sizeUnit) {
    this.avgDocSize = avgDocSize;
    this.numDocs = numDocs;
    this.deletedDocs = deletedDocs;
    this.filterCacheMax = filterCacheMax;
    this.queryResultCacheMax = queryResultCacheMax;
    this.documentCacheMax = documentCacheMax;
    this.queryResultMaxDocsCached = queryResultMaxDocsCached;
    this.estimationRatio = estimationRatio;
    this.sizeUnit = sizeUnit;
  }
}
