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
package org.apache.solr.client.api.endpoint;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.ClusterSizingResponse;

// solrj common params are not available!
// import org.apache.solr.common.params.SizeParams;

/** V2 API definition for cluster sizing */
@Path("/cluster/sizing")
public interface ClusterSizingApi {
  @GET
  @Operation(
      summary = "Get size estimates for resource usage planning based on the current cluster.",
      tags = {"cluster"})
  ClusterSizingResponse estimateSize(
      @Parameter(description = "Document size used to compute resource usage.")
          @Schema(
              description =
                  "Document size used to compute resource usage. If less than 1, the value will be computed using the content of currently indexed documents.")
          @DefaultValue(value = "0")
          @QueryParam("avgDocSize")
          long avgDocSize,
      @Parameter(description = "Number of documents to use when computing resource usage.")
          @Schema(
              description =
                  "Number of documents to use when computing resource usage. If less than 1, actual number of indexed documents will be used. This parameter will be ignored if estimationRatio is specified.")
          @DefaultValue(value = "0")
          @QueryParam("numDocs")
          long numDocs,
      @Parameter(description = "Number of deleted documents.")
          @Schema(
              description =
                  "If specified, will be used as number of deleted documents in the index when computing resource usage, otherwise, current number of deleted documents will be used instead.")
          @QueryParam("deletedDocs")
          Long deletedDocs,
      @Parameter(description = "Size of the filter cache.")
          @Schema(
              description =
                  "Size of the filter cache to use for computing resource usage, if not specified, current filter cache size will be used.")
          @QueryParam("filterCacheMax")
          Long filterCacheMax,
      @Parameter(description = "Size of the query result cache.")
          @Schema(
              description =
                  "Size of the query result cache to use for computing resource usage, if not specified, current query result cache size will be used.")
          @QueryParam("queryResultCacheMax")
          Long queryResultCacheMax,
      @Parameter(description = "Size of the document cache.")
          @Schema(
              description =
                  "Size of the document cache to use for computing resource usage, if not specified, current document cache size will be used.")
          @QueryParam("documentCacheMax")
          Long documentCacheMax,
      @Parameter(description = "Maximum number of documents to cache per entry.")
          @Schema(
              description =
                  "Maximum number of documents to cache per entry in query result cache to use for computing resource usage, if not specified, current maximum will be used.")
          @QueryParam("queryResultMaxDocsCached")
          Long queryResultMaxDocsCached,
      @Parameter(description = "Ratio used for resource usage estimations.")
          @Schema(
              description =
                  "Ratio used for resource usage estimations. If a value greater than 0.0 is specified, the current number of documents will be multiplied by this ratio in order to determine number of documents to be used when computing resource usage.")
          @DefaultValue(value = "0.0")
          @QueryParam("estimationRatio")
          double estimationRatio,
      @Parameter(description = "Size unit to dispay the information.")
          @Schema(
              description =
                  "If present, values will be output as 'double', according to the chosen size unit. Default behavior, if not present is a human-readable format.  Allowed values are: GB, MB, KB, bytes")
          @QueryParam("sizeUnit")
          String sizeUnit)
      throws Exception;
}
