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
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.CollectionStatusResponse;

/**
 * V2 API definition for fetching collection metadata
 *
 * <p>This API (GET /v2/collections/collectionName) is analogous to the v1
 * /admin/collections?action=COLSTATUS command.
 */
@Path("/collections/{collectionName}")
public interface CollectionStatusApi {

  // TODO Query parameters currently match those offered by the v1
  // /admin/collections?action=COLSTATUS.  Should param names be updated/clarified?
  @GET
  @Operation(
      summary = "Fetches metadata about the specified collection",
      tags = {"collections"})
  CollectionStatusResponse getCollectionStatus(
      @Parameter(description = "The name of the collection return metadata for", required = true)
          @PathParam("collectionName")
          String collectionName,
      @Parameter(
              description =
                  "Boolean flag to include metadata (e.g. index an data directories, IndexWriter configuration, etc.) about each shard leader's core")
          @QueryParam("coreInfo")
          Boolean coreInfo,
      @Parameter(
              description =
                  "Boolean flag to include metadata and statistics about the segments used by each shard leader.  Implicitly set to true by 'fieldInfo' and 'sizeInfo'")
          @QueryParam("segments")
          Boolean segments,
      @Parameter(
              description =
                  "Boolean flag to include statistics about the indexed fields present on each shard leader. Implicitly sets the 'segments' flag to 'true'")
          @QueryParam("fieldInfo")
          Boolean fieldInfo,
      @Parameter(
              description =
                  "Boolean flag to include simple estimates of the disk size taken up by each field (e.g. \"id\", \"_version_\") and by each index data structure (e.g. 'storedFields', 'docValues_numeric').")
          @QueryParam("rawSize")
          Boolean rawSize,
      @Parameter(
              description =
                  "Boolean flag to include more involved estimates of the disk size taken up by index data structures, on a per-field basis (e.g. how much data does the \"id\" field contribute to 'storedField' index files).  More detail than 'rawSize', less detail than 'rawSizeDetails'.")
          @QueryParam("rawSizeSummary")
          Boolean rawSizeSummary,
      @Parameter(
              description =
                  "Boolean flag to include detailed statistics about the disk size taken up by various fields and data structures.  More detail than 'rawSize' and 'rawSizeSummary'.")
          @QueryParam("rawSizeDetails")
          Boolean rawSizeDetails,
      @Parameter(
              description =
                  "Percentage (between 0 and 100) of data to read when estimating index size and statistics.  Defaults to 5.0 (i.e. 5%).")
          @QueryParam("rawSizeSamplingPercent")
          Float rawSizeSamplingPercent,
      @Parameter(
              description =
                  "Boolean flag to include information about the largest index files for each Lucene segment.  Implicitly sets the 'segment' flag to 'true'")
          @QueryParam("sizeInfo")
          Boolean sizeInfo)
      throws Exception;
}
