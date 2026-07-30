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
      @Parameter(description = SegmentsApi.CORE_INFO_PARAM_DESC) @QueryParam("coreInfo")
          Boolean coreInfo,
      @Parameter(
              description =
                  "Boolean flag to include metadata and statistics about the segments used by each shard leader.  Implicitly set to true by 'fieldInfo' and 'sizeInfo'")
          @QueryParam("segments")
          Boolean segments,
      @Parameter(
              description =
                  SegmentsApi.FIELD_INFO_PARAM_DESC
                      + " Implicitly sets the 'segments' flag to 'true'")
          @QueryParam("fieldInfo")
          Boolean fieldInfo,
      @Parameter(description = SegmentsApi.RAW_SIZE_PARAM_DESC) @QueryParam("rawSize")
          Boolean rawSize,
      @Parameter(description = SegmentsApi.RAW_SIZE_SUMMARY_DESC) @QueryParam("rawSizeSummary")
          Boolean rawSizeSummary,
      @Parameter(description = SegmentsApi.RAW_SIZE_DETAILS_DESC) @QueryParam("rawSizeDetails")
          Boolean rawSizeDetails,
      @Parameter(description = SegmentsApi.RAW_SIZE_SAMPLING_PERCENT_DESC)
          @QueryParam("rawSizeSamplingPercent")
          Float rawSizeSamplingPercent,
      @Parameter(
              description =
                  SegmentsApi.SIZE_INFO_PARAM_DESC
                      + ". Implicitly sets the 'segment' flag to 'true'")
          @QueryParam("sizeInfo")
          Boolean sizeInfo)
      throws Exception;
}
