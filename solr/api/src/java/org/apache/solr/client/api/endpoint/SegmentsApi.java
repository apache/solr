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
import jakarta.ws.rs.QueryParam;
import org.apache.solr.client.api.model.GetSegmentDataResponse;
import org.apache.solr.client.api.util.CoreApiParameters;

/**
 * V2 API definition for fetching metadata about a core's segments
 *
 * <p>This API (GET /v2/cores/coreName/segments) is analogous to the v1
 * /solr/coreName/admin/segments API
 */
@Path("/cores/{coreName}/segments")
public interface SegmentsApi {

  String CORE_INFO_PARAM_DESC =
      "Boolean flag to include metadata (e.g. index an data directories, IndexWriter configuration, etc.) about each shard leader's core";
  String FIELD_INFO_PARAM_DESC =
      "Boolean flag to include statistics about the indexed fields present on each shard leader.";
  String RAW_SIZE_PARAM_DESC =
      "Boolean flag to include simple estimates of the disk size taken up by each field (e.g. \"id\", \"_version_\") and by each index data structure (e.g. 'storedFields', 'docValues_numeric').";
  String RAW_SIZE_SUMMARY_DESC =
      "Boolean flag to include more involved estimates of the disk size taken up by index data structures, on a per-field basis (e.g. how much data does the \"id\" field contribute to 'storedField' index files).  More detail than 'rawSize', less detail than 'rawSizeDetails'.";
  String RAW_SIZE_DETAILS_DESC =
      "Boolean flag to include detailed statistics about the disk size taken up by various fields and data structures.  More detail than 'rawSize' and 'rawSizeSummary'.";
  String RAW_SIZE_SAMPLING_PERCENT_DESC =
      "Percentage (between 0 and 100) of data to read when estimating index size and statistics.  Defaults to 5.0 (i.e. 5%).";
  String SIZE_INFO_PARAM_DESC =
      "Boolean flag to include information about the largest index files for each Lucene segment.";

  @GET
  @CoreApiParameters
  @Operation(
      summary = "Fetches metadata about the segments in use by the specified core",
      tags = {"segments"})
  GetSegmentDataResponse getSegmentData(
      @Parameter(description = CORE_INFO_PARAM_DESC) @QueryParam("coreInfo") Boolean coreInfo,
      @Parameter(description = FIELD_INFO_PARAM_DESC) @QueryParam("fieldInfo") Boolean fieldInfo,
      @Parameter(description = RAW_SIZE_PARAM_DESC) @QueryParam("rawSize") Boolean rawSize,
      @Parameter(description = RAW_SIZE_SUMMARY_DESC) @QueryParam("rawSizeSummary")
          Boolean rawSizeSummary,
      @Parameter(description = RAW_SIZE_DETAILS_DESC) @QueryParam("rawSizeDetails")
          Boolean rawSizeDetails,
      @Parameter(description = RAW_SIZE_SAMPLING_PERCENT_DESC) @QueryParam("rawSizeSamplingPercent")
          Float rawSizeSamplingPercent,
      @Parameter(description = SIZE_INFO_PARAM_DESC) @QueryParam("sizeInfo") Boolean sizeInfo)
      throws Exception;
}
