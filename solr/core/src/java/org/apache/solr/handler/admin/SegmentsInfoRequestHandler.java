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
package org.apache.solr.handler.admin;

import java.util.Collection;
import java.util.List;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.api.GetSegmentData;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

/** This handler exposes information about last commit generation segments */
public class SegmentsInfoRequestHandler extends RequestHandlerBase {

  public static final String FIELD_INFO_PARAM = "fieldInfo";
  public static final String CORE_INFO_PARAM = "coreInfo";
  public static final String SIZE_INFO_PARAM = "sizeInfo";
  public static final String RAW_SIZE_PARAM = "rawSize";
  public static final String RAW_SIZE_SUMMARY_PARAM = "rawSizeSummary";
  public static final String RAW_SIZE_DETAILS_PARAM = "rawSizeDetails";
  public static final String RAW_SIZE_SAMPLING_PERCENT_PARAM = "rawSizeSamplingPercent";

  @Override
  public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
    final SolrParams params = req.getParams();
    final GetSegmentData segmentDataApi = new GetSegmentData(req.getCore(), req, rsp);
    final SolrJerseyResponse response =
        segmentDataApi.getSegmentData(
            params.getBool(CORE_INFO_PARAM),
            params.getBool(FIELD_INFO_PARAM),
            params.getBool(RAW_SIZE_PARAM),
            params.getBool(RAW_SIZE_SUMMARY_PARAM),
            params.getBool(RAW_SIZE_DETAILS_PARAM),
            params.getFloat(RAW_SIZE_SAMPLING_PERCENT_PARAM),
            params.getBool(SIZE_INFO_PARAM));
    V2ApiUtils.squashIntoSolrResponseWithoutHeader(rsp, response);

    rsp.setHttpCaching(false);
  }

  @Override
  public String getDescription() {
    return "Lucene segments info.";
  }

  @Override
  public Category getCategory() {
    return Category.ADMIN;
  }

  @Override
  public Name getPermissionName(AuthorizationContext request) {
    return Name.METRICS_READ_PERM;
  }

  @Override
  public Boolean registerV2() {
    return Boolean.TRUE;
  }

  @Override
  public Collection<Class<? extends JerseyResource>> getJerseyResources() {
    return List.of(GetSegmentData.class);
  }
}
