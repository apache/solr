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
package org.apache.solr.handler.admin.api;

import static org.apache.solr.common.params.CommonAdminParams.SPLIT_KEY;
import static org.apache.solr.common.params.CommonAdminParams.SPLIT_METHOD;
import static org.apache.solr.common.params.CommonParams.PATH;
import static org.apache.solr.common.params.CoreAdminParams.TARGET_CORE;
import static org.apache.solr.handler.ClusterAPI.wrapParams;
import static org.apache.solr.security.PermissionNameProvider.Name.CORE_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.api.endpoint.SplitCoreApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.client.api.model.SplitCoreRequestBody;
import org.apache.solr.common.params.CommonAdminParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/**
 * V2 API for splitting a single core into multiple pieces.
 *
 * <p>The new API (POST /v2/cores/{coreName}/split) is equivalent to the v1
 * /admin/cores?action=split command. The logic remains in the V1 {@link CoreAdminHandler}.
 */
public class SplitCoreAPI extends CoreAdminAPIBase implements SplitCoreApi {

  @Inject
  public SplitCoreAPI(
      CoreContainer coreContainer,
      CoreAdminHandler.CoreAdminAsyncTracker coreAdminAsyncTracker,
      SolrQueryRequest req,
      SolrQueryResponse rsp) {
    super(coreContainer, coreAdminAsyncTracker, req, rsp);
  }

  @Override
  @PermissionName(CORE_EDIT_PERM)
  public SolrJerseyResponse splitCore(String coreName, SplitCoreRequestBody requestBody)
      throws Exception {
    ensureRequiredParameterProvided("coreName", coreName);
    SolrJerseyResponse response = instantiateJerseyResponse(SolrJerseyResponse.class);

    final Map<String, Object> v1Params = new HashMap<>();
    v1Params.put(
        CoreAdminParams.ACTION,
        CoreAdminParams.CoreAdminAction.SPLIT.name().toLowerCase(Locale.ROOT));
    v1Params.put(CoreAdminParams.CORE, coreName);

    if (requestBody != null) {
      if (requestBody.path != null && !requestBody.path.isEmpty()) {
        v1Params.put(PATH, requestBody.path.toArray(new String[0]));
      }
      if (requestBody.targetCore != null && !requestBody.targetCore.isEmpty()) {
        v1Params.put(TARGET_CORE, requestBody.targetCore.toArray(new String[0]));
      }
      if (requestBody.splitKey != null) {
        v1Params.put(SPLIT_KEY, requestBody.splitKey);
      }
      if (requestBody.splitMethod != null) {
        v1Params.put(SPLIT_METHOD, requestBody.splitMethod);
      }
      if (requestBody.getRanges != null) {
        v1Params.put(CoreAdminParams.GET_RANGES, requestBody.getRanges);
      }
      if (requestBody.ranges != null) {
        v1Params.put(CoreAdminParams.RANGES, requestBody.ranges);
      }
      if (requestBody.async != null) {
        v1Params.put(CommonAdminParams.ASYNC, requestBody.async);
      }
    }

    CoreAdminHandler coreHandler = coreContainer.getMultiCoreHandler();
    coreHandler.handleRequestBody(wrapParams(req, v1Params), rsp);
    return response;
  }
}
