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

import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ParamSetsApi;
import org.apache.solr.client.api.model.ParamSetsResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.RequestParams;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;

public class GetParamSets extends JerseyResource implements ParamSetsApi.Get {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public GetParamSets(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ParamSetsResponse listParamSets() {
    return buildResponse(null);
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ParamSetsResponse getParamSet(String paramSetName) {
    return buildResponse(paramSetName);
  }

  @SuppressWarnings("unchecked")
  private ParamSetsResponse buildResponse(String paramSetName) {
    final var response = instantiateJerseyResponse(ParamSetsResponse.class);
    final RequestParams requestParams =
        solrQueryRequest.getCore().getSolrConfig().getRequestParams();
    response.znodeVersion = requestParams.getZnodeVersion();

    if (paramSetName == null) {
      Map<String, Object> paramsMap = Utils.convertToMap(requestParams, new LinkedHashMap<>());
      response.params = (Map<String, Object>) paramsMap.get(RequestParams.NAME);
    } else {
      RequestParams.ParamSet paramSet = requestParams.getParams(paramSetName);
      if (paramSet == null) {
        throw new SolrException(
            SolrException.ErrorCode.NOT_FOUND, "No paramset found with the name: " + paramSetName);
      }
      Map<String, Object> paramsMap = new LinkedHashMap<>();
      paramsMap.put(paramSetName, Utils.convertToMap(paramSet, new LinkedHashMap<>()));
      response.params = paramsMap;
    }
    return response;
  }
}
