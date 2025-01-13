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

package org.apache.solr.handler.configsets;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.handler.admin.ConfigSetsHandler.DISABLE_CREATE_AUTH_CHECKS;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;
import org.apache.solr.client.api.model.CloneConfigsetRequestBody;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.cloud.ConfigSetCmds;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API implementation for ConfigsetsApi.Clone */
public class CloneConfigSet extends ConfigSetAPIBase implements ConfigsetsApi.Clone {

  @Inject
  public CloneConfigSet(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse cloneExistingConfigSet(CloneConfigsetRequestBody requestBody)
      throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    if (configSetService.checkConfigExists(requestBody.name)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "ConfigSet already exists: " + requestBody.name);
    }

    // is there a base config that already exists
    if (!configSetService.checkConfigExists(requestBody.baseConfigSet)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST,
          "Base ConfigSet does not exist: " + requestBody.baseConfigSet);
    }

    if (!DISABLE_CREATE_AUTH_CHECKS
        && !isTrusted(solrQueryRequest.getUserPrincipal(), coreContainer.getAuthenticationPlugin())
        && configSetService.isConfigSetTrusted(requestBody.baseConfigSet)) {
      throw new SolrException(
          SolrException.ErrorCode.UNAUTHORIZED,
          "Can't create a configset with an unauthenticated request from a trusted "
              + ConfigSetCmds.BASE_CONFIGSET);
    }

    final Map<String, Object> configsetCommandMsg = new HashMap<>();
    configsetCommandMsg.put(NAME, requestBody.name);
    configsetCommandMsg.put(ConfigSetCmds.BASE_CONFIGSET, requestBody.baseConfigSet);
    if (requestBody.properties != null) {
      for (Map.Entry<String, Object> e : requestBody.properties.entrySet()) {
        configsetCommandMsg.put(
            ConfigSetCmds.CONFIG_SET_PROPERTY_PREFIX + e.getKey(), e.getValue());
      }
    }

    runConfigSetCommand(
        solrQueryResponse, ConfigSetParams.ConfigSetAction.CREATE, configsetCommandMsg);
    return response;
  }
}
