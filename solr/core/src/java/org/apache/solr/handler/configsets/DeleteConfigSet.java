/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.handler.configsets;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_EDIT_PERM;

import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.client.api.endpoint.ConfigsetsApi;
import org.apache.solr.client.api.model.SolrJerseyResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ConfigSetParams;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;

/** V2 API implementation for ConfigsetsApi.Delete */
public class DeleteConfigSet extends ConfigSetAPIBase implements ConfigsetsApi.Delete {

  @Inject
  public DeleteConfigSet(
      CoreContainer coreContainer,
      SolrQueryRequest solrQueryRequest,
      SolrQueryResponse solrQueryResponse) {
    super(coreContainer, solrQueryRequest, solrQueryResponse);
  }

  @Override
  @PermissionName(CONFIG_EDIT_PERM)
  public SolrJerseyResponse deleteConfigSet(String configSetName) throws Exception {
    final var response = instantiateJerseyResponse(SolrJerseyResponse.class);
    if (StrUtils.isNullOrEmpty(configSetName)) {
      throw new SolrException(
          SolrException.ErrorCode.BAD_REQUEST, "No configset name provided to delete");
    }
    final Map<String, Object> configsetCommandMsg = new HashMap<>();
    configsetCommandMsg.put(NAME, configSetName);

    runConfigSetCommand(
        solrQueryResponse, ConfigSetParams.ConfigSetAction.DELETE, configsetCommandMsg);
    return response;
  }
}
