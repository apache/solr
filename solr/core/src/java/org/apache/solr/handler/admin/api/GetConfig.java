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
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ConfigApi;
import org.apache.solr.client.api.model.ConfigInfoResponse;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class GetConfig extends JerseyResource implements ConfigApi.Get {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public GetConfig(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ConfigInfoResponse getConfig() {
    final var response = instantiateJerseyResponse(ConfigInfoResponse.class);
    response.config = buildConfigMap(solrQueryRequest);
    return response;
  }

  @SuppressWarnings({"unchecked"})
  private Map<String, Object> buildConfigMap(SolrQueryRequest solrQueryRequest) {
    Map<String, Object> map =
        Utils.convertToMap(solrQueryRequest.getCore().getSolrConfig(), new LinkedHashMap<>());

    Map<String, Object> reqHandlers =
        (Map<String, Object>)
            map.computeIfAbsent(SolrRequestHandler.TYPE, k -> new LinkedHashMap<>());
    List<PluginInfo> plugins = solrQueryRequest.getCore().getImplicitHandlers();
    for (PluginInfo plugin : plugins) {
      if (SolrRequestHandler.TYPE.equals(plugin.type) && !reqHandlers.containsKey(plugin.name)) {
          reqHandlers.put(plugin.name, plugin);
      }
    }

    return (Map<String, Object>) Utils.getDeepCopy(map, 20, true);
  }


}
