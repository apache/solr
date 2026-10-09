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

import static org.apache.solr.core.PluginInfo.APPENDS;
import static org.apache.solr.core.PluginInfo.DEFAULTS;
import static org.apache.solr.core.PluginInfo.INVARIANTS;
import static org.apache.solr.core.RequestParams.USEPARAM;
import static org.apache.solr.security.PermissionNameProvider.Name.CONFIG_READ_PERM;

import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ConfigApi;
import org.apache.solr.client.api.model.ConfigInfoResponse;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.jersey.NullKeyTolerantMap;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.SolrPluginUtils;

public class GetConfig extends JerseyResource implements ConfigApi.Get {

  private final SolrQueryRequest solrQueryRequest;

  @Inject
  public GetConfig(SolrQueryRequest solrQueryRequest) {
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ConfigInfoResponse getConfig(boolean expandParams) {
    final var response = instantiateJerseyResponse(ConfigInfoResponse.class);
    response.config = buildConfigMap(solrQueryRequest, expandParams);
    return response;
  }

  @SuppressWarnings({"unchecked"})
  private Map<String, Object> buildConfigMap(
      SolrQueryRequest solrQueryRequest, boolean expandParams) {
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

    if (expandParams) {
      for (Map.Entry<String, Object> entry : reqHandlers.entrySet()) {
        entry.setValue(expandUseParams(solrQueryRequest, entry.getValue()));
      }
    }

    final var jsonSafeCopy = new NullKeyTolerantMap("children");
    jsonSafeCopy.putAll((Map<String, Object>) Utils.getDeepCopy(map, 20, true));
    return jsonSafeCopy;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private Map<String, Object> expandUseParams(SolrQueryRequest req, Object plugin) {
    Map<String, Object> pluginInfo;
    if (plugin instanceof Map) {
      pluginInfo = (Map<String, Object>) plugin;
    } else {
      pluginInfo = Utils.convertToMap((PluginInfo) plugin, new LinkedHashMap<>());
    }
    String useParams = (String) pluginInfo.get(USEPARAM);
    String useParamsInReq = req.getOriginalParams().get(USEPARAM);
    if (useParams != null || useParamsInReq != null) {
      Map<String, Object> expanded = new LinkedHashMap<>();
      pluginInfo.put("_useParamsExpanded_", expanded);
      List<String> params = new ArrayList<>();
      if (useParams != null) params.addAll(StrUtils.splitSmart(useParams, ','));
      if (useParamsInReq != null) params.addAll(StrUtils.splitSmart(useParamsInReq, ','));
      for (String param : params) {
        var paramSet = req.getCore().getSolrConfig().getRequestParams().getParams(param);
        expanded.put(param, paramSet != null ? paramSet : "[NOT AVAILABLE]");
      }
      SolrQueryRequest subRequest = req.subRequest(req.getOriginalParams());
      subRequest.getContext().put(USEPARAM, useParams);
      NamedList<?> initArgs = new PluginInfo(SolrRequestHandler.TYPE, pluginInfo).initArgs;
      SolrPluginUtils.setDefaults(
          subRequest,
          RequestHandlerBase.getSolrParamsFromNamedList(initArgs, DEFAULTS),
          RequestHandlerBase.getSolrParamsFromNamedList(initArgs, APPENDS),
          RequestHandlerBase.getSolrParamsFromNamedList(initArgs, INVARIANTS));
      SolrParams mask = new MapSolrParams(Map.of("componentName", "", "expandParams", ""));
      pluginInfo.put("_effectiveParams_", SolrParams.wrapDefaults(mask, subRequest.getParams()));
    }
    return pluginInfo;
  }
}
