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
import java.util.Set;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.api.endpoint.ConfigApi;
import org.apache.solr.client.api.model.ConfigInfoResponse;
import org.apache.solr.client.api.model.ConfigOverlayResponse;
import org.apache.solr.client.api.model.ConfigZnodeVersionResponse;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SolrConfigHandler;
import org.apache.solr.jersey.NullKeyTolerantMap;
import org.apache.solr.jersey.PermissionName;
import org.apache.solr.pkg.PackageAPI;
import org.apache.solr.pkg.PackageListeners;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.util.SolrPluginUtils;

public class GetConfig extends JerseyResource implements ConfigApi.Get {

  // TODO: Keep this in sync with @Path regex on ConfigApi.Get#getConfigComponent. These are the
  // internal SolrConfig section names (also used by the legacy v1 GET /config/{component} path);
  // see EXTERNAL_TO_INTERNAL_COMPONENT_NAME for the v2-only pluralized renames.
  // TODO: Remove this set when remaining GET /config/{component} sections also migrate.
  public static final Set<String> MIGRATED_CONFIG_COMPONENTS =
      Set.of(
          "updateHandler",
          "directoryFactory",
          "indexConfig",
          "codecFactory",
          "query",
          "searchComponent",
          "queryResponseWriter",
          "luceneMatchVersion",
          "initParams",
          "listener",
          "requestDispatcher",
          "jmx",
          "queryParser",
          "valueSourceParser",
          "transformer",
          "updateProcessor",
          "updateRequestProcessorChain",
          "queryConverter",
          "cache",
          "circuitBreaker",
          "expressible",
          "schemaFactory",
          "restManager",
          "statsCache",
          "recoveryStrategy",
          "indexReaderFactory");

  // v2-only renames: external (URL/response-key) component name -> internal SolrConfig section
  // name. v1's GET /config/{component} keeps using the internal names unchanged.
  private static final Map<String, String> EXTERNAL_TO_INTERNAL_COMPONENT_NAME =
      Map.of(
          "searchComponents", "searchComponent",
          "queryResponseWriters", "queryResponseWriter",
          "listeners", "listener");

  private static String toInternalComponentName(String externalName) {
    return EXTERNAL_TO_INTERNAL_COMPONENT_NAME.getOrDefault(externalName, externalName);
  }

  public static String toExternalComponentName(String internalName) {
    for (Map.Entry<String, String> e : EXTERNAL_TO_INTERNAL_COMPONENT_NAME.entrySet()) {
      if (e.getValue().equals(internalName)) {
        return e.getKey();
      }
    }
    return internalName;
  }

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

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ConfigOverlayResponse getOverlay() {
    final var response = instantiateJerseyResponse(ConfigOverlayResponse.class);
    response.overlay =
        Utils.convertToMap(
            solrQueryRequest.getCore().getSolrConfig().getOverlay(), new LinkedHashMap<>());
    return response;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ConfigZnodeVersionResponse getZnodeVersion(
      int expectedOverlayVersion, int expectedParamsVersion) {
    final var response = instantiateJerseyResponse(ConfigZnodeVersionResponse.class);
    final var core = solrQueryRequest.getCore();
    final var solrConfig = core.getSolrConfig();

    response.znodeVersion = new ConfigZnodeVersionResponse.ZnodeVersions();
    response.znodeVersion.overlay = solrConfig.getOverlay().getVersion();
    response.znodeVersion.params = solrConfig.getRequestParams().getZnodeVersion();

    SolrConfigHandler.maybeRefreshStaleConfig(core, expectedOverlayVersion, expectedParamsVersion);
    return response;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ConfigInfoResponse getConfigComponent(String component) {
    final var response = instantiateJerseyResponse(ConfigInfoResponse.class);
    Map<String, Object> section = new LinkedHashMap<>();
    section.put(component, solrConfigAsMap().get(toInternalComponentName(component)));
    response.config = jsonSafeCopy(section);
    return response;
  }

  @Override
  @PermissionName(CONFIG_READ_PERM)
  public ConfigInfoResponse getConfigComponentByName(
      String component, String componentName, boolean meta) {
    final var response = instantiateJerseyResponse(ConfigInfoResponse.class);
    Map<String, Object> section = new LinkedHashMap<>();
    section.put(component, solrConfigAsMap().get(toInternalComponentName(component)));
    filterByComponentName(section, component, componentName, meta, solrQueryRequest);
    response.config = jsonSafeCopy(section);
    return response;
  }

  /**
   * Shared by the v2 {@code getConfigComponentByName} API and {@link
   * org.apache.solr.handler.SolrConfigHandler}'s legacy v1 handling for components not yet migrated
   * to v2, so the two don't carry separate copies of this logic.
   *
   * @throws SolrException NOT_FOUND if {@code componentName} does not exist within {@code
   *     component}
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void filterByComponentName(
      Map<String, Object> section,
      String component,
      String componentName,
      boolean meta,
      SolrQueryRequest req) {
    Map pluginNameVsPluginInfo = (Map) section.get(component);
    if (pluginNameVsPluginInfo == null) {
      return;
    }

    Object o =
        pluginNameVsPluginInfo instanceof MapWriter
            ? pluginNameVsPluginInfo
            : pluginNameVsPluginInfo.get(componentName);
    if (o == null) {
      throw new SolrException(
          SolrException.ErrorCode.NOT_FOUND,
          componentName + " not found in config section " + component);
    }

    Map<String, Object> pluginInfo =
        o instanceof MapWriter ? new SimpleOrderedMap<>((MapWriter) o) : (Map<String, Object>) o;
    section.put(
        component,
        pluginNameVsPluginInfo instanceof MapWriter
            ? pluginInfo
            : Map.of(componentName, pluginInfo));

    if (meta) {
      List<PackageListeners.Listener> listeners =
          req.getCore().getPackageListeners().getListeners();
      for (PackageListeners.Listener listener : listeners) {
        Map<String, PackageAPI.PkgVersion> infos = listener.packageDetails();
        if (infos == null || infos.isEmpty()) continue;
        infos.forEach(
            (s, mapWriter) -> {
              Map<String, Object> componentInfo = getComponentInfo(pluginInfo, componentName);
              if (s.equals(componentInfo.get("class"))) {
                (componentInfo).put("_packageinfo_", mapWriter);
              }
            });
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private static Map<String, Object> getComponentInfo(
      Map<String, Object> pluginInfo, String componentName) {
    if (pluginInfo.containsKey("class")) {
      return pluginInfo;
    }
    return (Map<String, Object>)
        pluginInfo.computeIfAbsent(componentName, k -> new LinkedHashMap<>());
  }

  private Map<String, Object> solrConfigAsMap() {
    return Utils.convertToMap(solrQueryRequest.getCore().getSolrConfig(), new LinkedHashMap<>());
  }

  @SuppressWarnings("unchecked")
  private NullKeyTolerantMap jsonSafeCopy(Map<String, Object> map) {
    final var jsonSafeCopy = new NullKeyTolerantMap("children");
    jsonSafeCopy.putAll((Map<String, Object>) Utils.getDeepCopy(map, 20, true));
    return jsonSafeCopy;
  }

  @SuppressWarnings({"unchecked"})
  private Map<String, Object> buildConfigMap(
      SolrQueryRequest solrQueryRequest, boolean expandParams) {

    Map<String, Object> configMap = solrConfigAsMap();

    Map<String, Object> reqHandlers =
        (Map<String, Object>)
            configMap.computeIfAbsent(SolrRequestHandler.TYPE, k -> new LinkedHashMap<>());
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

    return jsonSafeCopy(configMap);
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
