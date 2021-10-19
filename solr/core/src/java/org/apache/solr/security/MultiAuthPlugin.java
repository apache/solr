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
package org.apache.solr.security;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.http.HttpRequest;
import org.apache.http.protocol.HttpContext;
import org.apache.lucene.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SpecProvider;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.CommandOperation;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ValidatingJsonMap;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.eclipse.jetty.client.api.Request;

/**
 * Authentication plugin that supports multiple Authorization schemes, such as Bearer and Basic.
 * The impl simply delegates to one of Solr's other AuthenticationPlugins, such as the BasicAuthPlugin or JWTAuthPlugin.
 */
public class MultiAuthPlugin extends AuthenticationPlugin implements ConfigEditablePlugin, SpecProvider {
  public static final String PROPERTY_SCHEMES = "schemes";
  public static final String PROPERTY_DEFAULT_SCHEME = "default-scheme";
  public static final String PROPERTY_SCHEME = "scheme";
  public static final String AUTHORIZATION_HEADER = "Authorization";

  private static final ThreadLocal<AuthenticationPlugin> pluginInRequest = new ThreadLocal<>();
  private final String UNKNOWN_SCHEME = "";

  private final Map<String, AuthenticationPlugin> pluginMap = new LinkedHashMap<>();
  private final ResourceLoader loader;

  // We forward to this scheme where we're asked to auth w/o an Authorization header
  private String defaultScheme = null;

  // Get the loader from the CoreContainer so we can load the sub-plugins, such as the BasicAuthPlugin for Basic
  public MultiAuthPlugin(CoreContainer cc) {
    this.loader = cc.getResourceLoader();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void init(Map<String, Object> pluginConfig) {
    Object o = pluginConfig.get(PROPERTY_SCHEMES);
    if (!(o instanceof List)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid config: MultiAuthPlugin requires a list of schemes!");
    }

    List<Object> schemeList = (List<Object>) o;
    // if you only have one scheme, then you don't need to use this class
    if (schemeList.size() < 2) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid config: MultiAuthPlugin requires at least two schemes!");
    }

    for (Object s : schemeList) {
      if (!(s instanceof Map)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Invalid scheme config, expected JSON object but found: " + s);
      }
      initPluginForScheme((Map<String, Object>) s);
    }

    defaultScheme = (String) pluginConfig.get(PROPERTY_DEFAULT_SCHEME);
    if (defaultScheme != null) {
      if (!pluginMap.containsKey(defaultScheme)) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Default scheme '" + defaultScheme + "' not configured!");
      }
    } else {
      // first scheme listed in the config is the default
      defaultScheme = pluginMap.keySet().iterator().next();
    }
  }

  protected void initPluginForScheme(Map<String, Object> schemeMap) {
    Map<String, Object> schemeConfig = new HashMap<>(schemeMap);

    String scheme = (String) schemeConfig.remove(PROPERTY_SCHEME);
    if (StringUtils.isEmpty(scheme)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "'scheme' is a required attribute: " + schemeMap);
    }

    String clazz = (String) schemeConfig.remove("class");
    if (StringUtils.isEmpty(clazz)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "'class' is a required attribute: " + schemeMap);
    }

    AuthenticationPlugin pluginForScheme = loader.newInstance(clazz, AuthenticationPlugin.class);
    pluginForScheme.init(schemeConfig);
    pluginMap.put(scheme.toLowerCase(Locale.ROOT), pluginForScheme);
  }

  @Override
  public void initializeMetrics(SolrMetricsContext parentContext, String scope) {
    for (AuthenticationPlugin plugin : pluginMap.values()) {
      plugin.initializeMetrics(parentContext, scope);
    }
  }

  private String getSchemeFromAuthHeader(final String authHeader) {
    final int firstSpace = authHeader.indexOf(' ');
    return (firstSpace != -1) ? authHeader.substring(0, firstSpace).toLowerCase(Locale.ROOT) : UNKNOWN_SCHEME;
  }

  @Override
  public boolean doAuthenticate(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws Exception {
    final String authHeader = request.getHeader(AUTHORIZATION_HEADER);

    // if no Authorization header but is an AJAX request, forward to the default scheme so it can handle it
    if (authHeader == null) {
      if (BasicAuthPlugin.isAjaxRequest(request)) {
        return pluginMap.get(defaultScheme).doAuthenticate(request, response, filterChain);
      }

      throw new SolrException(ErrorCode.UNAUTHORIZED, "No Authorization header");
    }

    final String scheme = getSchemeFromAuthHeader(authHeader);
    final AuthenticationPlugin plugin = pluginMap.get(scheme);
    if (plugin == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Authorization scheme '" + scheme + "' not supported!");
    }

    pluginInRequest.set(plugin);
    return plugin.doAuthenticate(request, response, filterChain);
  }

  @Override
  public void close() throws IOException {
    IOException exc = null;
    for (AuthenticationPlugin plugin : pluginMap.values()) {
      try {
        plugin.close();
      } catch (IOException ioExc) {
        if (exc == null) {
          exc = ioExc;
        }
      }
    }

    if (exc != null) {
      throw exc;
    }
  }

  @Override
  public void closeRequest() {
    AuthenticationPlugin plugin = pluginInRequest.get();
    if (plugin != null) {
      plugin.closeRequest();
      pluginInRequest.remove();
    }
  }

  @Override
  protected boolean interceptInternodeRequest(HttpRequest httpRequest, HttpContext httpContext) {
    for (AuthenticationPlugin plugin : pluginMap.values()) {
      if (plugin.interceptInternodeRequest(httpRequest, httpContext)) {
        return true; // first one to fire wins
      }
    }
    return false;
  }

  @Override
  protected boolean interceptInternodeRequest(Request request) {
    for (AuthenticationPlugin plugin : pluginMap.values()) {
      if (plugin.interceptInternodeRequest(request)) {
        return true; // first one to fire wins
      }
    }
    return false;
  }

  @Override
  public ValidatingJsonMap getSpec() {
    return Utils.getSpec("cluster.security.MultiPluginAuth.Commands").getSpec();
  }

  @Override
  public Map<String, Object> edit(Map<String, Object> latestConf, List<CommandOperation> commands) {
    // TODO: Implement an editing strategy
    return null;
  }
}
