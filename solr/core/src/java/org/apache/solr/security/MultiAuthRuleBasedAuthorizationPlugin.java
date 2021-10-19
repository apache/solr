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

import java.security.Principal;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.ResourceLoader;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.core.CoreContainer;

/**
 * Authorization plugin designed to work with the MultiAuthPlugin to support different AuthorizationPlugin per scheme.
 */
public class MultiAuthRuleBasedAuthorizationPlugin extends RuleBasedAuthorizationPluginBase {
  private final Map<String, RuleBasedAuthorizationPluginBase> pluginMap = new LinkedHashMap<>();
  private final ResourceLoader loader;

  // Need the CC to get the resource loader for loading the sub-plugins
  public MultiAuthRuleBasedAuthorizationPlugin(CoreContainer cc) {
    this.loader = cc.getResourceLoader();
  }

  @Override
  @SuppressWarnings({"unchecked"})
  public void init(Map<String, Object> initInfo) {
    super.init(initInfo);

    Object o = initInfo.get(MultiAuthPlugin.PROPERTY_SCHEMES);
    if (!(o instanceof List)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid config: "+getClass().getName()+" requires a list of schemes!");
    }

    List<Object> schemeList = (List<Object>) o;
    if (schemeList.size() < 2) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid config: "+getClass().getName()+" requires at least two schemes!");
    }

    for (Object s : schemeList) {
      if (!(s instanceof Map)) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid scheme config, expected JSON object but found: " + s);
      }
      initPluginForScheme((Map<String, Object>) s);
    }
  }

  protected void initPluginForScheme(Map<String, Object> schemeMap) {
    Map<String, Object> schemeConfig = new HashMap<>(schemeMap);

    String scheme = (String) schemeConfig.remove(MultiAuthPlugin.PROPERTY_SCHEME);
    if (StringUtils.isEmpty(scheme)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'scheme' is a required attribute: " + schemeMap);
    }

    String clazz = (String) schemeConfig.remove("class");
    if (StringUtils.isEmpty(clazz)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "'class' is a required attribute: " + schemeMap);
    }

    RuleBasedAuthorizationPluginBase pluginForScheme = loader.newInstance(clazz, RuleBasedAuthorizationPluginBase.class);
    pluginForScheme.init(schemeConfig);
    pluginMap.put(scheme.toLowerCase(Locale.ROOT), pluginForScheme);
  }

  /**
   * Pulls roles from the Principal
   *
   * @param principal the user Principal which should contain roles
   * @return set of roles as strings
   */
  @Override
  public Set<String> getUserRoles(Principal principal) {
    for (RuleBasedAuthorizationPluginBase plugin : pluginMap.values()) {
      final Set<String> userRoles = plugin.getUserRoles(principal);
      if (userRoles != null && !userRoles.isEmpty()) {
        return userRoles; // first non-empty match wins
      }
    }
    return Collections.emptySet();
  }
}
