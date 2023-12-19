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
package org.apache.solr.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;

/**
 * Loads the {@link ClusterPluginsSource} depending on the declared implementation. The default
 * implementation is {@link ZkClusterPluginsSource}, but can be overridden by a system property, or
 * by declaring the implementation class in solr.xml
 */
public class ClusterPluginsSourceConfigurator implements NamedListInitializedPlugin {

  private static final String DEFAULT_CLASS_NAME =
      System.getProperty("solr.clusterPluginsSource", ZkClusterPluginsSource.class.getName());

  /**
   * Resolves the name of the class that will be used to provide cluster plugins
   *
   * @param pluginInfo The clusterPluginsSource plugin from the NodeConfig
   * @return The name of the class to use as the {@link ClusterPluginsSource}
   */
  public static String resolveClassName(final PluginInfo pluginInfo) {
    return pluginInfo != null && pluginInfo.isEnabled() ? pluginInfo.className : DEFAULT_CLASS_NAME;
  }

  public static ClusterPluginsSource loadClusterPluginsSource(
      CoreContainer cc, SolrResourceLoader loader, PluginInfo info) {
    ClusterPluginsSource clusterPluginsSource = newInstance(loader, resolveClassName(info), cc);
    if (info != null && info.isEnabled() && info.initArgs != null) {
      Map<String, Object> config = new HashMap<>();
      info.initArgs.toMap(config);
      try {
        ContainerPluginsRegistry.configure(clusterPluginsSource, config, null);
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR, "Invalid " + info.type + " configuration", e);
      }
    }
    return clusterPluginsSource;
  }

  private static ClusterPluginsSource newInstance(
      SolrResourceLoader loader, String className, CoreContainer cc) {
    return loader.newInstance(
        className,
        ClusterPluginsSource.class,
        new String[0],
        new Class<?>[] {CoreContainer.class},
        new Object[] {cc});
  }
}
