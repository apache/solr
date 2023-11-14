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

package org.apache.solr.cluster.placement.impl;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.plugins.AffinityPlacementFactory;
import org.apache.solr.cluster.placement.plugins.MinimizeCoresPlacementFactory;
import org.apache.solr.cluster.placement.plugins.RandomPlacementFactory;
import org.apache.solr.cluster.placement.plugins.SimplePlacementFactory;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class to work with {@link PlacementPluginFactory} plugins. */
public class PlacementPluginFactoryLoader {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @VisibleForTesting
  static final String PLACEMENTPLUGIN_DEFAULT_SYSPROP = "solr.placementplugin.default";

  /**
   * Loads the {@link PlacementPluginFactory} configured in cluster plugins and then keep it up to
   * date as the plugin configuration changes.
   */
  public static void load(
      DelegatingPlacementPluginFactory pluginFactory, ContainerPluginsRegistry plugins) {
    ContainerPluginsRegistry.ApiInfo pluginFactoryInfo =
        plugins.getPlugin(PlacementPluginFactory.PLUGIN_NAME);
    if (pluginFactoryInfo != null
        && (pluginFactoryInfo.getInstance() instanceof PlacementPluginFactory)) {
      pluginFactory.setDelegate(
          (PlacementPluginFactory<? extends PlacementPluginConfig>)
              pluginFactoryInfo.getInstance());
    }
    ContainerPluginsRegistry.PluginRegistryListener pluginListener =
        new ContainerPluginsRegistry.PluginRegistryListener() {
          @Override
          public void added(ContainerPluginsRegistry.ApiInfo plugin) {
            if (plugin == null || plugin.getInstance() == null) {
              return;
            }
            Object instance = plugin.getInstance();
            if (instance instanceof PlacementPluginFactory) {
              setDelegate(
                  plugin.getInfo(),
                  (PlacementPluginFactory<? extends PlacementPluginConfig>) instance);
            }
          }

          @Override
          public void deleted(ContainerPluginsRegistry.ApiInfo plugin) {
            if (plugin == null || plugin.getInstance() == null) {
              return;
            }
            Object instance = plugin.getInstance();
            if (instance instanceof PlacementPluginFactory) {
              setDelegate(plugin.getInfo(), null);
            }
          }

          @Override
          public void modified(
              ContainerPluginsRegistry.ApiInfo old, ContainerPluginsRegistry.ApiInfo replacement) {
            added(replacement);
          }

          private void setDelegate(
              PluginMeta pluginMeta,
              PlacementPluginFactory<? extends PlacementPluginConfig> factory) {
            if (PlacementPluginFactory.PLUGIN_NAME.equals(pluginMeta.name)) {
              pluginFactory.setDelegate(factory);
            } else {
              log.warn(
                  "Ignoring PlacementPluginFactory plugin with non-standard name: {}", pluginMeta);
            }
          }
        };
    plugins.registerListener(pluginListener);
  }

  /** Returns the default {@link PlacementPluginFactory} configured in solr.xml. */
  public static PlacementPluginFactory<?> getDefaultPlacementPluginFactory(
      NodeConfig nodeConfig, SolrResourceLoader loader) {
    PluginInfo pluginInfo = nodeConfig.getReplicaPlacementFactoryConfig();
    if (null != pluginInfo) {
      return getPlacementPluginFactory(pluginInfo, loader);
    } else {
      return getDefaultPlacementPluginFactory();
    }
  }

  private static PlacementPluginFactory<?> getPlacementPluginFactory(
      PluginInfo pluginInfo, SolrResourceLoader loader) {
    // Load placement plugin factory from solr.xml.
    PlacementPluginFactory<?> placementPluginFactory =
        loader.newInstance(pluginInfo, PlacementPluginFactory.class, false);
    if (null != pluginInfo.initArgs) {
      Map<String, Object> config = new HashMap<>();
      pluginInfo.initArgs.toMap(config);
      try {
        ContainerPluginsRegistry.configure(placementPluginFactory, config, null);
      } catch (IOException e) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Invalid " + pluginInfo.type + " configuration",
            e);
      }
    }
    return placementPluginFactory;
  }

  private static PlacementPluginFactory<?> getDefaultPlacementPluginFactory() {
    // Otherwise use the default provided by system properties.
    String defaultPluginId = System.getProperty(PLACEMENTPLUGIN_DEFAULT_SYSPROP);
    if (defaultPluginId != null) {
      log.info(
          "Default replica placement plugin set in {} to {}",
          PLACEMENTPLUGIN_DEFAULT_SYSPROP,
          defaultPluginId);
      switch (defaultPluginId.toLowerCase(Locale.ROOT)) {
        case "simple":
          return new SimplePlacementFactory();
        case "affinity":
          return new AffinityPlacementFactory();
        case "minimizecores":
          return new MinimizeCoresPlacementFactory();
        case "random":
          return new RandomPlacementFactory();
        default:
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              "Invalid value for system property '"
                  + PLACEMENTPLUGIN_DEFAULT_SYSPROP
                  + "'. Supported values are 'simple', 'random', 'affinity' and 'minimizecores'");
      }
    } else {
      // TODO: Consider making the ootb default AffinityPlacementFactory, see
      // https://issues.apache.org/jira/browse/SOLR-16492
      return new SimplePlacementFactory();
    }
  }
}
