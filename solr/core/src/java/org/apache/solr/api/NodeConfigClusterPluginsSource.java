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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.handler.admin.ContainerPluginsApi;

/**
 * Plugin configurations that are defined in solr.xml. This supports immutable deployments, and the
 * /cluster/plugin Edit APIs are not available
 */
public class NodeConfigClusterPluginsSource implements ClusterPluginsSource {

  private final Map<String, Object> plugins;

  private final ContainerPluginsApi api;

  public NodeConfigClusterPluginsSource(final CoreContainer cc) {
    api = new ContainerPluginsApi(cc, this);
    plugins = Map.copyOf(readPlugins(cc.getNodeConfig()));
  }

  @Override
  public ContainerPluginsApi.Read getReadApi() {
    return api.readAPI;
  }

  @Override
  public ContainerPluginsApi.Edit getEditApi() {
    return null;
  }

  @Override
  public Map<String, Object> plugins() throws IOException {
    return plugins;
  }

  /**
   * This method should never be invoked because the Edit Apis are not made available by the plugin
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void persistPlugins(Function<Map<String, Object>, Map<String, Object>> modifier) {
    throw new UnsupportedOperationException(
        "The NodeConfigContainerPluginsSource does not support updates to plugin configurations");
  }

  private static Map<String, Object> readPlugins(final NodeConfig cfg) {
    Map<String, Object> pluginInfos = new HashMap<>();
    PluginInfo[] clusterSingletons = cfg.getClusterSingletonPlugins();
    if (clusterSingletons != null) {
      Arrays.stream(clusterSingletons)
          .forEach(
              p -> {
                Map<String, Object> pluginMap = new HashMap<>();
                pluginMap.put("name", p.name);
                pluginMap.put("class", p.className);

                if (p.initArgs.size() > 0) {
                  Map<String, Object> config = new HashMap<>();
                  p.initArgs.toMap(config);
                  pluginMap.put("config", config);
                }

                pluginInfos.put(p.name, pluginMap);
              });
    }
    return pluginInfos;
  }
}
