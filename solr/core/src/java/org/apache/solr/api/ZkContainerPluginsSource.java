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
import java.lang.invoke.MethodHandles;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.solr.client.solrj.request.beans.PluginMeta;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.handler.admin.ContainerPluginsApi;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The plugin configurations are stored and retrieved from the ZooKeeper cluster properties This
 * supports mutable configurations, and management via the /cluster/plugin APIs
 */
public class ZkContainerPluginsSource implements ContainerPluginsSource {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final String PLUGIN = ZkStateReader.CONTAINER_PLUGINS;
  private final Supplier<SolrZkClient> zkClientSupplier;

  private final ContainerPluginsApi api;

  public ZkContainerPluginsSource(CoreContainer coreContainer) {
    this.zkClientSupplier = coreContainer.zkClientSupplier;
    this.api = new ContainerPluginsApi(coreContainer, this);
  }

  @Override
  public ContainerPluginsApi.Read getReadApi() {
    return api.readAPI;
  }

  @Override
  public ContainerPluginsApi.Edit getEditApi() {
    return api.editAPI;
  }

  /**
   * Retrieve the current plugin configurations.
   *
   * @return current plugin configurations, where keys are plugin names and values are {@link
   *     PluginMeta} plugin metadata.
   * @throws IOException on IO errors
   */
  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> plugins() throws IOException {
    SolrZkClient zkClient = zkClientSupplier.get();
    try {
      Map<String, Object> clusterPropsJson =
          (Map<String, Object>)
              Utils.fromJSON(zkClient.getData(ZkStateReader.CLUSTER_PROPS, null, new Stat(), true));
      return (Map<String, Object>)
          clusterPropsJson.computeIfAbsent(PLUGIN, o -> new LinkedHashMap<>());
    } catch (KeeperException.NoNodeException e) {
      return new LinkedHashMap<>();
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading cluster property", SolrZkClient.checkInterrupted(e));
    }
  }

  @Override
  public void persistPlugins(Function<Map<String, Object>, Map<String, Object>> modifier)
      throws IOException {
    try {
      zkClientSupplier
          .get()
          .atomicUpdate(
              ZkStateReader.CLUSTER_PROPS,
              bytes -> {
                @SuppressWarnings("unchecked")
                Map<String, Object> rawJson =
                    bytes == null
                        ? new LinkedHashMap<>()
                        : (Map<String, Object>) Utils.fromJSON(bytes);
                @SuppressWarnings("unchecked")
                Map<String, Object> pluginsModified =
                    modifier.apply(
                        (Map<String, Object>)
                            rawJson.computeIfAbsent(PLUGIN, o -> new LinkedHashMap<>()));
                if (pluginsModified == null) return null;
                rawJson.put(PLUGIN, pluginsModified);
                return Utils.toJSON(rawJson);
              });
    } catch (KeeperException | InterruptedException e) {
      throw new IOException("Error reading cluster property", SolrZkClient.checkInterrupted(e));
    }
  }
}
