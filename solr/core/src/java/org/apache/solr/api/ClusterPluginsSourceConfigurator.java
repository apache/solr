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

import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.EnvUtils;

/**
 * Loads the {@link ClusterPluginsSource} depending on the declared implementation. The default
 * implementation is {@link ZkClusterPluginsSource}, but can be overridden by teh {@link
 * ContainerPluginsRegistry#MUTABLE_CLUSTER_PLUGINS} property
 */
public class ClusterPluginsSourceConfigurator {

  /**
   * Resolves the name of the class that will be used to provide cluster plugins.
   *
   * @return The name of the class to use as the {@link ClusterPluginsSource}
   */
  public static String resolveClassName() {
    return EnvUtils.getPropAsBool(ContainerPluginsRegistry.MUTABLE_CLUSTER_PLUGINS, true)
        ? ZkClusterPluginsSource.class.getName()
        : NodeConfigClusterPluginsSource.class.getName();
  }

  public static ClusterPluginsSource loadClusterPluginsSource(
      CoreContainer cc, SolrResourceLoader loader) {
    return loader.newInstance(
        resolveClassName(),
        ClusterPluginsSource.class,
        new String[0],
        new Class<?>[] {CoreContainer.class},
        new Object[] {cc});
  }
}
