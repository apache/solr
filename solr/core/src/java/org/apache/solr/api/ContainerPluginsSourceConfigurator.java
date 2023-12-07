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
import java.util.HashMap;
import java.util.Map;
import org.apache.solr.common.SolrException;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads the {@link ContainerPluginsSource} depending on the declared implementation. The default
 * implementation is {@link ZkContainerPluginsSource}, but can be overridden by a system property,
 * or by declaring the implementation class in solr.xml
 */
public class ContainerPluginsSourceConfigurator implements NamedListInitializedPlugin {

  private static final String DEFAULT_CLASS_NAME =
      System.getProperty("solr.containerPluginsSource", ZkContainerPluginsSource.class.getName());

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static ContainerPluginsSource loadContainerPluginsSource(
      CoreContainer cc, SolrResourceLoader loader, PluginInfo info) {
    ContainerPluginsSource containerPluginsSource;
    if (info != null && info.isEnabled()) {
      containerPluginsSource = newInstance(loader, info.className, cc);
      if (info.initArgs != null) {
        Map<String, Object> config = new HashMap<>();
        info.initArgs.toMap(config);
        try {
          ContainerPluginsRegistry.configure(containerPluginsSource, config, null);
        } catch (IOException e) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR, "Invalid " + info.type + " configuration", e);
        }
      }
    } else {
      containerPluginsSource = newInstance(loader, DEFAULT_CLASS_NAME, cc);
    }
    return containerPluginsSource;
  }

  private static ContainerPluginsSource newInstance(
      SolrResourceLoader loader, String className, CoreContainer cc) {
    return loader.newInstance(
        className,
        ContainerPluginsSource.class,
        new String[0],
        new Class<?>[] {CoreContainer.class},
        new Object[] {cc});
  }
}
