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
package org.apache.solr.schema;

import static org.apache.solr.schema.IndexSchema.SCHEMA;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.xml.parsers.ParserConfigurationException;
import net.jcip.annotations.NotThreadSafe;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.Pair;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.XmlConfigFile;
import org.apache.solr.util.DOMConfigNode;
import org.apache.solr.util.DataConfigNode;
import org.apache.solr.util.SystemIdResolver;
import org.apache.solr.util.plugin.NamedListInitializedPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/** Base class for factories for IndexSchema implementations */
@NotThreadSafe
public abstract class IndexSchemaFactory implements NamedListInitializedPlugin {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static IndexSchema buildIndexSchema(String resourceName, SolrConfig config) {
    return buildIndexSchema(resourceName, config, null);
  }

  /** Instantiates the configured schema factory, then calls create on it. */
  public static IndexSchema buildIndexSchema(
      String resourceName, SolrConfig config, ConfigSetService configSetService) {
    return newIndexSchemaFactory(config).create(resourceName, config, configSetService);
  }

  /** Instantiates us from {@link SolrConfig}. */
  public static IndexSchemaFactory newIndexSchemaFactory(SolrConfig config) {
    PluginInfo info = config.getPluginInfo(IndexSchemaFactory.class.getName());
    IndexSchemaFactory factory;
    if (null != info) {
      factory = config.getResourceLoader().newInstance(info.className, IndexSchemaFactory.class);
      factory.init(info.initArgs);
    } else {
      factory =
          config
              .getResourceLoader()
              .newInstance(ManagedIndexSchemaFactory.class.getName(), IndexSchemaFactory.class);
    }
    return factory;
  }

  /**
   * Returns the resource (file) name that will be used for the schema itself. The answer may be a
   * guess. Do not pass the result of this to {@link #create(String, SolrConfig, ConfigSetService)}.
   * The input is the name coming from the {@link org.apache.solr.core.CoreDescriptor} which acts as
   * a default or asked-for name.
   */
  public String getSchemaResourceName(String cdResourceName) {
    return cdResourceName;
  }

  /**
   * Returns an index schema created from a local resource. The input is usually from the core
   * descriptor.
   */
  public IndexSchema create(
      String resourceName, SolrConfig config, ConfigSetService configSetService) {
    SolrResourceLoader loader = config.getResourceLoader();

    if (null == resourceName) {
      resourceName = IndexSchema.DEFAULT_SCHEMA_FILE;
    }
    try {
      return new IndexSchema(
          resourceName,
          getConfigResource(configSetService, null, loader, resourceName),
          config.luceneMatchVersion,
          loader,
          config.getSubstituteProperties());
    } catch (RuntimeException rte) {
      throw rte;
    } catch (Exception e) {
      final String msg = "Error loading schema resource " + resourceName;
      log.error(msg, e);
      throw new SolrException(ErrorCode.SERVER_ERROR, msg, e);
    }
  }

  public static ConfigSetService.ConfigResource getConfigResource(
      ConfigSetService configSetService,
      InputStream schemaInputStream,
      SolrResourceLoader loader,
      String name) {
    return () ->
        getFromCache(
                name,
                loader,
                () -> {
                  if (!(configSetService instanceof ZkConfigSetService)) return null;
                  return ((ZkConfigSetService) configSetService)
                      .getSolrCloudManager()
                      .getObjectCache();
                },
                () -> loadConfig(schemaInputStream, loader, name))
            .data;
  }

  private static VersionedConfig loadConfig(
      InputStream schemaInputStream, SolrResourceLoader loader, String name) {
    try (InputStream is =
        (schemaInputStream == null ? loader.openResource(name) : schemaInputStream)) {
      ConfigNode node = getParsedSchema(is, loader, name);
      int version =
          is instanceof ZkSolrResourceLoader.ZkByteArrayInputStream
              ? ((ZkSolrResourceLoader.ZkByteArrayInputStream) is).getStat().getVersion()
              : 0;
      return new VersionedConfig(version, node);
    } catch (Exception e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Error fetching schema", e);
    }
  }

  // for testing purposes
  public static volatile Consumer<String> CACHE_MISS_LISTENER = null;

  @SuppressWarnings("unchecked")
  public static VersionedConfig getFromCache(
      String name,
      SolrResourceLoader loader,
      Supplier<ObjectCache> objectCacheSupplier,
      Supplier<VersionedConfig> c) {
    Consumer<String> listener = CACHE_MISS_LISTENER;
    Supplier<VersionedConfig> cfgLoader =
        listener == null
            ? c
            : () -> {
              listener.accept(name);
              return c.get();
            };

    if (loader instanceof ZkSolrResourceLoader) {
      ZkSolrResourceLoader zkLoader = (ZkSolrResourceLoader) loader;
      ObjectCache objectCache = objectCacheSupplier.get();
      if (objectCache == null) return cfgLoader.get();
      Map<String, VersionedConfig> confCache =
          (Map<String, VersionedConfig>)
              objectCache.computeIfAbsent(
                  ConfigSetService.ConfigResource.class.getName(), k -> new ConcurrentHashMap<>());
      Pair<String, Integer> res = zkLoader.getZkResourceInfo(name);
      if (res == null) return cfgLoader.get();
      VersionedConfig result = null;
      result = confCache.computeIfAbsent(res.first(), k -> cfgLoader.get());
      if (result.version == res.second()) {
        return result;
      } else {
        confCache.remove(res.first());
        return confCache.computeIfAbsent(res.first(), k -> cfgLoader.get());
      }
    } else {
      // it's a file system loader, no caching necessary
      return cfgLoader.get();
    }
  }

  public static ConfigNode getParsedSchema(InputStream is, SolrResourceLoader loader, String name)
      throws IOException, SAXException, ParserConfigurationException {
    XmlConfigFile schemaConf = null;
    InputSource inputSource = new InputSource(is);
    inputSource.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
    schemaConf = new XmlConfigFile(loader, SCHEMA, inputSource, "/" + SCHEMA + "/", null);
    return new DataConfigNode(new DOMConfigNode(schemaConf.getDocument().getDocumentElement()));
  }

  public static class VersionedConfig {
    public final int version;
    public final ConfigNode data;

    public VersionedConfig(int version, ConfigNode data) {
      this.version = version;
      this.data = data;
    }
  }
}
