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
package org.apache.solr.core;

import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.ZkSolrResourceLoader;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.util.DOMConfigNode;
import org.apache.solr.util.DataConfigNode;
import org.apache.solr.util.SystemIdResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;

import static org.apache.solr.schema.IndexSchema.SCHEMA;
import static org.apache.solr.schema.IndexSchema.SLASH;

/**
 * Service class used by the CoreContainer to load ConfigSets for use in SolrCore
 * creation.
 */
public abstract class ConfigSetService {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static ConfigSetService createConfigSetService(CoreContainer coreContainer) {

    NodeConfig nodeConfig = coreContainer.getConfig();
    SolrResourceLoader loader = coreContainer.getResourceLoader();
    ZkController zkController = coreContainer.getZkController();

    String configSetServiceClass = nodeConfig.getConfigSetServiceClass();

    if(configSetServiceClass != null){
      try {
        Class<? extends ConfigSetService> clazz = loader.findClass(configSetServiceClass, ConfigSetService.class);
        Constructor<? extends ConfigSetService> constructor = clazz.getConstructor(CoreContainer.class);
        return constructor.newInstance(coreContainer);
      } catch (Exception e) {
        throw new RuntimeException("create configSetService instance faild,configSetServiceClass:" + configSetServiceClass, e);
      }
    }else if(zkController == null){
      return new FileSystemConfigSetService(coreContainer);
    }else{
      return new ZkConfigSetService(coreContainer);
    }

  }

  /** If in SolrCloud mode, upload config sets for each SolrCore in solr.xml. */
  public static void bootstrapConf(CoreContainer cc) throws IOException {
    // List<String> allCoreNames = cfg.getAllCoreNames();
    List<CoreDescriptor> cds = cc.getCoresLocator().discover(cc);

    if (log.isInfoEnabled()) {
      log.info(
          "bootstrapping config for {} cores into ZooKeeper using solr.xml from {}",
          cds.size(),
          cc.getSolrHome());
    }

    for (CoreDescriptor cd : cds) {
      String coreName = cd.getName();
      String confName = cd.getCollectionName();
      if (StringUtils.isEmpty(confName)) confName = coreName;
      Path udir = cd.getInstanceDir().resolve("conf");
      log.info("Uploading directory {} with name {} for solrCore {}", udir, confName, coreName);
      cc.getConfigSetService().uploadConfig(udir, confName);
    }
  }

  public static ConfigNode getParsedSchema(InputStream is, SolrResourceLoader loader, String name) throws IOException, SAXException, ParserConfigurationException {
    XmlConfigFile schemaConf = null;
    InputSource inputSource = new InputSource(is);
    inputSource.setSystemId(SystemIdResolver.createSystemIdFromResourceName(name));
    schemaConf = new XmlConfigFile(loader, SCHEMA, inputSource, SLASH + SCHEMA + SLASH, null);
    return new DataConfigNode(new DOMConfigNode(schemaConf.getDocument().getDocumentElement()));
  }

  /**
   * Load the ConfigSet for a core
   * @param dcore the core's CoreDescriptor
   * @return a ConfigSet
   */
  @SuppressWarnings({"rawtypes"})
  public final ConfigSet loadConfigSet(CoreDescriptor dcore) {

    SolrResourceLoader coreLoader = createCoreResourceLoader(dcore);

    try {

      // ConfigSet properties are loaded from ConfigSetProperties.DEFAULT_FILENAME file.
      NamedList properties = loadConfigSetProperties(dcore, coreLoader);
      // ConfigSet flags are loaded from the metadata of the ZK node of the configset.
      NamedList flags = loadConfigSetFlags(dcore, coreLoader);

      boolean trusted =
          (coreLoader instanceof ZkSolrResourceLoader
              && flags != null
              && flags.get("trusted") != null
              && !flags.getBooleanArg("trusted")
              ) ? false: true;

      SolrConfig solrConfig = createSolrConfig(dcore, coreLoader, trusted);
      return new ConfigSet(configSetName(dcore), solrConfig, force -> createIndexSchema(dcore, solrConfig, force), properties, trusted);
    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Could not load conf for core " + dcore.getName() +
              ": " + e.getMessage(), e);
    }

  }

  protected final SolrResourceLoader parentLoader;

  /** Optional cache of schemas, key'ed by a bunch of concatenated things */
  private final Cache<String, IndexSchema> schemaCache;

  /**
   * Create a new ConfigSetService
   * @param loader the CoreContainer's resource loader
   * @param shareSchema should we share the IndexSchema among cores of same config?
   */
  public ConfigSetService(SolrResourceLoader loader, boolean shareSchema) {
    this.parentLoader = loader;
    this.schemaCache = shareSchema ? Caffeine.newBuilder().weakValues().build() : null;
  }

  /**
   * Create a SolrConfig object for a core
   * @param cd the core's CoreDescriptor
   * @param loader the core's resource loader
   * @param isTrusted is the configset trusted?
   * @return a SolrConfig object
   */
  protected SolrConfig createSolrConfig(CoreDescriptor cd, SolrResourceLoader loader, boolean isTrusted) {
    return SolrConfig.readFromResourceLoader(loader, cd.getConfigName(), isTrusted, cd.getSubstitutableProperties());
  }

  /**
   * Create an IndexSchema object for a core.  It might be a cached lookup.
   * @param cd the core's CoreDescriptor
   * @param solrConfig the core's SolrConfig
   * @return an IndexSchema
   */
  protected IndexSchema createIndexSchema(CoreDescriptor cd, SolrConfig solrConfig, boolean forceFetch) {
    // This is the schema name from the core descriptor.  Sometimes users specify a custom schema file.
    //   Important:  indexSchemaFactory.create wants this!
    String cdSchemaName = cd.getSchemaName();
    // This is the schema name that we think will actually be used.  In the case of a managed schema,
    //  we don't know for sure without examining what files exists in the configSet, and we don't
    //  want to pay the overhead of that at this juncture.  If we guess wrong, no schema sharing.
    //  The fix is usually to name your schema managed-schema instead of schema.xml.
    IndexSchemaFactory indexSchemaFactory = IndexSchemaFactory.newIndexSchemaFactory(solrConfig);

    String configSet = cd.getConfigSet();
    if (configSet != null && schemaCache != null) {
      String guessSchemaName = indexSchemaFactory.getSchemaResourceName(cdSchemaName);
      Long modVersion = getCurrentSchemaModificationVersion(configSet, solrConfig, guessSchemaName);
      if (modVersion != null) {
        // note: luceneMatchVersion influences the schema
        String cacheKey = configSet + "/" + guessSchemaName + "/" + modVersion + "/" + solrConfig.luceneMatchVersion;
        return schemaCache.get(cacheKey,
            (key) -> indexSchemaFactory.create(cdSchemaName, solrConfig, ConfigSetService.this));
      } else {
        log.warn("Unable to get schema modification version, configSet={} schema={}", configSet, guessSchemaName);
        // see explanation above; "guessSchema" is a guess
      }
    }

    return indexSchemaFactory.create(cdSchemaName, solrConfig, this);
  }

  /**
   * Returns a modification version for the schema file.
   * Null may be returned if not known, and if so it defeats schema caching.
   */
  protected abstract Long getCurrentSchemaModificationVersion(String configSet, SolrConfig solrConfig, String schemaFile);

  /**
   * Return the ConfigSet properties or null if none.
   * @see ConfigSetProperties
   * @param cd the core's CoreDescriptor
   * @param loader the core's resource loader
   * @return the ConfigSet properties
   */
  @SuppressWarnings({"rawtypes"})
  protected NamedList loadConfigSetProperties(CoreDescriptor cd, SolrResourceLoader loader) {
    return ConfigSetProperties.readFromResourceLoader(loader, cd.getConfigSetPropertiesName());
  }

  /**
   * Return the ConfigSet flags or null if none.
   */
  // TODO should fold into configSetProps -- SOLR-14059
  @SuppressWarnings({"rawtypes"})
  protected NamedList loadConfigSetFlags(CoreDescriptor cd, SolrResourceLoader loader) {
    return null;
  }

  /**
   * Create a SolrResourceLoader for a core
   * @param cd the core's CoreDescriptor
   * @return a SolrResourceLoader
   */
  protected abstract SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd);

  /**
   * Return a name for the ConfigSet for a core to be used for printing/diagnostic purposes.
   * @param cd the core's CoreDescriptor
   * @return a name for the core's ConfigSet
   */
  public abstract String configSetName(CoreDescriptor cd);

  /**
   * Check whether a config exists
   *
   * @param configName the config to check existance on
   * @return whether the config exists or not
   * @throws IOException if an I/O error occurs
   */
  public boolean checkConfigExists(String configName) throws IOException {
    return listConfigs().contains(configName);
  }

  /**
   * Delete a config
   *
   * @param configName the config to delete
   * @throws IOException if an I/O error occurs
   */
  public abstract void deleteConfig(String configName) throws IOException;

  /**
   * Copy a config
   *
   * @param fromConfig the config to copy from
   * @param toConfig   the config to copy to
   * @throws IOException if an I/O error occurs
   */
  public abstract void copyConfig(String fromConfig, String toConfig) throws IOException;

  /**
   * Copy a config
   *
   * @param fromConfig      the config to copy from
   * @param toConfig        the config to copy to
   * @param copiedToZkPaths should be an empty Set, will be filled in by function
   *                        with the paths that were actually copied to.
   * @throws IOException if an I/O error occurs
   */
  public abstract void copyConfig(String fromConfig, String toConfig, Set<String> copiedToZkPaths) throws IOException;

  /**
   * Upload files from a given path to a config Zookeeper
   *
   * @param dir        {@link java.nio.file.Path} to the files
   * @param configName the name to give the config
   * @throws IOException if an I/O error occurs or the path does not exist
   */
  public abstract void uploadConfig(Path dir, String configName) throws IOException;

  public abstract void uploadFileToConfig(String fileName, String configName) throws IOException;

  /**
   * Download a config from Zookeeper and write it to the filesystem
   *
   * @param configName the config to download
   * @param dir        the {@link Path} to write files under
   * @throws IOException if an I/O error occurs or the config does not exist
   */
  public abstract void downloadConfig(String configName, Path dir) throws IOException;

  public abstract List<String> listConfigs() throws IOException;

  public interface ConfigResource {

    ConfigNode get() throws Exception;

  }

}
