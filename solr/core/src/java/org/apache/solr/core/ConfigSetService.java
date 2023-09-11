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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.solr.cloud.ZkConfigSetService;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Service class used by the CoreContainer to load ConfigSets for use in SolrCore creation. */
public abstract class ConfigSetService {

  public static final String UPLOAD_FILENAME_EXCLUDE_REGEX = "^\\..*$";
  public static final Pattern UPLOAD_FILENAME_EXCLUDE_PATTERN =
      Pattern.compile(UPLOAD_FILENAME_EXCLUDE_REGEX);
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static ConfigSetService createConfigSetService(CoreContainer coreContainer) {
    ConfigSetService configSetService = instantiate(coreContainer);
    // bootstrap conf in SolrCloud mode
    if (coreContainer.getZkController() != null) {
      configSetService.bootstrapConfigSet(coreContainer);
    }
    return configSetService;
  }

  private static ConfigSetService instantiate(CoreContainer coreContainer) {
    final NodeConfig nodeConfig = coreContainer.getConfig();
    final SolrResourceLoader loader = coreContainer.getResourceLoader();
    final ZkController zkController = coreContainer.getZkController();

    final String configSetServiceClass = nodeConfig.getConfigSetServiceClass();

    if (configSetServiceClass != null) {
      try {
        Class<? extends ConfigSetService> clazz =
            loader.findClass(configSetServiceClass, ConfigSetService.class);
        Constructor<? extends ConfigSetService> constructor =
            clazz.getConstructor(CoreContainer.class);
        return constructor.newInstance(coreContainer);
      } catch (Exception e) {
        throw new RuntimeException(
            "create configSetService instance failed, configSetServiceClass:"
                + configSetServiceClass,
            e);
      }
    } else if (zkController == null) {
      return new FileSystemConfigSetService(coreContainer);
    } else {
      return new ZkConfigSetService(coreContainer);
    }
  }

  private void bootstrapConfigSet(CoreContainer coreContainer) {
    // bootstrap _default conf, bootstrap_confdir and bootstrap_conf if provided via system property
    try {
      // _default conf
      bootstrapDefaultConf();

      // bootstrap_confdir
      String confDir = System.getProperty("bootstrap_confdir");
      if (confDir != null) {
        bootstrapConfDir(confDir);
      }

      // bootstrap_conf
      boolean boostrapConf = Boolean.getBoolean("bootstrap_conf");
      if (boostrapConf == true) {
        bootstrapConf(coreContainer);
      }
    } catch (IOException e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR, "Config couldn't be uploaded ", e);
    }
  }

  private void bootstrapDefaultConf() throws IOException {
    if (this.checkConfigExists("_default") == false) {
      Path configDirPath = getDefaultConfigDirPath();
      if (configDirPath == null) {
        log.warn(
            "The _default configset could not be uploaded. Please provide 'solr.default.confdir' parameter that points to a configset {} {}",
            "intended to be the default. Current 'solr.default.confdir' value:",
            System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE));
      } else {
        this.uploadConfig(ConfigSetsHandler.DEFAULT_CONFIGSET_NAME, configDirPath);
      }
    }
  }

  private void bootstrapConfDir(String confDir) throws IOException {
    Path configPath = Path.of(confDir);
    if (!Files.isDirectory(configPath)) {
      throw new IllegalArgumentException(
          "bootstrap_confdir must be a directory of configuration files, configPath: "
              + configPath);
    }
    String confName =
        System.getProperty(
            ZkController.COLLECTION_PARAM_PREFIX + ZkController.CONFIGNAME_PROP, "configuration1");
    this.uploadConfig(confName, configPath);
  }

  /**
   * Gets the absolute filesystem path of the _default configset to bootstrap from. First tries the
   * sysprop "solr.default.confdir". If not found, tries to find the _default dir relative to the
   * sysprop "solr.install.dir". Returns null if not found anywhere.
   *
   * @lucene.internal
   * @see SolrDispatchFilter#SOLR_DEFAULT_CONFDIR_ATTRIBUTE
   */
  public static Path getDefaultConfigDirPath() {
    String confDir = System.getProperty(SolrDispatchFilter.SOLR_DEFAULT_CONFDIR_ATTRIBUTE);
    if (confDir != null) {
      Path path = Path.of(confDir);
      if (Files.exists(path)) {
        return path;
      }
    }

    String installDir = System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
    if (installDir != null) {
      Path subPath = Path.of("server", "solr", "configsets", "_default", "conf");
      Path path = Path.of(installDir).resolve(subPath);
      if (Files.exists(path)) {
        return path;
      }
    }

    return null;
  }

  // Order is important here since "confDir" may be
  // 1> a full path to the parent of a solrconfig.xml or parent of /conf/solrconfig.xml
  // 2> one of the canned config sets only, e.g. _default
  // and trying to assemble a path for configsetDir/confDir is A Bad Idea. if confDir is a full
  // path.
  public static Path getConfigsetPath(String confDir, String configSetDir) {

    // A local path to the source, probably already includes "conf".
    Path ret = Path.of(confDir, "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Path.of(confDir).normalize();
    }

    // a local path to the parent of a "conf" directory
    ret = Path.of(confDir, "conf", "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Path.of(confDir, "conf").normalize();
    }

    // one of the canned configsets.
    ret = Path.of(configSetDir, confDir, "conf", "solrconfig.xml").normalize();
    if (Files.exists(ret)) {
      return Path.of(configSetDir, confDir, "conf").normalize();
    }

    throw new IllegalArgumentException(
        String.format(
            Locale.ROOT,
            "Could not find solrconfig.xml at %s, %s or %s",
            Path.of(confDir, "solrconfig.xml").normalize().toAbsolutePath(),
            Path.of(confDir, "conf", "solrconfig.xml").normalize().toAbsolutePath(),
            Path.of(configSetDir, confDir, "conf", "solrconfig.xml").normalize().toAbsolutePath()));
  }

  /** If in SolrCloud mode, upload configSets for each SolrCore in solr.xml. */
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
      if (StrUtils.isNullOrEmpty(confName)) confName = coreName;
      Path udir = cd.getInstanceDir().resolve("conf");
      log.info("Uploading directory {} with name {} for solrCore {}", udir, confName, coreName);
      cc.getConfigSetService().uploadConfig(confName, udir);
    }
  }

  /**
   * Return whether the given configSet is trusted.
   *
   * @param name name of the configSet
   */
  public boolean isConfigSetTrusted(String name) throws IOException {
    Map<String, Object> contentMap = getConfigMetadata(name);
    return (boolean) contentMap.getOrDefault("trusted", true);
  }

  /**
   * Return whether the configSet used for the given resourceLoader is trusted.
   *
   * @param coreLoader resourceLoader for a core
   */
  public boolean isConfigSetTrusted(SolrResourceLoader coreLoader) throws IOException {
    // ConfigSet flags are loaded from the metadata of the ZK node of the configset. (For the
    // ZKConfigSetService)
    NamedList<?> flags = loadConfigSetFlags(coreLoader);

    // Trust if there is no trusted flag (i.e. the ConfigSetApi was not used for this configSet)
    // or if the trusted flag is set to "true".
    return (flags == null || flags.get("trusted") == null || flags.getBooleanArg("trusted"));
  }

  /**
   * Load the ConfigSet for a core
   *
   * @param dcore the core's CoreDescriptor
   * @return a ConfigSet
   */
  public final ConfigSet loadConfigSet(CoreDescriptor dcore) {

    SolrResourceLoader coreLoader = createCoreResourceLoader(dcore);

    try {
      // ConfigSet properties are loaded from ConfigSetProperties.DEFAULT_FILENAME file.
      NamedList<?> properties = loadConfigSetProperties(dcore, coreLoader);
      boolean trusted = isConfigSetTrusted(coreLoader);

      SolrConfig solrConfig = createSolrConfig(dcore, coreLoader, trusted);
      return new ConfigSet(
          configSetName(dcore),
          solrConfig,
          force -> {
            try {
              return createIndexSchema(dcore, solrConfig, force);
            } catch (IOException e) {
              throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e.getMessage(), e);
            }
          },
          properties,
          trusted);
    } catch (Exception e) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Could not load conf for core " + dcore.getName() + ": " + e.getMessage(),
          e);
    }
  }

  protected final SolrResourceLoader parentLoader;

  /** Optional cache of schemas, key'ed by a bunch of concatenated things */
  private final Cache<String, IndexSchema> schemaCache;

  /**
   * Create a new ConfigSetService
   *
   * @param loader the CoreContainer's resource loader
   * @param shareSchema should we share the IndexSchema among cores of same config?
   */
  public ConfigSetService(SolrResourceLoader loader, boolean shareSchema) {
    this.parentLoader = loader;
    this.schemaCache = shareSchema ? Caffeine.newBuilder().weakValues().build() : null;
  }

  /**
   * Create a SolrConfig object for a core
   *
   * @param cd the core's CoreDescriptor
   * @param loader the core's resource loader
   * @param isTrusted is the configset trusted?
   * @return a SolrConfig object
   */
  protected SolrConfig createSolrConfig(
      CoreDescriptor cd, SolrResourceLoader loader, boolean isTrusted) throws IOException {
    return SolrConfig.readFromResourceLoader(
        loader, cd.getConfigName(), isTrusted, cd.getSubstitutableProperties());
  }

  /**
   * Create an IndexSchema object for a core. It might be a cached lookup.
   *
   * @param cd the core's CoreDescriptor
   * @param solrConfig the core's SolrConfig
   * @return an IndexSchema
   */
  protected IndexSchema createIndexSchema(
      CoreDescriptor cd, SolrConfig solrConfig, boolean forceFetch) throws IOException {
    // This is the schema name from the core descriptor.  Sometimes users specify a custom schema
    // file. Important:  indexSchemaFactory.create wants this!
    String cdSchemaName = cd.getSchemaName();
    // This is the schema name that we think will actually be used.  In the case of a managed
    // schema, we don't know for sure without examining what files exists in the configSet, and we
    // don't want to pay the overhead of that at this juncture.  If we guess wrong, no schema
    // sharing. The fix is usually to name your schema managed-schema.xml instead of schema.xml.
    IndexSchemaFactory indexSchemaFactory = IndexSchemaFactory.newIndexSchemaFactory(solrConfig);

    String configSet = cd.getConfigSet();
    if (configSet != null && schemaCache != null) {
      String guessSchemaName = indexSchemaFactory.getSchemaResourceName(cdSchemaName);
      Long modVersion = getCurrentSchemaModificationVersion(configSet, solrConfig, guessSchemaName);
      if (modVersion != null) {
        // note: luceneMatchVersion influences the schema
        String cacheKey =
            configSet
                + "/"
                + guessSchemaName
                + "/"
                + modVersion
                + "/"
                + solrConfig.luceneMatchVersion;
        return schemaCache.get(
            cacheKey,
            (key) -> indexSchemaFactory.create(cdSchemaName, solrConfig, ConfigSetService.this));
      } else {
        log.warn(
            "Unable to get schema modification version, configSet={} schema={}",
            configSet,
            guessSchemaName);
        // see explanation above; "guessSchema" is a guess
      }
    }

    return indexSchemaFactory.create(cdSchemaName, solrConfig, this);
  }

  /**
   * Returns a modification version for the schema file. Null may be returned if not known, and if
   * so it defeats schema caching.
   */
  protected abstract Long getCurrentSchemaModificationVersion(
      String configSet, SolrConfig solrConfig, String schemaFile) throws IOException;

  /**
   * Return the ConfigSet properties or null if none.
   *
   * @see ConfigSetProperties
   * @param cd the core's CoreDescriptor
   * @param loader the core's resource loader
   * @return the ConfigSet properties
   */
  protected NamedList<Object> loadConfigSetProperties(CoreDescriptor cd, SolrResourceLoader loader)
      throws IOException {
    return ConfigSetProperties.readFromResourceLoader(loader, cd.getConfigSetPropertiesName());
  }

  /** Return the ConfigSet flags or null if none. */
  // TODO should fold into configSetProps -- SOLR-14059
  protected NamedList<Object> loadConfigSetFlags(SolrResourceLoader loader) throws IOException {
    return null;
  }

  /**
   * Create a SolrResourceLoader for a core
   *
   * @param cd the core's CoreDescriptor
   * @return a SolrResourceLoader
   */
  protected abstract SolrResourceLoader createCoreResourceLoader(CoreDescriptor cd);

  /**
   * Return a name for the ConfigSet for a core to be used for printing/diagnostic purposes.
   *
   * @param cd the core's CoreDescriptor
   * @return a name for the core's ConfigSet
   */
  public abstract String configSetName(CoreDescriptor cd);

  /**
   * Upload files from a given path to config
   *
   * @param configName the config name
   * @param dir {@link Path} to the files
   * @throws IOException if an I/O error occurs or the path does not exist
   */
  public abstract void uploadConfig(String configName, Path dir) throws IOException;

  /**
   * Upload a file to config If file does not exist, it will be uploaded If overwriteOnExists is set
   * to true then file will be overwritten
   *
   * @param configName the name to give the config
   * @param fileName the name of the file with '/' used as the file path separator
   * @param data the content of the file
   * @param overwriteOnExists if true then file will be overwritten
   * @throws SolrException if file exists and overwriteOnExists == false
   */
  public abstract void uploadFileToConfig(
      String configName, String fileName, byte[] data, boolean overwriteOnExists)
      throws IOException;

  /**
   * Download all files from this config to the filesystem at dir
   *
   * @param configName the config to download
   * @param dir the {@link Path} to write files under
   */
  public abstract void downloadConfig(String configName, Path dir) throws IOException;

  /**
   * Download a file from config If the file does not exist, it returns null
   *
   * @param configName the name of the config
   * @param filePath the file to download with '/' as the separator
   * @return the content of the file
   */
  public abstract byte[] downloadFileFromConfig(String configName, String filePath)
      throws IOException;

  /**
   * Copy a config
   *
   * @param fromConfig the config to copy from
   * @param toConfig the config to copy to
   */
  public abstract void copyConfig(String fromConfig, String toConfig) throws IOException;

  /**
   * Check whether a config exists
   *
   * @param configName the config to check if it exists
   * @return whether the config exists or not
   */
  public abstract boolean checkConfigExists(String configName) throws IOException;

  /**
   * Delete a config (recursively deletes its files if not empty)
   *
   * @param configName the config to delete
   */
  public abstract void deleteConfig(String configName) throws IOException;

  /**
   * Delete files in config
   *
   * @param configName the name of the config
   * @param filesToDelete a list of file paths to delete using '/' as file path separator
   */
  public abstract void deleteFilesFromConfig(String configName, List<String> filesToDelete)
      throws IOException;

  /**
   * Set the config metadata If config does not exist, it will be created and set metadata on it
   * Else metadata will be replaced with the provided metadata
   *
   * @param configName the config name
   * @param data the metadata to be set on config
   */
  public abstract void setConfigMetadata(String configName, Map<String, Object> data)
      throws IOException;

  /**
   * Get the config metadata (mutable, non-null)
   *
   * @param configName the config name
   * @return the config metadata
   */
  public abstract Map<String, Object> getConfigMetadata(String configName) throws IOException;

  /**
   * List the names of configs (non-null)
   *
   * @return list of config names
   */
  public abstract List<String> listConfigs() throws IOException;

  /**
   * Get the names of the files in config including dirs (mutable, non-null) sorted
   * lexicographically e.g. solrconfig.xml, lang/, lang/stopwords_en.txt
   *
   * @param configName the config name
   * @return list of file name paths in the config with '/' uses as file path separators
   */
  public abstract List<String> getAllConfigFiles(String configName) throws IOException;

  public interface ConfigResource {

    ConfigNode get() throws Exception;
  }
}
