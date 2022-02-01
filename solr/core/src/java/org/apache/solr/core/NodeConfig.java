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

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.ModuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;


public class NodeConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // all Path fields here are absolute and normalized.

  private final String nodeName;

  private final Path coreRootDirectory;

  private final Path solrDataHome;

  private final Integer booleanQueryMaxClauseCount;
  
  private final Path configSetBaseDirectory;

  private final Set<Path> allowPaths;

  private final List<String> allowUrls;

  private final String sharedLibDirectory;

  private final String modules;

  private final PluginInfo shardHandlerFactoryConfig;

  private final UpdateShardHandlerConfig updateShardHandlerConfig;

  private final String configSetServiceClass;

  private final String coreAdminHandlerClass;

  private final String collectionsAdminHandlerClass;

  private final String healthCheckHandlerClass;

  private final String infoHandlerClass;

  private final String configSetsHandlerClass;

  private final LogWatcherConfig logWatcherConfig;

  private final CloudConfig cloudConfig;

  private final Integer coreLoadThreads;

  private final int replayUpdatesThreads;

  @Deprecated
  // This should be part of the transientCacheConfig, remove in 7.0
  private final int transientCacheSize;

  private final boolean useSchemaCache;

  private final String managementPath;

  private final PluginInfo[] backupRepositoryPlugins;

  private final MetricsConfig metricsConfig;

  private final PluginInfo transientCacheConfig;

  private final PluginInfo tracerConfig;

  // Track if this config was loaded from zookeeper so that we can skip validating the zookeeper connection later
  // If it becomes necessary to track multiple potential sources in the future, replace this with an Enum
  private final boolean fromZookeeper;
  private final String defaultZkHost;


  private NodeConfig(String nodeName, Path coreRootDirectory, Path solrDataHome, Integer booleanQueryMaxClauseCount,
                     Path configSetBaseDirectory, String sharedLibDirectory,
                     PluginInfo shardHandlerFactoryConfig, UpdateShardHandlerConfig updateShardHandlerConfig,
                     String coreAdminHandlerClass, String collectionsAdminHandlerClass,
                     String healthCheckHandlerClass, String infoHandlerClass, String configSetsHandlerClass,
                     LogWatcherConfig logWatcherConfig, CloudConfig cloudConfig, Integer coreLoadThreads, int replayUpdatesThreads,
                     int transientCacheSize, boolean useSchemaCache, String managementPath,
                     Path solrHome, SolrResourceLoader loader,
                     Properties solrProperties, PluginInfo[] backupRepositoryPlugins,
                     MetricsConfig metricsConfig, PluginInfo transientCacheConfig, PluginInfo tracerConfig,
                     boolean fromZookeeper, String defaultZkHost, Set<Path> allowPaths, List<String> allowUrls,
                     String configSetServiceClass, String modules) {
    // all Path params here are absolute and normalized.
    this.nodeName = nodeName;
    this.coreRootDirectory = coreRootDirectory;
    this.solrDataHome = solrDataHome;
    this.booleanQueryMaxClauseCount = booleanQueryMaxClauseCount;
    this.configSetBaseDirectory = configSetBaseDirectory;
    this.sharedLibDirectory = sharedLibDirectory;
    this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
    this.updateShardHandlerConfig = updateShardHandlerConfig;
    this.coreAdminHandlerClass = coreAdminHandlerClass;
    this.collectionsAdminHandlerClass = collectionsAdminHandlerClass;
    this.healthCheckHandlerClass = healthCheckHandlerClass;
    this.infoHandlerClass = infoHandlerClass;
    this.configSetsHandlerClass = configSetsHandlerClass;
    this.logWatcherConfig = logWatcherConfig;
    this.cloudConfig = cloudConfig;
    this.coreLoadThreads = coreLoadThreads;
    this.replayUpdatesThreads = replayUpdatesThreads;
    this.transientCacheSize = transientCacheSize;
    this.useSchemaCache = useSchemaCache;
    this.managementPath = managementPath;
    this.solrHome = solrHome;
    this.loader = loader;
    this.solrProperties = solrProperties;
    this.backupRepositoryPlugins = backupRepositoryPlugins;
    this.metricsConfig = metricsConfig;
    this.transientCacheConfig = transientCacheConfig;
    this.tracerConfig = tracerConfig;
    this.fromZookeeper = fromZookeeper;
    this.defaultZkHost = defaultZkHost;
    this.allowPaths = allowPaths;
    this.allowUrls = allowUrls;
    this.configSetServiceClass = configSetServiceClass;
    this.modules = modules;

    if (this.cloudConfig != null && this.getCoreLoadThreadCount(false) < 2) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 for coreLoadThreads (configured value = " + this.coreLoadThreads + ")");
    }
    if (null == this.solrHome) throw new NullPointerException("solrHome");
    if (null == this.loader) throw new NullPointerException("loader");

    setupSharedLib();
    initModules();
  }

  /**
   * Get the NodeConfig whether stored on disk, in ZooKeeper, etc.
   * This may also be used by custom filters to load relevant configuration.
   * @return the NodeConfig
   */
  public static NodeConfig loadNodeConfig(Path solrHome, Properties nodeProperties) {
    if (!StringUtils.isEmpty(System.getProperty("solr.solrxml.location"))) {
      log.warn("Solr property solr.solrxml.location is no longer supported. Will automatically load solr.xml from ZooKeeper if it exists");
    }
    nodeProperties = SolrXmlConfig.wrapAndSetZkHostFromSysPropIfNeeded(nodeProperties);
    String zkHost = nodeProperties.getProperty(SolrXmlConfig.ZK_HOST);
    if (!StringUtils.isEmpty(zkHost)) {
      int startUpZkTimeOut = Integer.getInteger("waitForZk", 30);
      startUpZkTimeOut *= 1000;
      try (SolrZkClient zkClient = new SolrZkClient(zkHost, startUpZkTimeOut, startUpZkTimeOut)) {
        if (zkClient.exists("/solr.xml", true)) {
          log.info("solr.xml found in ZooKeeper. Loading...");
          byte[] data = zkClient.getData("/solr.xml", null, null, true);
          return SolrXmlConfig.fromInputStream(solrHome, new ByteArrayInputStream(data), nodeProperties, true);
        }
      } catch (Exception e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Error occurred while loading solr.xml from zookeeper", e);
      }
      log.info("Loading solr.xml from SolrHome (not found in ZooKeeper)");
    }

    return SolrXmlConfig.fromSolrHome(solrHome, nodeProperties);
  }

  public String getConfigSetServiceClass() {
    return this.configSetServiceClass;
  }

  public String getNodeName() {
    return nodeName;
  }

  /** Absolute. */
  public Path getCoreRootDirectory() {
    return coreRootDirectory;
  }

  /** Absolute. */
  public Path getSolrDataHome() {
    return solrDataHome;
  }

  /**
   * Obtain the path of solr's binary installation directory, e.g. <code>/opt/solr</code>
   * @return path to install dir
   * @throws SolrException if property 'solr.install.dir' has not been initialized
   */
  public Path getSolrInstallDir() {
    String prop = System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
    if (prop == null || prop.isBlank()) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "solr.install.dir property not initialized");
    }
    return Paths.get(prop);
  }

  /**
   * If null, the lucene default will not be overridden
   *
   * @see IndexSearcher#setMaxClauseCount
   */
  public Integer getBooleanQueryMaxClauseCount() {
    return booleanQueryMaxClauseCount;
  }
  
  public PluginInfo getShardHandlerFactoryPluginInfo() {
    return shardHandlerFactoryConfig;
  }

  public UpdateShardHandlerConfig getUpdateShardHandlerConfig() {
    return updateShardHandlerConfig;
  }

  public int getCoreLoadThreadCount(boolean zkAware) {
    return coreLoadThreads == null ?
        (zkAware ? NodeConfigBuilder.DEFAULT_CORE_LOAD_THREADS_IN_CLOUD : NodeConfigBuilder.DEFAULT_CORE_LOAD_THREADS)
        : coreLoadThreads;
  }

  public int getReplayUpdatesThreads() {
    return replayUpdatesThreads;
  }

  /**
   * Returns a directory, optionally a comma separated list of directories
   * that will be added to Solr's class path for searching for classes and plugins.
   * The path is either absolute or relative to SOLR_HOME. Note that SOLR_HOME/lib
   * will always be added to the search path even if not included in this list.
   * @return a comma separated list of path strings or null if no paths defined
   */
  public String getSharedLibDirectory() {
    return sharedLibDirectory;
  }

  public String getCoreAdminHandlerClass() {
    return coreAdminHandlerClass;
  }
  
  public String getCollectionsHandlerClass() {
    return collectionsAdminHandlerClass;
  }

  public String getHealthCheckHandlerClass() {
    return healthCheckHandlerClass;
  }

  public String getInfoHandlerClass() {
    return infoHandlerClass;
  }

  public String getConfigSetsHandlerClass() {
    return configSetsHandlerClass;
  }

  public boolean hasSchemaCache() {
    return useSchemaCache;
  }

  public String getManagementPath() {
    return managementPath;
  }

  /** Absolute. */
  public Path getConfigSetBaseDirectory() {
    return configSetBaseDirectory;
  }

  public LogWatcherConfig getLogWatcherConfig() {
    return logWatcherConfig;
  }

  public CloudConfig getCloudConfig() {
    return cloudConfig;
  }

  public int getTransientCacheSize() {
    return transientCacheSize;
  }

  protected final Path solrHome;
  protected final SolrResourceLoader loader;
  protected final Properties solrProperties;

  public Properties getSolrProperties() {
    return solrProperties;
  }

  public Path getSolrHome() {
    return solrHome;
  }

  public SolrResourceLoader getSolrResourceLoader() {
    return loader;
  }

  public PluginInfo[] getBackupRepositoryPlugins() {
    return backupRepositoryPlugins;
  }

  public MetricsConfig getMetricsConfig() {
    return metricsConfig;
  }

  public PluginInfo getTransientCachePluginInfo() { return transientCacheConfig; }

  public PluginInfo getTracerConfiguratorPluginInfo() {
    return tracerConfig;
  }

  /** 
   * True if this node config was loaded from zookeeper
   * @see #getDefaultZkHost
   */
  public boolean isFromZookeeper() {
    return fromZookeeper;
  }
  
  /** 
   * This method returns the default "zkHost" value for this node -- either read from the system properties, 
   * or from the "extra" properties configured explicitly on the SolrDispatchFilter; or null if not specified.
   *
   * This is the value that would have been used when attempting locate the solr.xml in ZooKeeper (regardless of wether
   * the file was actaully loaded from ZK or from local disk)
   * 
   * (This value should only be used for "accounting" purposes to track where the node config came from if 
   * it <em>was</em> loaded from zk -- ie: to check if the chroot has already been applied.
   * It may be different from the "zkHost" <em>configured</em> in the "cloud" section of the solr.xml,
   * which should be used for all zk connections made by this node to participate in the cluster)
   *
   * @see #isFromZookeeper
   * @see #getCloudConfig()
   * @see CloudConfig#getZkHost()
   */
  public String getDefaultZkHost() {
    return defaultZkHost;
  }

  /**
   * Extra file paths that will be allowed for core creation, in addition to
   * SOLR_HOME, SOLR_DATA_HOME and coreRootDir
   */
  public Set<Path> getAllowPaths() { return allowPaths; }

  /**
   * Allow-list of Solr nodes URLs.
   */
  public List<String> getAllowUrls() {
    return allowUrls;
  }

  // Configures SOLR_HOME/lib to the shared class loader
  private void setupSharedLib() {
    // Always add $SOLR_HOME/lib to the shared resource loader
    Set<String> libDirs = new LinkedHashSet<>();
    libDirs.add("lib");

    if (!StringUtils.isBlank(getSharedLibDirectory())) {
      List<String> sharedLibs = Arrays.asList(getSharedLibDirectory().split("\\s*,\\s*"));
      libDirs.addAll(sharedLibs);
    }

    addFoldersToSharedLib(libDirs);
  }

  /**
   * Returns the modules as configured in solr.xml. Comma separated list. May be null if not defined
   */
  public String getModules() {
    return modules;
  }

  // Finds every jar in each folder and adds it to shardLib, then reloads Lucene SPI
  private void addFoldersToSharedLib(Set<String> libDirs) {
    boolean modified = false;
    // add the sharedLib to the shared resource loader before initializing cfg based plugins
    for (String libDir : libDirs) {
      Path libPath = getSolrHome().resolve(libDir);
      if (Files.exists(libPath)) {
        try {
          loader.addToClassLoader(SolrResourceLoader.getURLs(libPath));
          modified = true;
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Couldn't load libs: " + e, e);
        }
      }
    }
    if (modified) {
      loader.reloadLuceneSPI();
    }
  }

  // Adds modules to shared classpath
  private void initModules() {
    var moduleNames = ModuleUtils.resolveModulesFromStringOrSyspropOrEnv(getModules());
    boolean modified = false;
    for (String m : moduleNames) {
      if (!ModuleUtils.moduleExists(getSolrInstallDir(), m)) {
        log.error("No module with name {}, available modules are {}", m, ModuleUtils.listAvailableModules(getSolrInstallDir()));
        // Fail-fast if user requests a non-existing module
        throw new SolrException(ErrorCode.SERVER_ERROR, "No module with name " + m);
      }
      Path moduleLibPath = ModuleUtils.getModuleLibPath(getSolrInstallDir(), m);
      if (Files.exists(moduleLibPath)) {
        try {
          List<URL> urls = SolrResourceLoader.getURLs(moduleLibPath);
          loader.addToClassLoader(urls);
          if (log.isInfoEnabled()) {
            log.info("Added module {}. libPath={} with {} libs", m, moduleLibPath, urls.size());
          }
          if (log.isDebugEnabled()) {
            log.debug("Libs loaded from {}: {}", moduleLibPath, urls);
          }
          modified = true;
        } catch (IOException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, "Couldn't load libs for module " + m + ": " + e, e);
        }
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "Module lib folder " + moduleLibPath + " not found.");
      }
    }
    if (modified) {
      loader.reloadLuceneSPI();
    }
  }

  public static class NodeConfigBuilder {
    // all Path fields here are absolute and normalized.
    private SolrResourceLoader loader;
    private Path coreRootDirectory;
    private Path solrDataHome;
    private Integer booleanQueryMaxClauseCount;
    private Path configSetBaseDirectory;
    private String sharedLibDirectory;
    private String modules;
    private PluginInfo shardHandlerFactoryConfig;
    private UpdateShardHandlerConfig updateShardHandlerConfig = UpdateShardHandlerConfig.DEFAULT;
    private String configSetServiceClass;
    private String coreAdminHandlerClass = DEFAULT_ADMINHANDLERCLASS;
    private String collectionsAdminHandlerClass = DEFAULT_COLLECTIONSHANDLERCLASS;
    private String healthCheckHandlerClass = DEFAULT_HEALTHCHECKHANDLERCLASS;
    private String infoHandlerClass = DEFAULT_INFOHANDLERCLASS;
    private String configSetsHandlerClass = DEFAULT_CONFIGSETSHANDLERCLASS;
    private LogWatcherConfig logWatcherConfig = new LogWatcherConfig(true, null, null, 50);
    private CloudConfig cloudConfig;
    private int coreLoadThreads = DEFAULT_CORE_LOAD_THREADS;
    private int replayUpdatesThreads = Runtime.getRuntime().availableProcessors();
    @Deprecated
    //Remove in 7.0 and put it all in the transientCache element in solrconfig.xml
    private int transientCacheSize = DEFAULT_TRANSIENT_CACHE_SIZE;
    private boolean useSchemaCache = false;
    private String managementPath;
    private Properties solrProperties = new Properties();
    private PluginInfo[] backupRepositoryPlugins;
    private MetricsConfig metricsConfig;
    private PluginInfo transientCacheConfig;
    private PluginInfo tracerConfig;
    private boolean fromZookeeper = false;
    private String defaultZkHost;
    private Set<Path> allowPaths = Collections.emptySet();
    private List<String> allowUrls = Collections.emptyList();

    private final Path solrHome;
    private final String nodeName;

    public static final int DEFAULT_CORE_LOAD_THREADS = 3;
    //No:of core load threads in cloud mode is set to a default of 8
    public static final int DEFAULT_CORE_LOAD_THREADS_IN_CLOUD = 8;

    public static final int DEFAULT_TRANSIENT_CACHE_SIZE = Integer.MAX_VALUE;

    private static final String DEFAULT_ADMINHANDLERCLASS = "org.apache.solr.handler.admin.CoreAdminHandler";
    private static final String DEFAULT_INFOHANDLERCLASS = "org.apache.solr.handler.admin.InfoHandler";
    private static final String DEFAULT_COLLECTIONSHANDLERCLASS = "org.apache.solr.handler.admin.CollectionsHandler";
    private static final String DEFAULT_HEALTHCHECKHANDLERCLASS = "org.apache.solr.handler.admin.HealthCheckHandler";
    private static final String DEFAULT_CONFIGSETSHANDLERCLASS = "org.apache.solr.handler.admin.ConfigSetsHandler";

    public static final Set<String> DEFAULT_HIDDEN_SYS_PROPS = new HashSet<>(Arrays.asList(
        "javax.net.ssl.keyStorePassword",
        "javax.net.ssl.trustStorePassword",
        "basicauth",
        "zkDigestPassword",
        "zkDigestReadonlyPassword",
        "aws.secretKey", // AWS SDK v1
        "aws.secretAccessKey", // AWS SDK v2
        "http.proxyPassword"
    ));

    public NodeConfigBuilder(String nodeName, Path solrHome) {
      this.nodeName = nodeName;
      this.solrHome = solrHome.toAbsolutePath();
      this.coreRootDirectory = solrHome;
      // always init from sysprop because <solrDataHome> config element may be missing
      setSolrDataHome(System.getProperty(SolrXmlConfig.SOLR_DATA_HOME));
      setConfigSetBaseDirectory("configsets");
      this.metricsConfig = new MetricsConfig.MetricsConfigBuilder().build();
    }

    public NodeConfigBuilder setCoreRootDirectory(String coreRootDirectory) {
      this.coreRootDirectory = solrHome.resolve(coreRootDirectory).normalize();
      return this;
    }

    public NodeConfigBuilder setSolrDataHome(String solrDataHomeString) {
      // keep it null unless explicitly set to non-empty value
      if (solrDataHomeString != null && !solrDataHomeString.isEmpty()) {
        this.solrDataHome = solrHome.resolve(solrDataHomeString).normalize();
      }
      return this;
    }
    
    public NodeConfigBuilder setBooleanQueryMaxClauseCount(Integer booleanQueryMaxClauseCount) {
      this.booleanQueryMaxClauseCount = booleanQueryMaxClauseCount;
      return this;
    }

    public NodeConfigBuilder setConfigSetBaseDirectory(String configSetBaseDirectory) {
      this.configSetBaseDirectory = solrHome.resolve(configSetBaseDirectory);
      return this;
    }

    public NodeConfigBuilder setSharedLibDirectory(String sharedLibDirectory) {
      this.sharedLibDirectory = sharedLibDirectory;
      return this;
    }

    public NodeConfigBuilder setShardHandlerFactoryConfig(PluginInfo shardHandlerFactoryConfig) {
      this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
      return this;
    }

    public NodeConfigBuilder setUpdateShardHandlerConfig(UpdateShardHandlerConfig updateShardHandlerConfig) {
      this.updateShardHandlerConfig = updateShardHandlerConfig;
      return this;
    }

    public NodeConfigBuilder setCoreAdminHandlerClass(String coreAdminHandlerClass) {
      this.coreAdminHandlerClass = coreAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setCollectionsAdminHandlerClass(String collectionsAdminHandlerClass) {
      this.collectionsAdminHandlerClass = collectionsAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setHealthCheckHandlerClass(String healthCheckHandlerClass) {
      this.healthCheckHandlerClass = healthCheckHandlerClass;
      return this;
    }

    public NodeConfigBuilder setInfoHandlerClass(String infoHandlerClass) {
      this.infoHandlerClass = infoHandlerClass;
      return this;
    }

    public NodeConfigBuilder setConfigSetsHandlerClass(String configSetsHandlerClass) {
      this.configSetsHandlerClass = configSetsHandlerClass;
      return this;
    }

    public NodeConfigBuilder setLogWatcherConfig(LogWatcherConfig logWatcherConfig) {
      this.logWatcherConfig = logWatcherConfig;
      return this;
    }

    public NodeConfigBuilder setCloudConfig(CloudConfig cloudConfig) {
      this.cloudConfig = cloudConfig;
      return this;
    }

    public NodeConfigBuilder setCoreLoadThreads(int coreLoadThreads) {
      this.coreLoadThreads = coreLoadThreads;
      return this;
    }

    public NodeConfigBuilder setReplayUpdatesThreads(int replayUpdatesThreads) {
      this.replayUpdatesThreads = replayUpdatesThreads;
      return this;
    }

    // Remove in Solr 7.0
    @Deprecated
    public NodeConfigBuilder setTransientCacheSize(int transientCacheSize) {
      this.transientCacheSize = transientCacheSize;
      return this;
    }

    public NodeConfigBuilder setUseSchemaCache(boolean useSchemaCache) {
      this.useSchemaCache = useSchemaCache;
      return this;
    }

    public NodeConfigBuilder setManagementPath(String managementPath) {
      this.managementPath = managementPath;
      return this;
    }

    public NodeConfigBuilder setSolrProperties(Properties solrProperties) {
      this.solrProperties = solrProperties;
      return this;
    }

    public NodeConfigBuilder setBackupRepositoryPlugins(PluginInfo[] backupRepositoryPlugins) {
      this.backupRepositoryPlugins = backupRepositoryPlugins;
      return this;
    }

    public NodeConfigBuilder setMetricsConfig(MetricsConfig metricsConfig) {
      this.metricsConfig = metricsConfig;
      return this;
    }
    
    public NodeConfigBuilder setSolrCoreCacheFactoryConfig(PluginInfo transientCacheConfig) {
      this.transientCacheConfig = transientCacheConfig;
      return this;
    }

    public NodeConfigBuilder setTracerConfig(PluginInfo tracerConfig) {
      this.tracerConfig = tracerConfig;
      return this;
    }

    public NodeConfigBuilder setFromZookeeper(boolean fromZookeeper) {
      this.fromZookeeper = fromZookeeper;
      return this;
    }
    
    public NodeConfigBuilder setDefaultZkHost(String defaultZkHost) {
      this.defaultZkHost = defaultZkHost;
      return this;
    }

    public NodeConfigBuilder setAllowPaths(Set<Path> paths) {
      this.allowPaths = paths;
      return this;
    }

    public NodeConfigBuilder setAllowUrls(List<String> urls) {
      this.allowUrls = urls;
      return this;
    }

    public NodeConfigBuilder setConfigSetServiceClass(String configSetServiceClass){
      this.configSetServiceClass = configSetServiceClass;
      return this;
    }

    /**
     * Set list of modules to add to class path
     * @param moduleNames comma separated list of module names to add to class loader, e.g. "extracting,ltr,langid"
     */
    public NodeConfigBuilder setModules(String moduleNames) {
      this.modules = moduleNames;
      return this;
    }

    public NodeConfig build() {
      // if some things weren't set then set them now.  Simple primitives are set on the field declaration
      if (loader == null) {
        loader = new SolrResourceLoader(solrHome);
      }
      return new NodeConfig(
              nodeName, coreRootDirectory, solrDataHome, booleanQueryMaxClauseCount,
              configSetBaseDirectory, sharedLibDirectory, shardHandlerFactoryConfig,
              updateShardHandlerConfig, coreAdminHandlerClass, collectionsAdminHandlerClass,
              healthCheckHandlerClass, infoHandlerClass, configSetsHandlerClass,
              logWatcherConfig, cloudConfig, coreLoadThreads, replayUpdatesThreads,
              transientCacheSize, useSchemaCache, managementPath,
              solrHome, loader, solrProperties,
              backupRepositoryPlugins, metricsConfig, transientCacheConfig, tracerConfig,
              fromZookeeper, defaultZkHost, allowPaths, allowUrls, configSetServiceClass,
              modules);
    }

    public NodeConfigBuilder setSolrResourceLoader(SolrResourceLoader resourceLoader) {
      this.loader = resourceLoader;
      return this;
    }
  }
}

