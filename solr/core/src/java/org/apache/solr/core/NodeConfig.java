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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.logging.DeprecationLog;
import org.apache.solr.logging.LogWatcherConfig;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.servlet.SolrDispatchFilter;
import org.apache.solr.update.UpdateShardHandlerConfig;
import org.apache.solr.util.ModuleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NodeConfig {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // all Path fields here are absolute and normalized.

  private final String nodeName;

  private final Path coreRootDirectory;
  private final String coresLocatorClass;

  private final Path solrDataHome;

  private final Integer booleanQueryMaxClauseCount;

  private final Path configSetBaseDirectory;

  private final Set<Path> allowPaths;

  private final List<String> allowUrls;

  private final boolean hideStackTraces;

  private final String sharedLibDirectory;

  private final String modules;

  private final Set<String> hiddenSysProps;
  private final Predicate<String> hiddenSysPropPattern;

  private final PluginInfo shardHandlerFactoryConfig;
  private final UpdateShardHandlerConfig updateShardHandlerConfig;
  private final PluginInfo replicaPlacementFactoryConfig;

  private final String configSetServiceClass;

  private final String coreAdminHandlerClass;

  private final Map<String, String> coreAdminHandlerActions;

  private final String collectionsAdminHandlerClass;

  private final String healthCheckHandlerClass;

  private final String infoHandlerClass;

  private final String configSetsHandlerClass;

  private final LogWatcherConfig logWatcherConfig;

  private final CloudConfig cloudConfig;

  private final Integer coreLoadThreads;

  private final int replayUpdatesThreads;

  @Deprecated private final int transientCacheSize;

  private final boolean useSchemaCache;

  private final String managementPath;

  private final PluginInfo[] backupRepositoryPlugins;

  private final MetricsConfig metricsConfig;

  private final Map<String, CacheConfig> cachesConfig;

  private final PluginInfo tracerConfig;

  // Track if this config was loaded from zookeeper so that we can skip validating the zookeeper
  // connection later. If it becomes necessary to track multiple potential sources in the future,
  // replace this with an Enum
  private final boolean fromZookeeper;
  private final String defaultZkHost;

  private NodeConfig(
      String nodeName,
      Path coreRootDirectory,
      String coresLocatorClass,
      Path solrDataHome,
      Integer booleanQueryMaxClauseCount,
      Path configSetBaseDirectory,
      String sharedLibDirectory,
      PluginInfo shardHandlerFactoryConfig,
      UpdateShardHandlerConfig updateShardHandlerConfig,
      PluginInfo replicaPlacementFactoryConfig,
      String coreAdminHandlerClass,
      Map<String, String> coreAdminHandlerActions,
      String collectionsAdminHandlerClass,
      String healthCheckHandlerClass,
      String infoHandlerClass,
      String configSetsHandlerClass,
      LogWatcherConfig logWatcherConfig,
      CloudConfig cloudConfig,
      Integer coreLoadThreads,
      int replayUpdatesThreads,
      int transientCacheSize,
      boolean useSchemaCache,
      String managementPath,
      Path solrHome,
      SolrResourceLoader loader,
      Properties solrProperties,
      PluginInfo[] backupRepositoryPlugins,
      MetricsConfig metricsConfig,
      Map<String, CacheConfig> cachesConfig,
      PluginInfo tracerConfig,
      boolean fromZookeeper,
      String defaultZkHost,
      Set<Path> allowPaths,
      List<String> allowUrls,
      boolean hideStackTraces,
      String configSetServiceClass,
      String modules,
      Set<String> hiddenSysProps) {
    // all Path params here are absolute and normalized.
    this.nodeName = nodeName;
    this.coreRootDirectory = coreRootDirectory;
    this.coresLocatorClass = coresLocatorClass;
    this.solrDataHome = solrDataHome;
    this.booleanQueryMaxClauseCount = booleanQueryMaxClauseCount;
    this.configSetBaseDirectory = configSetBaseDirectory;
    this.sharedLibDirectory = sharedLibDirectory;
    this.shardHandlerFactoryConfig = shardHandlerFactoryConfig;
    this.updateShardHandlerConfig = updateShardHandlerConfig;
    this.replicaPlacementFactoryConfig = replicaPlacementFactoryConfig;
    this.coreAdminHandlerClass = coreAdminHandlerClass;
    this.coreAdminHandlerActions = coreAdminHandlerActions;
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
    this.cachesConfig = cachesConfig == null ? Collections.emptyMap() : cachesConfig;
    this.tracerConfig = tracerConfig;
    this.fromZookeeper = fromZookeeper;
    this.defaultZkHost = defaultZkHost;
    this.allowPaths = allowPaths;
    this.allowUrls = allowUrls;
    this.hideStackTraces = hideStackTraces;
    this.configSetServiceClass = configSetServiceClass;
    this.modules = modules;
    this.hiddenSysProps = hiddenSysProps;
    this.hiddenSysPropPattern =
        Pattern.compile("^(" + String.join("|", hiddenSysProps) + ")$", Pattern.CASE_INSENSITIVE)
            .asMatchPredicate();

    if (this.cloudConfig != null && this.getCoreLoadThreadCount(false) < 2) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "SolrCloud requires a value of at least 2 for coreLoadThreads (configured value = "
              + this.coreLoadThreads
              + ")");
    }
    if (null == this.solrHome) throw new NullPointerException("solrHome");
    if (null == this.loader) throw new NullPointerException("loader");

    setupSharedLib();
    initModules();
  }

  /**
   * Get the NodeConfig whether stored on disk, in ZooKeeper, etc. This may also be used by custom
   * filters to load relevant configuration.
   *
   * @return the NodeConfig
   */
  public static NodeConfig loadNodeConfig(Path solrHome, Properties nodeProperties) {
    if (StrUtils.isNotNullOrEmpty(System.getProperty("solr.solrxml.location"))) {
      log.warn(
          "Solr property solr.solrxml.location is no longer supported. Will automatically load solr.xml from ZooKeeper if it exists");
    }
    final SolrResourceLoader loader = new SolrResourceLoader(solrHome);
    initModules(loader, null);
    nodeProperties = SolrXmlConfig.wrapAndSetZkHostFromSysPropIfNeeded(nodeProperties);
    String zkHost = nodeProperties.getProperty(SolrXmlConfig.ZK_HOST);
    if (StrUtils.isNotNullOrEmpty(zkHost)) {
      int startUpZkTimeOut = Integer.getInteger("waitForZk", 30);
      startUpZkTimeOut *= 1000;
      try (SolrZkClient zkClient =
          new SolrZkClient.Builder()
              .withUrl(zkHost)
              .withTimeout(startUpZkTimeOut, TimeUnit.MILLISECONDS)
              .withConnTimeOut(startUpZkTimeOut, TimeUnit.MILLISECONDS)
              .withSolrClassLoader(loader)
              .build()) {
        if (zkClient.exists("/solr.xml", true)) {
          log.info("solr.xml found in ZooKeeper. Loading...");
          DeprecationLog.log(
              "solrxml-zookeeper",
              "Loading solr.xml from zookeeper is deprecated. See reference guide for details.");
          byte[] data = zkClient.getData("/solr.xml", null, null, true);
          return SolrXmlConfig.fromInputStream(
              solrHome, new ByteArrayInputStream(data), nodeProperties, true);
        }
      } catch (Exception e) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Error occurred while loading solr.xml from zookeeper", e);
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

  public String getCoresLocatorClass() {
    return this.coresLocatorClass;
  }

  /** Absolute. */
  public Path getSolrDataHome() {
    return solrDataHome;
  }

  /**
   * Obtain the path of solr's binary installation directory, e.g. <code>/opt/solr</code>
   *
   * @return path to install dir or null if solr.install.dir not set.
   */
  public static Path getSolrInstallDir() {
    String prop = System.getProperty(SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
    if (prop == null || prop.isBlank()) {
      log.debug("solr.install.dir property not initialized.");
      return null;
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

  public PluginInfo getReplicaPlacementFactoryConfig() {
    return replicaPlacementFactoryConfig;
  }

  public int getCoreLoadThreadCount(boolean zkAware) {
    return coreLoadThreads == null
        ? (zkAware
            ? NodeConfigBuilder.DEFAULT_CORE_LOAD_THREADS_IN_CLOUD
            : NodeConfigBuilder.DEFAULT_CORE_LOAD_THREADS)
        : coreLoadThreads;
  }

  public int getReplayUpdatesThreads() {
    return replayUpdatesThreads;
  }

  /**
   * Returns a directory, optionally a comma separated list of directories that will be added to
   * Solr's class path for searching for classes and plugins. The path is either absolute or
   * relative to SOLR_HOME. Note that SOLR_HOME/lib will always be added to the search path even if
   * not included in this list.
   *
   * @return a comma separated list of path strings or null if no paths defined
   */
  public String getSharedLibDirectory() {
    return sharedLibDirectory;
  }

  public String getCoreAdminHandlerClass() {
    return coreAdminHandlerClass;
  }

  public Map<String, String> getCoreAdminHandlerActions() {
    return coreAdminHandlerActions;
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

  public Map<String, CacheConfig> getCachesConfig() {
    return cachesConfig;
  }

  public PluginInfo getTracerConfiguratorPluginInfo() {
    return tracerConfig;
  }

  /**
   * True if this node config was loaded from zookeeper
   *
   * @see #getDefaultZkHost
   */
  public boolean isFromZookeeper() {
    return fromZookeeper;
  }

  /**
   * This method returns the default "zkHost" value for this node -- either read from the system
   * properties, or from the "extra" properties configured explicitly on the SolrDispatchFilter; or
   * null if not specified.
   *
   * <p>This is the value that would have been used when attempting to locate the solr.xml in
   * ZooKeeper (regardless of whether the file was actually loaded from ZK or from local disk)
   *
   * <p>(This value should only be used for "accounting" purposes to track where the node config
   * came from if it <em>was</em> loaded from zk -- ie: to check if the chroot has already been
   * applied. It may be different from the "zkHost" <em>configured</em> in the "cloud" section of
   * the solr.xml, which should be used for all zk connections made by this node to participate in
   * the cluster)
   *
   * @see #isFromZookeeper
   * @see #getCloudConfig()
   * @see CloudConfig#getZkHost()
   */
  public String getDefaultZkHost() {
    return defaultZkHost;
  }

  /**
   * Extra file paths that will be allowed for core creation, in addition to SOLR_HOME,
   * SOLR_DATA_HOME and coreRootDir
   */
  public Set<Path> getAllowPaths() {
    return allowPaths;
  }

  /** Allow-list of Solr nodes URLs. */
  public List<String> getAllowUrls() {
    return allowUrls;
  }

  public boolean hideStackTraces() {
    return hideStackTraces;
  }

  // Configures SOLR_HOME/lib to the shared class loader
  private void setupSharedLib() {
    // Always add $SOLR_HOME/lib to the shared resource loader
    Set<String> libDirs = new LinkedHashSet<>();
    libDirs.add("lib");

    Path solrInstallDir = getSolrInstallDir();
    if (solrInstallDir == null) {
      log.warn(
          "Unable to add $SOLR_HOME/lib for shared lib since {} was not set.",
          SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE);
    } else {
      // Always add $SOLR_TIP/lib to the shared resource loader
      libDirs.add(solrInstallDir.resolve("lib").toAbsolutePath().normalize().toString());
    }

    if (StrUtils.isNotBlank(getSharedLibDirectory())) {
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

  /** Returns the list of hidden system properties. The list values are regex expressions */
  public Set<String> getHiddenSysProps() {
    return hiddenSysProps;
  }

  /** Returns whether a given system property is hidden */
  public boolean isSysPropHidden(String sysPropName) {
    return hiddenSysPropPattern.test(sysPropName);
  }

  public static final String REDACTED_SYS_PROP_VALUE = "--REDACTED--";

  /** Returns the a system property value, or "--REDACTED--" if the system property is hidden */
  public String getRedactedSysPropValue(String sysPropName) {
    return hiddenSysPropPattern.test(sysPropName)
        ? REDACTED_SYS_PROP_VALUE
        : System.getProperty(sysPropName);
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
    initModules(loader, getModules());
  }

  // can't we move this to ModuleUtils?
  public static void initModules(SolrResourceLoader loader, String modules) {
    var moduleNames = ModuleUtils.resolveModulesFromStringOrSyspropOrEnv(modules);
    boolean modified = false;

    Path solrInstallDir = getSolrInstallDir();
    if (solrInstallDir == null) {
      if (!moduleNames.isEmpty()) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Unable to setup modules "
                + moduleNames
                + " because "
                + SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE
                + " was not set.");
      }
      return;
    }
    for (String m : moduleNames) {
      if (!ModuleUtils.moduleExists(solrInstallDir, m)) {
        log.error(
            "No module with name {}, available modules are {}",
            m,
            ModuleUtils.listAvailableModules(solrInstallDir));
        // Fail-fast if user requests a non-existing module
        throw new SolrException(ErrorCode.SERVER_ERROR, "No module with name " + m);
      }
      Path moduleLibPath = ModuleUtils.getModuleLibPath(solrInstallDir, m);
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
          throw new SolrException(
              ErrorCode.SERVER_ERROR, "Couldn't load libs for module " + m + ": " + e, e);
        }
      } else {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Module lib folder " + moduleLibPath + " not found.");
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
    private String coresLocatorClass = DEFAULT_CORESLOCATORCLASS;
    private Path solrDataHome;
    private Integer booleanQueryMaxClauseCount;
    private Path configSetBaseDirectory;
    private String sharedLibDirectory;
    private String modules;
    private String hiddenSysProps;
    private PluginInfo shardHandlerFactoryConfig;
    private UpdateShardHandlerConfig updateShardHandlerConfig = UpdateShardHandlerConfig.DEFAULT;
    private PluginInfo replicaPlacementFactoryConfig;
    private String configSetServiceClass;
    private String coreAdminHandlerClass = DEFAULT_ADMINHANDLERCLASS;
    private Map<String, String> coreAdminHandlerActions = Collections.emptyMap();
    private String collectionsAdminHandlerClass = DEFAULT_COLLECTIONSHANDLERCLASS;
    private String healthCheckHandlerClass = DEFAULT_HEALTHCHECKHANDLERCLASS;
    private String infoHandlerClass = DEFAULT_INFOHANDLERCLASS;
    private String configSetsHandlerClass = DEFAULT_CONFIGSETSHANDLERCLASS;
    private LogWatcherConfig logWatcherConfig = new LogWatcherConfig(true, null, null, 50);
    private CloudConfig cloudConfig;
    private int coreLoadThreads = DEFAULT_CORE_LOAD_THREADS;
    private int replayUpdatesThreads = Runtime.getRuntime().availableProcessors();
    @Deprecated private int transientCacheSize = -1;
    private boolean useSchemaCache = false;
    private String managementPath;
    private Properties solrProperties = new Properties();
    private PluginInfo[] backupRepositoryPlugins;
    private MetricsConfig metricsConfig;
    private Map<String, CacheConfig> cachesConfig;
    private PluginInfo tracerConfig;
    private boolean fromZookeeper = false;
    private String defaultZkHost;
    private Set<Path> allowPaths = Collections.emptySet();
    private List<String> allowUrls = Collections.emptyList();
    private boolean hideStackTrace = Boolean.getBoolean("solr.hideStackTrace");

    private final Path solrHome;
    private final String nodeName;

    public static final int DEFAULT_CORE_LOAD_THREADS = 3;
    // No:of core load threads in cloud mode is set to a default of 8
    public static final int DEFAULT_CORE_LOAD_THREADS_IN_CLOUD = 8;

    private static final String DEFAULT_CORESLOCATORCLASS =
        "org.apache.solr.core.CorePropertiesLocator";
    private static final String DEFAULT_ADMINHANDLERCLASS =
        "org.apache.solr.handler.admin.CoreAdminHandler";
    private static final String DEFAULT_INFOHANDLERCLASS =
        "org.apache.solr.handler.admin.InfoHandler";
    private static final String DEFAULT_COLLECTIONSHANDLERCLASS =
        "org.apache.solr.handler.admin.CollectionsHandler";
    private static final String DEFAULT_HEALTHCHECKHANDLERCLASS =
        "org.apache.solr.handler.admin.HealthCheckHandler";
    private static final String DEFAULT_CONFIGSETSHANDLERCLASS =
        "org.apache.solr.handler.admin.ConfigSetsHandler";

    public static final Set<String> DEFAULT_HIDDEN_SYS_PROPS =
        Set.of(
            "javax\\.net\\.ssl\\.keyStorePassword",
            "javax\\.net\\.ssl\\.trustStorePassword",
            "basicauth",
            "zkDigestPassword",
            "zkDigestReadonlyPassword",
            "aws\\.secretKey", // AWS SDK v1
            "aws\\.secretAccessKey", // AWS SDK v2
            "http\\.proxyPassword",
            ".*password.*",
            ".*secret.*");

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

    public NodeConfigBuilder setCoresLocatorClass(String coresLocatorClass) {
      this.coresLocatorClass = coresLocatorClass;
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

    public NodeConfigBuilder setUpdateShardHandlerConfig(
        UpdateShardHandlerConfig updateShardHandlerConfig) {
      this.updateShardHandlerConfig = updateShardHandlerConfig;
      return this;
    }

    public NodeConfigBuilder setReplicaPlacementFactoryConfig(
        PluginInfo replicaPlacementFactoryConfig) {
      this.replicaPlacementFactoryConfig = replicaPlacementFactoryConfig;
      return this;
    }

    public NodeConfigBuilder setCoreAdminHandlerClass(String coreAdminHandlerClass) {
      this.coreAdminHandlerClass = coreAdminHandlerClass;
      return this;
    }

    public NodeConfigBuilder setCoreAdminHandlerActions(
        Map<String, String> coreAdminHandlerActions) {
      this.coreAdminHandlerActions = coreAdminHandlerActions;
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

    // Remove in Solr 10.0

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

    public NodeConfigBuilder setCachesConfig(Map<String, CacheConfig> cachesConfig) {
      this.cachesConfig = cachesConfig;
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

    public NodeConfigBuilder setHideStackTrace(boolean hide) {
      this.hideStackTrace = hide;
      return this;
    }

    public NodeConfigBuilder setConfigSetServiceClass(String configSetServiceClass) {
      this.configSetServiceClass = configSetServiceClass;
      return this;
    }

    /**
     * Set list of modules to add to class path
     *
     * @param moduleNames comma separated list of module names to add to class loader, e.g.
     *     "extracting,ltr,langid"
     */
    public NodeConfigBuilder setModules(String moduleNames) {
      this.modules = moduleNames;
      return this;
    }

    public NodeConfigBuilder setHiddenSysProps(String hiddenSysProps) {
      this.hiddenSysProps = hiddenSysProps;
      return this;
    }

    /**
     * Finds list of hiddenSysProps requested by system property or environment variable or the
     * default
     *
     * @return set of raw hidden sysProps, may be regex
     */
    private Set<String> resolveHiddenSysPropsFromSysPropOrEnvOrDefault(String hiddenSysProps) {
      // Fall back to sysprop and env.var if nothing configured through solr.xml
      if (!StrUtils.isNotNullOrEmpty(hiddenSysProps)) {
        String fromProps = System.getProperty("solr.hiddenSysProps");
        // Back-compat for solr 9x
        // DEPRECATED: Remove in 10.0
        if (StrUtils.isNotNullOrEmpty(fromProps)) {
          fromProps = System.getProperty("solr.redaction.system.pattern");
        }
        String fromEnv = System.getenv("SOLR_HIDDEN_SYS_PROPS");
        if (StrUtils.isNotNullOrEmpty(fromProps)) {
          hiddenSysProps = fromProps;
        } else if (StrUtils.isNotNullOrEmpty(fromEnv)) {
          hiddenSysProps = fromEnv;
        }
      }
      Set<String> hiddenSysPropSet = Collections.emptySet();
      if (hiddenSysProps != null) {
        hiddenSysPropSet =
            StrUtils.splitSmart(hiddenSysProps, ',').stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toSet());
      }
      return hiddenSysPropSet.isEmpty() ? DEFAULT_HIDDEN_SYS_PROPS : hiddenSysPropSet;
    }

    public NodeConfig build() {
      // if some things weren't set then set them now.  Simple primitives are set on the field
      // declaration
      if (loader == null) {
        loader = new SolrResourceLoader(solrHome);
      }
      return new NodeConfig(
          nodeName,
          coreRootDirectory,
          coresLocatorClass,
          solrDataHome,
          booleanQueryMaxClauseCount,
          configSetBaseDirectory,
          sharedLibDirectory,
          shardHandlerFactoryConfig,
          updateShardHandlerConfig,
          replicaPlacementFactoryConfig,
          coreAdminHandlerClass,
          coreAdminHandlerActions,
          collectionsAdminHandlerClass,
          healthCheckHandlerClass,
          infoHandlerClass,
          configSetsHandlerClass,
          logWatcherConfig,
          cloudConfig,
          coreLoadThreads,
          replayUpdatesThreads,
          transientCacheSize,
          useSchemaCache,
          managementPath,
          solrHome,
          loader,
          solrProperties,
          backupRepositoryPlugins,
          metricsConfig,
          cachesConfig,
          tracerConfig,
          fromZookeeper,
          defaultZkHost,
          allowPaths,
          allowUrls,
          hideStackTrace,
          configSetServiceClass,
          modules,
          resolveHiddenSysPropsFromSysPropOrEnvOrDefault(hiddenSysProps));
    }

    public NodeConfigBuilder setSolrResourceLoader(SolrResourceLoader resourceLoader) {
      this.loader = resourceLoader;
      return this;
    }
  }
}
