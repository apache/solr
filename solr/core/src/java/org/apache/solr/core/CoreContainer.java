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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Maps;
import org.apache.commons.ForkJoinParWorkRootExec;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Lookup;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder;
import org.apache.solr.client.solrj.io.Lang;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.Overseer;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.ParWork;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CloseTracker;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.OrderedExecutor;
import org.apache.solr.common.util.StopWatch;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.filestore.PackageStoreAPI;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.HealthCheckHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocal;
import org.apache.solr.handler.admin.SecurityConfHandlerZk;
import org.apache.solr.handler.admin.ZookeeperInfoHandler;
import org.apache.solr.handler.admin.ZookeeperReadAPI;
import org.apache.solr.handler.admin.ZookeeperStatusHandler;
import org.apache.solr.handler.component.SearchComponentStd;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.sql.CalciteSolrDriver;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.PackageLoader;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.search.SolrFieldCacheBean;
import org.apache.solr.security.AuditLoggerPlugin;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.security.SecurityPluginHolder;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.RefCounted;
import org.apache.zookeeper.KeeperException;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;
import static org.apache.solr.common.params.CommonParams.AUTHC_PATH;
import static org.apache.solr.common.params.CommonParams.AUTHZ_PATH;
import static org.apache.solr.common.params.CommonParams.COLLECTIONS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CONFIGSETS_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.CORES_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.INFO_HANDLER_PATH;
import static org.apache.solr.common.params.CommonParams.METRICS_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_PATH;
import static org.apache.solr.common.params.CommonParams.ZK_STATUS_PATH;
import static org.apache.solr.security.AuthenticationPlugin.AUTHENTICATION_PLUGIN_PROP;

/**
 * @since solr 1.3
 */
public class CoreContainer implements Closeable {

  private static final Pattern COMPILE = Pattern.compile("\\s*,\\s*");

  static {
    System.setProperty("java.util.concurrent.ForkJoinPool.common.threadFactory",
        ForkJoinParWorkRootExec.RootExecHolder.SolrForkJoinThreadFactory.class.getName());
  }

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static final Logger deprecationLog = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass().getName() + ".Deprecation");

  static {
    log.info("init expensive Lang static {} {} {} {}", Lang.class, CodecUtil.class, Caffeine.class, SearchComponentStd.standard_components);
  }

  final SolrCores solrCores = new SolrCores(this);
  private final SolrZkClient zkClient;

  private volatile boolean startedLoadingCores;
  private SolrFieldCacheBean fieldCacheBean;


  ExecutorService coreAdminExecutor = ParWork.getParExecutorService("coreAdminExecutor", 0, 8, 1000);

  public ExecutorService getCoreAdminExecutor() {
    return coreAdminExecutor;
  }

  public boolean isLoaded() {
    return loaded;
  }

  private volatile boolean loaded;

  public boolean isCoreLoading(String cname) {
    return solrCores.isCoreLoading(cname);
  }

  public static class CoreLoadFailure {

    public final CoreDescriptor cd;
    public final Exception exception;

    public CoreLoadFailure(CoreDescriptor cd, Exception loadFailure) {
      this.cd = new CoreDescriptor(cd.getName(), cd);
      exception = loadFailure;
    }

    @Override public String toString() {
      return "CoreLoadFailure{" + "cd=" + cd + ", exception=" + this.exception + '}';
    }
  }

  protected final Map<String, CoreLoadFailure> coreInitFailures = new NonBlockingHashMap<>();

  protected volatile CoreAdminHandler coreAdminHandler = null;
  protected volatile CollectionsHandler collectionsHandler = null;
  protected volatile HealthCheckHandler healthCheckHandler = null;

  private volatile InfoHandler infoHandler;
  protected volatile ConfigSetsHandler configSetsHandler = null;

  private volatile PKIAuthenticationPlugin pkiAuthenticationPlugin;

  protected volatile Properties containerProperties;

  private CloseTracker closeTracker;

  private volatile ConfigSetService coreConfigService;

  protected volatile ZkContainer zkSys = null;
  protected volatile ShardHandlerFactory shardHandlerFactory;

  private volatile UpdateShardHandler updateShardHandler;

  public final ExecutorService solrCoreExecutor = ParWork.getExecutorService("coreContainerExecutor", 64, false);

  public final ExecutorService solrCoreCloseExecutor = ParWork.getExecutorService("coreContainerCloseExecutor", 64, false);

  public final ExecutorService coreContainerExecutor = ParWork.getExecutorService("coreContainerExecutor", 32, false);

  {
//    for (int i = 0; i < 12; i++) {
//      solrCoreExecutor.prestartCoreThread();
//    }
//
//    for (int i = 0; i < 4; i++) {
//      coreContainerExecutor.prestartCoreThread();
//    }

  }

  private volatile OrderedExecutor replayUpdatesExecutor;

  @SuppressWarnings({"rawtypes"})
  protected volatile LogWatcher logging = null;

  protected final NodeConfig cfg;
  protected SolrResourceLoader loader;

  protected volatile Path solrHome;

  protected final CoresLocator coresLocator;

  private volatile String hostName;

  private final PluginBag<SolrRequestHandler> containerHandlers = new PluginBag<>(SolrRequestHandler.class, null);

  private volatile boolean asyncSolrCoreLoad = true;

  protected volatile SecurityConfHandler securityConfHandler;

  private volatile SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin;

  private volatile SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin;

  private volatile SecurityPluginHolder<AuditLoggerPlugin> auditloggerPlugin;

  private volatile BackupRepositoryFactory backupRepoFactory;

  protected volatile SolrMetricManager metricManager;

  protected volatile String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);

  protected volatile SolrMetricsContext solrMetricsContext;

  protected volatile MetricsHandler metricsHandler;

  private volatile SolrClientCache solrClientCache;

  private final ObjectCache objectCache = new ObjectCache();

  private PackageStoreAPI packageStoreAPI;
  private PackageLoader packageLoader;

  // Bits for the state variable.
  public final static long LOAD_COMPLETE = 0x1L;
  public final static long CORE_DISCOVERY_COMPLETE = 0x2L;
  public final static long INITIAL_CORE_LOAD_COMPLETE = 0x4L;
  private volatile long status = 0L;

  private enum CoreInitFailedAction {fromleader, none}

  /**
   * This method instantiates a new instance of {@linkplain BackupRepository}.
   *
   * @param repositoryName The name of the backup repository (Optional).
   *                       If not specified, a default implementation is used.
   * @return a new instance of {@linkplain BackupRepository}.
   */
  public BackupRepository newBackupRepository(Optional<String> repositoryName) {
    BackupRepository repository;
    if (repositoryName.isPresent()) {
      repository = backupRepoFactory.newInstance(loader, repositoryName.get());
    } else {
      repository = backupRepoFactory.newInstance(loader);
    }
    return repository;
  }

  public SolrRequestHandler getRequestHandler(String path) {
    return RequestHandlerBase.getRequestHandler(path, containerHandlers);
  }

  public PluginBag<SolrRequestHandler> getRequestHandlers() {
    return this.containerHandlers;
  }

  {
    if (log.isDebugEnabled()) {
      log.debug("New CoreContainer {}", System.identityHashCode(this));
    }
  }

  /**
   * Create a new CoreContainer using the given solr home directory.  The container's
   * cores are not loaded.
   *
   * @param solrHome a String containing the path to the solr home directory
   * @param properties substitutable properties (alternative to Sys props)
   * @see #load()
   */
  public CoreContainer(Path solrHome, Properties properties) throws IOException {
    this(SolrXmlConfig.fromSolrHome(solrHome, properties));
  }

  public CoreContainer(Path solrHome, Properties properties, boolean asyncSolrCoreLoad) throws IOException {
    this(SolrXmlConfig.fromSolrHome(solrHome, properties), asyncSolrCoreLoad);
  }


  /**
   * Create a new CoreContainer using the given SolrResourceLoader,
   * configuration and CoresLocator.  The container's cores are
   * not loaded.
   *
   * @param config a ConfigSolr representation of this container's configuration
   * @see #load()
   */
  public CoreContainer(NodeConfig config) {
    this(config, new CorePropertiesLocator(config.getCoreRootDirectory()));
  }

  public CoreContainer(NodeConfig config, boolean asyncSolrCoreLoad) {
    this(config, new CorePropertiesLocator(config.getCoreRootDirectory()), asyncSolrCoreLoad);
  }

  public CoreContainer(NodeConfig config, CoresLocator locator) {
    this(config, locator, true);
  }

  public CoreContainer(NodeConfig config, CoresLocator locator, boolean asyncSolrCoreLoad) {
    this(null, config, locator, asyncSolrCoreLoad);
  }

  public CoreContainer(SolrZkClient zkClient, NodeConfig config, CoresLocator locator, boolean asyncSolrCoreLoad) {
    this.cfg = requireNonNull(config);
    this.zkClient = zkClient;
    this.coresLocator = locator;
    this.asyncSolrCoreLoad = asyncSolrCoreLoad;

    assert ObjectReleaseTracker.getInstance().track(this);
    assert (closeTracker = new CloseTracker()) != null;
  }

  @SuppressWarnings({"unchecked"})
  private void initializeAuthorizationPlugin(Map<String, Object> authorizationConf) {
    authorizationConf = Utils.getDeepCopy(authorizationConf, 4);
    int newVersion = readVersion(authorizationConf);
    //Initialize the Authorization module
    SecurityPluginHolder<AuthorizationPlugin> old = authorizationPlugin;
    SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin = null;
    if (authorizationConf != null) {
      String klas = (String) authorizationConf.get("class");
      if (klas == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "class is required for authorization plugin");
      }
      if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
        log.debug("Authorization config not modified");
        return;
      }
      log.info("Initializing authorization plugin: {}", klas);
      authorizationPlugin = new SecurityPluginHolder<>(newVersion,
          loader.newInstance(klas, AuthorizationPlugin.class));

      // Read and pass the authorization context to the plugin
      authorizationPlugin.plugin.init(authorizationConf);
    } else {
      log.debug("Security conf doesn't exist. Skipping setup for authorization module.");
    }
    this.authorizationPlugin = authorizationPlugin;

    if (old != null) {
      try {
        old.plugin.close();
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        log.error("Exception while attempting to close old authorization plugin", e);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private void initializeAuditloggerPlugin(Map<String, Object> auditConf) {
    auditConf = Utils.getDeepCopy(auditConf, 4);
    int newVersion = readVersion(auditConf);
    //Initialize the Auditlog module
    SecurityPluginHolder<AuditLoggerPlugin> old = auditloggerPlugin;
    SecurityPluginHolder<AuditLoggerPlugin> newAuditloggerPlugin = null;
    if (auditConf != null) {
      String klas = (String) auditConf.get("class");
      if (klas == null) {
        throw new SolrException(ErrorCode.SERVER_ERROR, "class is required for auditlogger plugin");
      }
      if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
        log.debug("Auditlogger config not modified");
        return;
      }
      log.info("Initializing auditlogger plugin: {}", klas);
      newAuditloggerPlugin = new SecurityPluginHolder<>(newVersion,
          loader.newInstance(klas, AuditLoggerPlugin.class));

      newAuditloggerPlugin.plugin.init(auditConf);
      newAuditloggerPlugin.plugin.initializeMetrics(solrMetricsContext, "/auditlogging");
    } else {
      log.debug("Security conf doesn't exist. Skipping setup for audit logging module.");
    }
    this.auditloggerPlugin = newAuditloggerPlugin;
    if (old != null) {
      try {
        old.plugin.close();
      } catch (Exception e) {
        ParWork.propagateInterrupt(e);
        log.error("Exception while attempting to close old auditlogger plugin", e);
      }
    }
  }


  @SuppressWarnings({"unchecked"})
  private void initializeAuthenticationPlugin(Map<String, Object> authenticationConfig) {
    log.info("Initialize authentication plugin ..");
    authenticationConfig = Utils.getDeepCopy(authenticationConfig, 4);
    int newVersion = readVersion(authenticationConfig);
    String pluginClassName = null;
    if (authenticationConfig != null) {
      if (authenticationConfig.containsKey("class")) {
        pluginClassName = String.valueOf(authenticationConfig.get("class"));
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, "No 'class' specified for authentication in ZK.");
      }
    }

    if (pluginClassName != null) {
      log.debug("Authentication plugin class obtained from security.json: {}", pluginClassName);
    } else if (System.getProperty(AUTHENTICATION_PLUGIN_PROP) != null) {
      pluginClassName = System.getProperty(AUTHENTICATION_PLUGIN_PROP);
      log.debug("Authentication plugin class obtained from system property '{}': {}"
          , AUTHENTICATION_PLUGIN_PROP, pluginClassName);
    } else {
      log.debug("No authentication plugin used.");
    }
    SecurityPluginHolder<AuthenticationPlugin> old = authenticationPlugin;
    SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin = null;

    if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
      log.debug("Authentication config not modified");
      return;
    }

    // Initialize the plugin
    if (pluginClassName != null) {
      log.info("Initializing authentication plugin: {}", pluginClassName);
      authenticationPlugin = new SecurityPluginHolder<>(newVersion,
          loader.newInstance(pluginClassName,
              AuthenticationPlugin.class,
              null,
              new Class[]{CoreContainer.class},
              new Object[]{this}));
    }
    if (authenticationPlugin != null) {
      authenticationPlugin.plugin.init(authenticationConfig);
      setupHttpClientForAuthPlugin(authenticationPlugin.plugin);
      authenticationPlugin.plugin.initializeMetrics(solrMetricsContext, "/authentication");
    }
    this.authenticationPlugin = authenticationPlugin;
    try {
      if (old != null) old.plugin.close();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      log.error("Exception while attempting to close old authentication plugin", e);
    }

  }

  private void setupHttpClientForAuthPlugin(Object authcPlugin) {
    if (authcPlugin instanceof HttpClientBuilderPlugin) {
      // Setup HttpClient for internode communication
      HttpClientBuilderPlugin builderPlugin = ((HttpClientBuilderPlugin) authcPlugin);
      SolrHttpClientBuilder builder = builderPlugin.getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());
      shardHandlerFactory.setSecurityBuilder(builderPlugin);
      updateShardHandler.setSecurityBuilder(builderPlugin);

      // The default http client of the core container's shardHandlerFactory has already been created and
      // configured using the default httpclient configurer. We need to reconfigure it using the plugin's
      // http client configurer to set it up for internode communication.
      log.debug("Reconfiguring HttpClient settings.");

      if (builder != null) {
        SolrHttpClientContextBuilder httpClientBuilder = new SolrHttpClientContextBuilder();
        if (builder.getCredentialsProviderProvider() != null) {
          httpClientBuilder.setDefaultCredentialsProvider(new CredentialsProviderProvider(builder));
        }
        if (builder.getAuthSchemeRegistryProvider() != null) {
          httpClientBuilder.setAuthSchemeRegistryProvider(new AuthSchemeRegistryProvider(builder));
        }

        HttpClientUtil.setHttpClientRequestContextBuilder(httpClientBuilder);
      }
    }
    // Always register PKI auth interceptor, which will then delegate the decision of who should secure
    // each request to the configured authentication plugin.
    if (pkiAuthenticationPlugin != null && !pkiAuthenticationPlugin.isInterceptorRegistered()) {
      pkiAuthenticationPlugin.getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());
      shardHandlerFactory.setSecurityBuilder(pkiAuthenticationPlugin);
      updateShardHandler.setSecurityBuilder(pkiAuthenticationPlugin);
      // The overseer owns a separate internode client (overseerOnlyClient) used for collection admin
      // (e.g. /admin/cores during CREATE). On node startup the overseer election wins before this
      // plugin is created, so Overseer.start() could not PKI-sign that client. Wire it now that the
      // plugin exists; otherwise its requests go out unsigned and a blockUnknown auth plugin rejects
      // them with 401. Overseer.start() handles the reverse ordering (failover after PKI exists).
      if (isZooKeeperAware() && zkSys.getZkController() != null) {
        Overseer overseer = zkSys.getZkController().getOverseer();
        if (overseer != null && overseer.overseerOnlyClient != null) {
          pkiAuthenticationPlugin.setup(overseer.overseerOnlyClient);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  private static int readVersion(Map<String, Object> conf) {
    if (conf == null) return -1;
    Map meta = (Map) conf.get("");
    if (meta == null) return -1;
    Number v = (Number) meta.get("v");
    return v == null ? -1 : v.intValue();
  }

  /**
   * This method allows subclasses to construct a CoreContainer
   * without any default init behavior.
   *
   * @param testConstructor pass (Object)null.
   * @lucene.experimental
   */
  protected CoreContainer(Object testConstructor) {
    assert (closeTracker = new CloseTracker()) != null;
    solrHome = null;
    loader = null;
    coresLocator = null;
    cfg = null;
    containerProperties = null;
    zkClient = null;
    replayUpdatesExecutor = null;
  }


  public static CoreContainer createAndLoad(Path solrHome, Path configFile) throws IOException {
    return createAndLoad(solrHome, configFile, null);
  }
  /**
   * Create a new CoreContainer and load its cores
   *
   * @param solrHome   the solr home directory
   * @param configFile the file containing this container's configuration
   * @return a loaded CoreContainer
   */
  public static CoreContainer createAndLoad(Path solrHome, Path configFile, SolrZkClient zkClient) throws IOException {
    NodeConfig config = new SolrXmlConfig().fromFile(solrHome, configFile, new Properties());
    CoreContainer cc = new CoreContainer(zkClient, config, new CorePropertiesLocator(config.getCoreRootDirectory()), false);
    try {
      cc.load();
    } catch (Exception e) {
      ParWork.propagateInterrupt(e);
      cc.shutdown();
      throw e;
    }
    return cc;
  }

  public Properties getContainerProperties() {
    return containerProperties;
  }

  public PKIAuthenticationPlugin getPkiAuthenticationPlugin() {
    return pkiAuthenticationPlugin;
  }

  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  public MetricsHandler getMetricsHandler() {
    return metricsHandler;
  }

  public OrderedExecutor getReplayUpdatesExecutor() {
    return replayUpdatesExecutor;
  }

  public PackageLoader getPackageLoader() {
    return packageLoader;
  }

  public PackageStoreAPI getPackageStoreAPI() {
    return packageStoreAPI;
  }

  public SolrClientCache getSolrClientCache() {
    return solrClientCache;
  }

  public ObjectCache getObjectCache() {
    return objectCache;
  }

  //-------------------------------------------------------------------
  // Initialization / Cleanup
  //-------------------------------------------------------------------

  /**
   * Load the cores defined for this CoreContainer
   */
  public void load() {
    this.containerProperties = new Properties(cfg.getSolrProperties());

    this.loader = cfg.getSolrResourceLoader();

    this.solrHome = cfg.getSolrHome();

    boolean enableMetrics = Boolean.parseBoolean(System.getProperty("solr.enableMetrics", "true"));

    if (zkClient != null) {
      zkSys = new ZkContainer(zkClient);
      zkSys.initZooKeeper(this, cfg.getCloudConfig());
      MDCLoggingContext.setNode(zkSys.getZkController().getNodeName());
    }

    if (null != this.cfg.getBooleanQueryMaxClauseCount()) {
      IndexSearcher.setMaxClauseCount(this.cfg.getBooleanQueryMaxClauseCount());
    }

    int replayThreads = Integer.getInteger("solr.logReplayThreads", cfg.getReplayUpdatesThreads());
    this.replayUpdatesExecutor = new OrderedExecutor(cfg.getReplayUpdatesThreads(),
        ParWork.getParExecutorServiceCustomQueue("replayUpdatesExecutor", replayThreads, replayThreads,
            1000, new LinkedBlockingQueue<>(cfg.getReplayUpdatesThreads())));

    metricManager = new SolrMetricManager(loader, cfg.getMetricsConfig());
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    solrMetricsContext = new SolrMetricsContext(metricManager, registryName, metricTag, enableMetrics);

    try (ParWork work = new ParWork(this)) {

      // The PublicKeyHandler must always be created: in cloud mode the PKIAuthenticationPlugin
      // (created below when ZooKeeper-aware) signs internode requests with this handler's key pair.
      // Gating it behind a system property left PKIAuthenticationPlugin with a null handler, which
      // NPE'd in generateToken on every internode request (e.g. core creation). Keypair-generation
      // cost in tests is mitigated by PublicKeyHandler.REUSABLE_KEYPAIR.
      work.collect("", () -> {
        try {
          containerHandlers.put(PublicKeyHandler.PATH, new PublicKeyHandler(cfg.getCloudConfig()));
        } catch (IOException | InvalidKeySpecException e) {
          throw new RuntimeException("Bad PublicKeyHandler configuration.", e);
        }
      });

      work.collect("",() -> {
        updateShardHandler = new UpdateShardHandler(cfg.getUpdateShardHandlerConfig());
        updateShardHandler.initializeMetrics(solrMetricsContext, "updateShardHandler");
      });

      work.addCollect();

      work.collect("",() -> {
        shardHandlerFactory = ShardHandlerFactory.newInstance(cfg.getShardHandlerFactoryPluginInfo(),
            loader, updateShardHandler);
        if (shardHandlerFactory instanceof SolrMetricProducer) {
          SolrMetricProducer metricProducer = (SolrMetricProducer) shardHandlerFactory;
          metricProducer.initializeMetrics(solrMetricsContext, "httpShardHandler");
        }
      });
    }

    coreConfigService = ConfigSetService.createConfigSetService(cfg, loader, zkSys == null ? null : zkSys.zkController);

    containerProperties.putAll(cfg.getSolrProperties());

    if (isZooKeeperAware()) {
      MDCLoggingContext.setNode(zkSys.getZkController().getNodeName());
    }

    long start = System.nanoTime();
    if (log.isDebugEnabled()) {
      log.debug("Loading cores into CoreContainer [instanceDir={}]", getSolrHome());
    }

    if (loaded) {
      throw new IllegalStateException("CoreContainer already loaded");
    }

    loaded = true;

    List<CoreDescriptor> cds = coresLocator.discover(this);

    // Only enforce unique discovered core names in standalone mode. In SolrCloud (e.g.
    // BaseDistributedSearchTestCase-based tests like TestHighlightDedupGrouping), the same core
    // name can legitimately appear across nodes that share a solr home during test setup.
    if (!isZooKeeperAware()) {
      checkForDuplicateCoreNames(cds);
    }

    solrCores.load(loader);

    if (isZooKeeperAware()) {
      try {
        zkSys.start(this);
      } catch (IOException | KeeperException e) {
        throw new SolrException(ErrorCode.SERVER_ERROR, e);
      }
    }

    if (isZooKeeperAware()) {
      List<CoreDescriptor> removeCds = new ArrayList<>();
      for (final CoreDescriptor cd : cds) {

        DocCollection docCollection = getZkController().getClusterState().getCollectionOrNull(cd.getCollectionName());
        if (docCollection != null) {
          Replica replica = docCollection.getReplica(cd.getName());
          if (replica != null && !getZkController().getBaseUrl().equals(replica.getBaseUrl())
              || Long.parseLong(cd.getCoreProperty("collId", "0")) != docCollection.getId()) {
            removeCds.add(cd);
            log.error("IllegalStateException wrong base url or collection id for this node in replica entry replica={}", replica);

            Path instanceDir = cd.getInstanceDir();
            IOUtils.deleteDirectory(instanceDir);
            continue;
          }
        }
        if (!removeCds.contains(cd)) {
          solrCores.putDescriptor(cd);
          String collection = cd.getCollectionName();
          try {
            getZkController().getZkStateReader().registerCore(collection, cd.getName());
          } catch (Exception e) {
            log.error("Failed registering core with zkstatereader", e);
          }
        }
      }
      if (cds.size() > 0) {
        try {
          getZkController().publishNodeAs(getZkController().getNodeName(), OverseerAction.RECOVERYNODE);
        } catch (Exception e) {
          log.error("Failed publishing loading core as recovering", e);
        }

        // RECOVERYNODE is processed asynchronously by the overseer. If we create our live node
        // before the state change is visible, observers see live + stale ACTIVE/LEADER replica
        // state from before the restart and read from cores that are still loading/recovering
        // (e.g. waitForState(active) passes while the local index is empty and the ulog is
        // buffering). Wait (bounded) until none of our replicas appear ACTIVE before going live.
        String ourNodeName = getZkController().getNodeName();
        Set<String> ourCollections = new HashSet<>();
        for (CoreDescriptor cd : cds) {
          if (!removeCds.contains(cd) && cd.getCollectionName() != null) {
            ourCollections.add(cd.getCollectionName());
          }
        }
        for (String collection : ourCollections) {
          try {
            getZkController().getZkStateReader().waitForState(collection, 10, TimeUnit.SECONDS, (liveNodes, docColl) -> {
              if (docColl == null) {
                // State not loaded into the local reader yet — keep waiting; the watcher fires
                // when it arrives. The bounded timeout covers collections deleted while we were down.
                return false;
              }
              for (Replica r : docColl.getReplicas()) {
                if (ourNodeName.equals(r.getNodeName()) && r.getState() == Replica.State.ACTIVE) {
                  return false;
                }
              }
              return true;
            });
          } catch (InterruptedException e) {
            ParWork.propagateInterrupt(e);
            break;
          } catch (TimeoutException e) {
            log.warn("Timeout waiting for our replicas to be marked recovering before joining live nodes collection={}", collection);
          }
        }
      }

      zkSys.getZkController().createEphemeralLiveNode();

      for (CoreDescriptor removeCd : removeCds) {
        cds.remove(removeCd);
      }
    } else {
      for (final CoreDescriptor cd : cds) {
        solrCores.putDescriptor(cd);
      }
    }

    try {
      // Always add $SOLR_HOME/lib to the shared resource loader
      Set<String> libDirs = new LinkedHashSet<>();
      libDirs.add("lib");

      if (!StringUtils.isBlank(cfg.getSharedLibDirectory())) {
        List<String> sharedLibs = Arrays.asList(COMPILE.split(cfg.getSharedLibDirectory()));
        libDirs.addAll(sharedLibs);
      }

      boolean modified = false;
      // add the sharedLib to the shared resource loader before initializing cfg based plugins
      for (String libDir : libDirs) {
        Path libPath = Paths.get(getSolrHome()).resolve(libDir);
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

      packageStoreAPI = new PackageStoreAPI(this);
      containerHandlers.getApiBag().registerObject(packageStoreAPI.readAPI);
      containerHandlers.getApiBag().registerObject(packageStoreAPI.writeAPI);

      logging = LogWatcher.newRegisteredLogWatcher(cfg.getLogWatcherConfig(), loader);

      hostName = cfg.getNodeName();

      collectionsHandler = createHandler(COLLECTIONS_HANDLER_PATH, cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
      infoHandler = createHandler(INFO_HANDLER_PATH, cfg.getInfoHandlerClass(), InfoHandler.class);
      coreAdminHandler = createHandler(CORES_HANDLER_PATH, cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);
      configSetsHandler = createHandler(CONFIGSETS_HANDLER_PATH, cfg.getConfigSetsHandlerClass(), ConfigSetsHandler.class);

      createHandler(ZK_PATH, ZookeeperInfoHandler.class.getName(), ZookeeperInfoHandler.class);
      createHandler(ZK_STATUS_PATH, ZookeeperStatusHandler.class.getName(), ZookeeperStatusHandler.class);

      try (ParWork work = new ParWork(this, false)) {

        if (enableMetrics) {
          work.collect("", () -> {
            metricsHandler = new MetricsHandler(this);
            containerHandlers.put(METRICS_PATH, metricsHandler);
            metricsHandler.initializeMetrics(solrMetricsContext, METRICS_PATH);
          });

          work.addCollect();
        }

        work.collect("", () -> {
          securityConfHandler = isZooKeeperAware() ? new SecurityConfHandlerZk(this) : new SecurityConfHandlerLocal(this);
          securityConfHandler.initializeMetrics(solrMetricsContext, AUTHZ_PATH);
          securityConfHandler.initializeMetrics(solrMetricsContext, AUTHC_PATH);
          containerHandlers.put(AUTHC_PATH, securityConfHandler);
          containerHandlers.put(AUTHZ_PATH, securityConfHandler);
          this.backupRepoFactory = new BackupRepositoryFactory(cfg.getBackupRepositoryPlugins());
        });

        if (isZooKeeperAware()) {
          work.collect("", () -> {

            pkiAuthenticationPlugin = new PKIAuthenticationPlugin(this, zkSys.getZkController().getNodeName(), (PublicKeyHandler) containerHandlers.get(PublicKeyHandler.PATH));
            // use deprecated API for back-compat, remove in 9.0
            pkiAuthenticationPlugin.initializeMetrics(solrMetricsContext, "/authentication/pki");
            TracerConfigurator.loadTracer(loader, cfg.getTracerConfiguratorPluginInfo(), getZkController().getZkStateReader());
            packageLoader = new PackageLoader(this);
            containerHandlers.getApiBag().registerObject(packageLoader.getPackageAPI().editAPI);
            containerHandlers.getApiBag().registerObject(packageLoader.getPackageAPI().readAPI);
            ZookeeperReadAPI zookeeperReadAPI = new ZookeeperReadAPI(this);
            containerHandlers.getApiBag().registerObject(zookeeperReadAPI);

          });
        }

        work.addCollect();


        work.collect("", () -> {
          reloadSecurityProperties();
          warnUsersOfInsecureSettings();
        });

        work.collect("", () -> {
          solrClientCache = new SolrClientCache(isZooKeeperAware() ? zkSys.getZkController().getZkStateReader() : null, updateShardHandler.getTheSharedHttpClient());
          // initialize CalciteSolrDriver instance to use this solrClientCache
          CalciteSolrDriver.INSTANCE.setSolrClientCache(solrClientCache);
        });

      }

      if (!containerHandlers.keySet().contains(CORES_HANDLER_PATH)) {
        throw new IllegalStateException("No core admin path was loaded " + CORES_HANDLER_PATH);
      }
      // initialize gauges for reporting the number of cores and disk total/free

      // nocommit terrible perf issue getting the sizes this way
      solrMetricsContext.gauge(() -> solrCores.getCores().size(), true, "loaded", SolrInfoBean.Category.CONTAINER.toString(), "cores");
      solrMetricsContext.gauge(() -> solrCores.getLoadedCoreNames().size() - solrCores.getCores().size(), true, "lazy", SolrInfoBean.Category.CONTAINER.toString(), "cores");
      solrMetricsContext.gauge(() -> solrCores.getAllCoreNames().size() - solrCores.getLoadedCoreNames().size(), true, "unloaded", SolrInfoBean.Category.CONTAINER.toString(), "cores");
      Path dataHome = cfg.getSolrDataHome() != null ? cfg.getSolrDataHome() : cfg.getCoreRootDirectory();
      solrMetricsContext.gauge(() -> dataHome.toFile().getTotalSpace(), true, "totalSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs");
      solrMetricsContext.gauge(() -> dataHome.toFile().getUsableSpace(), true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs");
      solrMetricsContext.gauge(() -> dataHome.toAbsolutePath().toString(), true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs");
      solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toFile().getTotalSpace(), true, "totalSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
      solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toFile().getUsableSpace(), true, "usableSpace", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
      solrMetricsContext.gauge(() -> cfg.getCoreRootDirectory().toAbsolutePath().toString(), true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs", "coreRoot");
      // add version information
      solrMetricsContext.gauge(() -> this.getClass().getPackage().getSpecificationVersion(), true, "specification", SolrInfoBean.Category.CONTAINER.toString(), "version");
      solrMetricsContext.gauge(() -> this.getClass().getPackage().getImplementationVersion(), true, "implementation", SolrInfoBean.Category.CONTAINER.toString(), "version");

      fieldCacheBean = new SolrFieldCacheBean();
      fieldCacheBean.initializeMetrics(solrMetricsContext, null);

      if (isZooKeeperAware()) {
        // cloud-specific init
      }

    } catch (Exception e) {
      log.error("Exception in CoreContainer load", e);
      for (final CoreDescriptor cd : cds) {
        solrCores.removeCoreDescriptor(cd);
      }
      // Removing the core descriptors alone leaves updateShardHandler/shardHandlerFactory/metricManager/
      // zkSys initialized but unclosed -- and zkSys may have already created a live ephemeral node in ZK.
      // A direct load() caller (embedded/test) would otherwise be handed a half-init container that
      // advertises a live node but hosts no cores. Tear the container back down before rethrowing.
      try {
        shutdown();
      } catch (Exception ce) {
        log.error("Exception shutting down CoreContainer after failed load", ce);
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Exception in CoreContainer load", e);
    }

    if (isZooKeeperAware()) {
      cds = CoreSorter.sortCores(this, cds);
    }

    List<Future<SolrCore>> coreLoadFutures = new ArrayList<>(cds.size());
    for (final CoreDescriptor cd : cds) {

      if (log.isDebugEnabled()) log.debug("Process core descriptor {} {}", cd.getName(), cd.isLoadOnStartup());
      if (!cd.isLoadOnStartup()) {
        solrCores.addCoreDescriptor(cd);
      }


      if (cd.isLoadOnStartup()) {
        coreLoadFutures.add(solrCoreExecutor.submit(() -> {
          SolrCore core = null;
          MDCLoggingContext.setCoreName(cd.getName());
          try {
            core = createFromDescriptor(cd, false, false);
          } catch (AlreadyClosedException e){
            log.warn("Will not finish creating and registering core={} because we are shutting down", cd.getName());
          } catch (Exception e){
            log.error("Error creating and register core {}", cd.getName(), e);
            solrCores.removeCoreDescriptor(cd);
            throw e;
          } finally {
            MDCLoggingContext.clear();
          }
          return core;
        }));
      }
    }

    if (!asyncSolrCoreLoad || !isZooKeeperAware()) {
      for (Future<SolrCore> future : coreLoadFutures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          ParWork.propagateInterrupt(e);
        } catch (ExecutionException e) {
          log.error("Error waiting for SolrCore to be loaded on startup", e.getCause());
        }
      }
    }

    if (isZooKeeperAware()) {

      // zkSys.getZkController().checkOverseerDesignate();
      // initialize this handler here when SolrCloudManager is ready
    }
    // This is a bit redundant but these are two distinct concepts for all they're accomplished at the same time.
   // status |= LOAD_COMPLETE | INITIAL_CORE_LOAD_COMPLETE;
    log.info("load took {}ms", TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS));
  }

  public void securityNodeChanged() {
    log.info("Security node changed, reloading security.json");
    reloadSecurityProperties();
  }

  /**
   * Make sure securityConfHandler is initialized
   */
  @SuppressWarnings({"unchecked"})
  private void reloadSecurityProperties() {
    if (securityConfHandler == null) {
      return;
    }

    SecurityConfHandler.SecurityConfig securityConfig = securityConfHandler.getSecurityConfig(true);
    initializeAuthorizationPlugin((Map<String, Object>) securityConfig.getData().get("authorization"));
    initializeAuthenticationPlugin((Map<String, Object>) securityConfig.getData().get("authentication"));
    initializeAuditloggerPlugin((Map<String, Object>) securityConfig.getData().get("auditlogging"));
  }

  private void warnUsersOfInsecureSettings() {
    if (authenticationPlugin == null || authorizationPlugin == null) {
      log.warn("Not all security plugins configured!  authentication={} authorization={}.  Solr is only as secure as " +
              "you make it. Consider configuring authentication/authorization before exposing Solr to users internal or " +
              "external.  See https://s.apache.org/solrsecurity for more info",
          (authenticationPlugin != null) ? "enabled" : "disabled",
          (authorizationPlugin != null) ? "enabled" : "disabled");
    }

    if (authenticationPlugin !=null && StringUtils.isNotEmpty(System.getProperty("solr.jetty.https.port"))) {
      log.warn("Solr authentication is enabled, but SSL is off.  Consider enabling SSL to protect user credentials and data with encryption.");
    }
  }

  private static void checkForDuplicateCoreNames(List<CoreDescriptor> cds) {
    Map<String, Path> addedCores = Maps.newHashMap();
    for (CoreDescriptor cd : cds) {
      final String name = cd.getName();
      if (addedCores.containsKey(name))
        throw new SolrException(ErrorCode.SERVER_ERROR,
            String.format(Locale.ROOT, "Found multiple cores with the name [%s], with instancedirs [%s] and [%s]",
                name, addedCores.get(name), cd.getInstanceDir()));
      addedCores.put(name, cd.getInstanceDir());
    }
  }

  private final ReentrantLock shutdownLock = new ReentrantLock();

  private volatile boolean isShutDown = false;

  public boolean isShutDown() {
    return isShutDown;
  }

  @Override
  public void close() throws IOException {
    if (closeTracker != null) closeTracker.close();

    isShutDown = true;

    if (solrCores != null) {
      solrCores.closing();
    }

    log.info("Shutting down CoreContainer instance={}", System.identityHashCode(this));

    if (isZooKeeperAware() && zkSys != null && zkSys.getZkController() != null && !zkSys.getZkController().isDcCalled()) {
      zkSys.zkController.disconnect(true);
    }

    // must do before isShutDown=true
    if (isZooKeeperAware()) {
      try {
        cancelCoreRecoveries(true, true);
      } catch (Exception e) {

        ParWork.propagateInterrupt(e);
        log.error("Exception trying to cancel recoveries on shutdown", e);
      }
    }

//    CalciteSolrDriver.INSTANCE.setSolrClientCache(null);

    coreAdminExecutor.shutdown();

    IOUtils.closeQuietly(solrCores);

    solrCoreExecutor.shutdown();

    while (true) {
      try {
        solrCoreExecutor.awaitTermination(5, TimeUnit.SECONDS);
        break;
      } catch (InterruptedException e) {

      }
    }

    // SolrCores.close() (via closeQuietly(solrCores) above) submits the actual core.closeAndWait()
    // work to solrCoreCloseExecutor, NOT solrCoreExecutor. We must drain it here, before the ParWork
    // below closes zkSys (-> ZkController -> StatePublisher); otherwise an async core-close runs after
    // StatePublisher is gone and the replica's final state is published into a dead channel
    // (NPE/AlreadyClosed) and never lands.
    solrCoreCloseExecutor.shutdown();

    while (true) {
      try {
        if (solrCoreCloseExecutor.awaitTermination(5, TimeUnit.SECONDS)) break;
      } catch (InterruptedException e) {

      }
    }

    try (ParWork closer = new ParWork(this, true)) {

      closer.collect("replayUpdateExec", () -> {
        if (replayUpdatesExecutor != null) {
          replayUpdatesExecutor.shutdownAndAwaitTermination();
        }
      });

      List<Callable<?>> callables = new ArrayList<>();

      if (metricManager != null) {
        callables.add(() -> {
          metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node), metricTag);
          return metricManager.getClass().getName() + ":GA:NODE";
        });
        callables.add(() -> {
          metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm), metricTag);
          return metricManager.getClass().getName() + ":GA:JVM";
        });
        callables.add(() -> {
          metricManager.unregisterGauges(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty), metricTag);
          return metricManager.getClass().getName() + ":GA:JETTY";
        });
      }

      closer.collect("SolrCoreInternals", callables);

      callables = new ArrayList<>();

      if (coreAdminHandler != null) {
        callables.add(() -> {
          coreAdminHandler.shutdown();
          return coreAdminHandler;
        });
      }
      AuthorizationPlugin authPlugin = null;
      if (authorizationPlugin != null) {
        authPlugin = authorizationPlugin.plugin;
      }
      AuthenticationPlugin authenPlugin = null;
      if (authenticationPlugin != null) {
        authenPlugin = authenticationPlugin.plugin;
      }
      AuditLoggerPlugin auditPlugin = null;
      if (auditloggerPlugin != null) {
        auditPlugin = auditloggerPlugin.plugin;
      }
      closer.collect(authPlugin);
      closer.collect(authenPlugin);
      closer.collect(auditPlugin);
      closer.collect(callables);

      closer.addCollect();

      closer.collect(shardHandlerFactory);
      closer.collect(fieldCacheBean);
      closer.collect(solrClientCache);

      closer.collect(loader);
      closer.collect();
      closer.collect(updateShardHandler);
      closer.collect();
      closer.collect(zkSys);
    }

    log.info("CoreContainer closed");
    assert ObjectReleaseTracker.getInstance().release(this);
  }

  public void shutdown() {
    try {
      close();
    } catch (IOException e) {
      log.error("IOException during shutdown", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
  }

  public void waitForCoresToFinish() {
    solrCores.waitForLoadingCoresToFinish(30000);
  }

  public void cancelCoreRecoveries(boolean wait, boolean prepForClose) {

    Collection<SolrCore> cores = solrCores.getCores();

    // we must cancel without holding the cores sync
    // make sure we wait for any recoveries to stop
    try (ParWork work = new ParWork(this, true)) {
      for (SolrCore core : cores) {
        work.collect("cancelRecoveryFor-" + core.getName(), () -> {
          try {
            core.getSolrCoreState().cancelRecovery(wait, prepForClose);
          } catch (Exception e) {
            SolrException.log(log, "Error canceling recovery for core", e);
          }
        });
      }
    }
  }

  public CoresLocator getCoresLocator() {
    return coresLocator;
  }

  protected SolrCore registerCore(CoreDescriptor cd, SolrCore core, boolean closeOld) {

    log.debug("registerCore name={}", cd.getName());

    if (core == null) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Can not register a null core.");
    }

    if (isShutDown) {
      throw new AlreadyClosedException("Will not register SolrCore with ZooKeeper, already closed");
    }

    core.setName(cd.getName());
    SolrCore old = solrCores.putCore(cd, core);

    coreInitFailures.remove(cd.getName());

    if (old == null || old == core) {
      return null;
    } else {
      log.info("replacing core name={}", cd.getName());
      if (closeOld) {
        if (old != null) {
          SolrCore finalCore = old;
          try {
            Future<?> future = solrCoreExecutor.submit(() -> {
              if (log.isDebugEnabled()) {
                log.debug("Closing replaced core {}", cd.getName());
              }
              finalCore.closeAndWait();
            });
          } catch (RejectedExecutionException e) {
            finalCore.close();
          }
        }
      }
      return old;
    }
  }

  public void markCoreAsLoading(String name) {
    solrCores.markCoreAsLoading(name);
  }

  public void markCoreAsNotLoading(String name) {
    solrCores.markCoreAsNotLoading(name);
  }

  /**
   * Creates a new core, publishing the core state to the cluster
   *
   * @param coreName   the core name
   * @param parameters the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Map<String, String> parameters) {
    return create(coreName, cfg.getCoreRootDirectory().resolve(coreName), parameters, false, false);
  }

  /**
   * Creates a new core in a specified instance directory, publishing the core state to the cluster
   *
   * @param coreName     the core name
   * @param instancePath the instance directory
   * @param parameters   the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Path instancePath, Map<String, String> parameters, boolean newCollection, boolean newCore) {
    if (isShutDown) {
      throw new AlreadyClosedException();
    }
    // Reject duplicate core names synchronously — without this, a CREATE for an existing core
    // tramples the live core's registration and the caller gets a success for a core that was
    // never (re)created.
    if (solrCores.getCoreDescriptor(coreName) != null || solrCores.isLoaded(coreName)) {
      throw new SolrException(ErrorCode.SERVER_ERROR, "Core with name '" + coreName + "' already exists.");
    }
    StopWatch timeStartToCreate = new StopWatch(coreName + "-startToCreate");
    SolrCore core = null;
    CoreDescriptor cd = new CoreDescriptor(coreName, instancePath, parameters, containerProperties, getZkController());


    try {

      timeStartToCreate.done();

      // Much of the logic in core handling pre-supposes that the core.properties file already exists, so create it
      // first and clean it up if there's an error.
      StopWatch timeCreateCoresLocator = new StopWatch(coreName + "-createCoresLocator");
      coresLocator.create(this, cd);
      timeCreateCoresLocator.done();

      solrCores.putDescriptor(cd);

      StopWatch timeCreateFromDescriptor = new StopWatch(coreName + "-createFromDescriptor");
      core = createFromDescriptor(cd, newCollection, newCore);
      timeCreateFromDescriptor.done();

      StopWatch timePersist = new StopWatch(coreName + "-persist");
      coresLocator.persist(this, cd); // Write out the current core properties in case anything changed when the core was created
      timePersist.done();

      return core;
    } catch (Exception ex) {
      ParWork.propagateInterrupt(ex);
      // First clean up any core descriptor, there should never be an existing core.properties file for any core that
      // failed to be created on-the-fly.
      solrCores.removeCoreDescriptor(cd);
      try {
        coresLocator.delete(this, cd);
      } finally {
        if (core != null) {
          core.doClose();
        }
      }

      Throwable tc = ex;
      Throwable c = null;
      do {
        tc = tc.getCause();
        if (tc != null) {
          c = tc;
        }
      } while (tc != null);

      String rootMsg = "";
      if (c != null) {
        rootMsg = " Caused by: " + c.getMessage();
      }

      throw new SolrException(ErrorCode.BAD_REQUEST, "Error CREATEing SolrCore '" + coreName + "': " + ex.getMessage() + rootMsg, ex);
    }
  }

  /**
   * Creates a new core based on a CoreDescriptor.
   *
   * @param dcore        a core descriptor
   *                     <p>
   *                     WARNING: Any call to this method should be surrounded by a try/finally block
   *                     that calls solrCores.waitAddPendingCoreOps(...) and solrCores.removeFromPendingOps(...)
   *
   *                     <pre>
   *                                                               <code>
   *                                                               try {
   *                                                                  solrCores.waitAddPendingCoreOps(dcore.getName());
   *                                                                  createFromDescriptor(...);
   *                                                               } finally {
   *                                                                  solrCores.removeFromPendingOps(dcore.getName());
   *                                                               }
   *                                                               </code>
   *                                                             </pre>
   *                     <p>
   *                     Trying to put the waitAddPending... in this method results in Bad Things Happening due to race conditions.
   *                     getCore() depends on getting the core returned _if_ it's in the pending list due to some other thread opening it.
   *                     If the core is not in the pending list and not loaded, then getCore() calls this method. Anything that called
   *                     to check if the core was loaded _or_ in pending ops and, based on the return called createFromDescriptor would
   *                     introduce a race condition, see getCore() for the place it would be a problem
   * @return the newly created core
   */
  private SolrCore createFromDescriptor(CoreDescriptor dcore, boolean newCollection, boolean newCore) {

    log.debug("createFromDescriptor {} {}", dcore, newCollection);

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has been shutdown.");
    }

    SolrCore core = null;
    SolrCore old = null;

    String name = dcore.getName();

    boolean registered = false;
    try {
      MDCLoggingContext.setCoreName(dcore.getName());

      StopWatch timeValidateCoreNameLoadConfigSet = new StopWatch(dcore.getName() + "-validateCoreNameLoadConfigSet");

      SolrIdentifierValidator.validateCoreName(dcore.getName());

      ConfigSet coreConfig = coreConfigService.loadConfigSet(dcore);
      dcore.setConfigSetTrusted(coreConfig.isTrusted());
      if (log.isDebugEnabled()) {
        log.debug("Creating SolrCore '{}' using configuration from {} solrconfig={}, trusted={}", name, coreConfig.getName(),
            coreConfig.getSolrConfig().getName(), dcore.isConfigSetTrusted());
      }
      timeValidateCoreNameLoadConfigSet.done();

      SolrCore startFailedCore = null;
      try {
        core = new SolrCore(this, dcore, coreConfig, newCore);
        core.start();
      } catch (Exception e) {
        if (core != null) {
          startFailedCore = core;
          core = null;
        }
        core = processCoreCreateException(e, dcore, coreConfig, newCore);
        core.start();
      } finally {
        // Close the core that failed start() only after the recovered core has started,
        // to avoid closing the shared SolrCoreState prematurely.
        if (startFailedCore != null) {
          IOUtils.closeQuietly(startFailedCore);
        }
      }

      StopWatch timeRegisterCore = new StopWatch(name + "-registerCore");
      registerCore(dcore, core, true);
      registered = true;
      timeRegisterCore.done();

      if (isZooKeeperAware()) {
        StopWatch timeKickOffAsyncZkReg = new StopWatch(name + "-kickOffAsyncZkReg");
        if (!newCollection) {
          //          if (core.getDirectoryFactory().isSharedStorage()) {
          //            zkSys.getZkController().throwErrorIfReplicaReplaced(dcore);
          //          }
        }
        zkSys.getZkController().zkRegisterExecutor.submit(new ZkController.RegisterCoreAsync(zkSys.zkController, dcore, false));
        timeKickOffAsyncZkReg.done();
      }

      // always kick off recovery if we are in non-Cloud mode
      if (!isZooKeeperAware() && core.getUpdateHandler().getUpdateLog() != null) {
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      return core;
    } catch (AlreadyClosedException e) {
      throw e;
    } catch (Exception e) {
      log.error("Unable to create SolrCore", e);
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      if (e instanceof ZkController.NotInClusterStateException && !newCollection) {
        // this mostly happen when the core is deleted when this node is down
        unload(dcore.getName(), true, true, true);
        throw e;
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Unable to create core [" + dcore.getName() + "]", e);
    } catch (Throwable t) {
      log.error("Unable to create SolrCore {}", name, t);

      SolrException e = new SolrException(ErrorCode.SERVER_ERROR, "JVM Error creating core [" + dcore.getName() + "]: " + t.getMessage(), t);
      coreInitFailures.put(name, new CoreLoadFailure(dcore, e));
      solrCores.remove(dcore.getName());
      throw t;
    } finally {
      try {
        if (core != null) {
          if (!registered) {
            try {
              solrCoreExecutor.submit(core::closeAndWait);
            } catch (RejectedExecutionException e) {
              core.closeAndWait();
            }
          }
        }
      } finally {
        MDCLoggingContext.clear();
      }
    }
  }

  public boolean isSharedFs(CoreDescriptor cd) {
    try (SolrCore core = this.getCore(cd.getName())) {
      if (core != null) {
        return core.getDirectoryFactory().isSharedStorage();
      } else {
        ConfigSet configSet = coreConfigService.loadConfigSet(cd);
        return DirectoryFactory.loadDirectoryFactory(configSet.getSolrConfig(), this, null).isSharedStorage();
      }
    }
  }

  /**
   * Take action when we failed to create a SolrCore. If error is due to corrupt index, try to recover. Various recovery
   * strategies can be specified via system properties "-DCoreInitFailedAction={fromleader, none}"
   *
   * @param original   the problem seen when loading the core the first time.
   * @param dcore      core descriptor for the core to create
   * @param coreConfig core config for the core to create
   * @return if possible
   * @throws SolrException rethrows the original exception if we will not attempt to recover, throws a new SolrException with the
   *                       original exception as a suppressed exception if there is a second problem creating the solr core.
   * @see CoreInitFailedAction
   */
  private SolrCore processCoreCreateException(Exception original, CoreDescriptor dcore, ConfigSet coreConfig, boolean newCore) {
    log.error("Error creating SolrCore", original);

    // Traverse full chain since CIE may not be root exception
    Throwable cause = original;
    if (!(cause instanceof  CorruptIndexException)) {
      while (true) {
        final Throwable cause1 = cause.getCause();
        if ((cause = cause1) == null) break;
        if (cause instanceof CorruptIndexException) {
          break;
        }
      }
    }

    // If no CorruptIndexException, nothing we can try here
    if (cause == null) {
      if (original instanceof RuntimeException) {
        throw (RuntimeException) original;
      } else {
        throw new SolrException(ErrorCode.SERVER_ERROR, original);
      }
    }

    CoreInitFailedAction action = CoreInitFailedAction.valueOf(System.getProperty(CoreInitFailedAction.class.getSimpleName(), "none"));
    log.debug("CorruptIndexException while creating core, will attempt to repair via {}", action);

    switch (action) {
      case fromleader: // Recovery from leader on a CorruptedIndexException
        if (isZooKeeperAware()) {
          CloudDescriptor desc = dcore.getCloudDescriptor();
          try {
            Replica leader = getZkController().getClusterState()
                .getCollection(desc.getCollectionName())
                .getSlice(desc.getShardId())
                .getLeader(getZkController().getZkStateReader().getLiveNodes());
            if (leader != null && !leader.getName().equals(dcore.getName())
                && (leader.getState() == State.ACTIVE || leader.getState() == State.LEADER)) {
              log.info("Found active leader, will attempt to create fresh core and recover.");

              SolrConfig config = coreConfig.getSolrConfig();

              String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, dcore.getName());
              DirectoryFactory df = DirectoryFactory.loadDirectoryFactory(config, this, registryName);
              String dataDir = SolrCore.findDataDir(df, null, config, dcore);
              df.close();

              Path dir = Paths.get(dataDir);
              IOUtils.deleteDirectory(dir);

              SolrCore core = new SolrCore(this, dcore, coreConfig, newCore);

              // Initialize a new empty index so start() can open a searcher without hitting IndexNotFoundException
              core.getUpdateHandler().getSolrCoreState().newIndexWriter(core, false, true);

              // the index of this core is emptied, its term should be set to 0
              getZkController().getShardTerms(desc.getCollectionName(), desc.getShardId()).setTermToZero(dcore.getName());
              return core;
            }
          } catch (Exception se) {
            se.addSuppressed(original);
            if (se instanceof  SolrException) {
              throw (SolrException) se;
            } else {
              throw new SolrException(ErrorCode.SERVER_ERROR, se);
            }
          }
        }
        if (original instanceof RuntimeException) {
          throw (RuntimeException) original;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, original);
        }
      case none:
        if (original instanceof RuntimeException) {
          throw (RuntimeException) original;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, original);
        }
      default:
        log.warn("Failed to create core, and did not recognize specified 'CoreInitFailedAction': [{}]. Valid options are {}.",
            action, Arrays.asList(CoreInitFailedAction.values()));
        if (original instanceof RuntimeException) {
          throw (RuntimeException) original;
        } else {
          throw new SolrException(ErrorCode.SERVER_ERROR, original);
        }
    }
  }

  /**
   * @return a Collection of registered SolrCores
   */
  public Collection<SolrCore> getCores() {
    return solrCores.getCores();
  }

  /**
   * Gets the cores that are currently loaded, i.e. cores that have
   * 1: loadOnStartup=true and are either not-transient or, if transient, have been loaded and have not been aged out
   * 2: loadOnStartup=false and have been loaded but are either non-transient or have not been aged out.
   * <p>
   * Put another way, this will not return any names of cores that are lazily loaded but have not been called for yet
   * or are transient and either not loaded or have been swapped out.
   */
  public Collection<String> getLoadedCoreNames() {
    return solrCores.getLoadedCoreNames();
  }

  /**
   * This method is currently experimental.
   *
   * @return a Collection of the names that a specific core object is mapped to, there are more than one.
   */
  public Collection<String> getNamesForCore(SolrCore core) {
    return solrCores.getNamesForCore(core);
  }

  /**
   * get a list of all the cores that are currently known, whether currently loaded or not
   *
   * @return a list of all the available core names in either permanent or transient cores
   */
  public Collection<String> getAllCoreNames() {
    return solrCores.getAllCoreNames();

  }

  /**
   * Returns an immutable Map of Exceptions that occurred when initializing
   * SolrCores (either at startup, or do to runtime requests to create cores)
   * keyed off of the name (String) of the SolrCore that had the Exception
   * during initialization.
   * <p>
   * While the Map returned by this method is immutable and will not change
   * once returned to the client, the source data used to generate this Map
   * can be changed as various SolrCore operations are performed:
   * </p>
   * <ul>
   * <li>Failed attempts to create new SolrCores will add new Exceptions.</li>
   * <li>Failed attempts to re-create a SolrCore using a name already contained in this Map will replace the Exception.</li>
   * <li>Failed attempts to reload a SolrCore will cause an Exception to be added to this list -- even though the existing SolrCore with that name will continue to be available.</li>
   * <li>Successful attempts to re-created a SolrCore using a name already contained in this Map will remove the Exception.</li>
   * <li>Registering an existing SolrCore with a name already contained in this Map (ie: ALIAS or SWAP) will remove the Exception.</li>
   * </ul>
   */
  public Map<String, CoreLoadFailure> getCoreInitFailures() {
    return Collections.unmodifiableMap(coreInitFailures);
  }


  // ---------------- Core name related methods ---------------

//  private CoreDescriptor reloadCoreDescriptor(CoreDescriptor oldDesc) {
//    if (oldDesc == null) {
//      return null;
//    }
//
//    CorePropertiesLocator cpl = new CorePropertiesLocator(null);
//    CoreDescriptor ret = cpl.buildCoreDescriptor(oldDesc.getInstanceDir().resolve(PROPERTIES_FILENAME), this);
//
//    // Ok, this little jewel is all because we still create core descriptors on the fly from lists of properties
//    // in tests particularly. Theoretically, there should be _no_ way to create a CoreDescriptor in the new world
//    // of core discovery without writing the core.properties file out first.
//    //
//    // TODO: remove core.properties from the conf directory in test files, it's in a bad place there anyway.
//    if (ret == null) {
//      oldDesc.loadExtraProperties(); // there may be changes to extra properties that we need to pick up.
//      return oldDesc;
//
//    }
//    // The CloudDescriptor bit here is created in a very convoluted way, requiring access to private methods
//    // in ZkController. When reloading, this behavior is identical to what used to happen where a copy of the old
//    // CoreDescriptor was just re-used.
//
//    if (ret.getCloudDescriptor() != null) {
//      ret.getCloudDescriptor().reload(oldDesc.getCloudDescriptor());
//    }
//
//    return ret;
//  }

  /**
   * Recreates a SolrCore.
   * While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   *
   * @param name the name of the SolrCore to reload
   */
  public void reload(String name) throws InterruptedException {
    if (isShutDown) {
      throw new AlreadyClosedException();
    }
    // If the core previously failed to initialize (e.g. a corrupt or temporarily-missing index that
    // has since been repaired), a reload must retry creating it from its descriptor rather than
    // rethrowing the cached init failure. getCore(name) throws SolrCoreInitializationException for
    // cores recorded in coreInitFailures, so detect that case up front and recreate from the saved
    // CoreDescriptor (mirrors upstream Solr's reload-of-failed-core behavior).
    // Existence check ONLY. Use isLoaded() (which does NOT touch the refcount) rather than
    // getCoreFromAnyList() (which calls core.open()). The previous code opened the core here purely for
    // a null check and discarded the returned ref WITHOUT closing it, leaking one refCount per reload().
    // Under the config-overlay watcher (which fires cc.reload() repeatedly/concurrently), these leaked
    // refs accumulated (observed refCount climbing to 7/14 via -Dsolr.core.trackOpens) so the old
    // SolrCore's refCount never returned to 0 -> closeAndWait hit its 10s "continuing..." timeout -> the
    // abandoned old core kept gripping the shared SolrCoreState/reloadLock -> a subsequent reload (e.g. a
    // /config overlay bump) could not settle the new config version within the 30s waitForAllReplicasState
    // window (root cause of TestSolrConfigHandlerCloud's "N out of M ... within 30 seconds" 500).
    if (!solrCores.isLoaded(name)) {
      CoreLoadFailure clf = getCoreInitFailures().get(name);
      if (clf != null) {
        createFromDescriptor(clf.cd, true, false);
        return;
      }
    }
    try (SolrCore core = getCore(name)) {
      SolrCoreState corestate;
      ReentrantLock lock;
//[[      if (core == null) {
//        while (isCoreLoading(origCorename)) {
//          Thread.sleep(250); // nocommit - make efficient
//        }
//        core = getCore(origCorename);
//      }]]
      if (core == null) {
        throw new SolrException(ErrorCode.BAD_REQUEST, "SolrCore does not exist name=" + name + " " + solrCores.getCoreDescriptors() + ' ' + solrCores.getRegisteredCoreNames());
      }
      corestate = core.getUpdateHandler().getSolrCoreState();
      lock = corestate.getReloadLock();
      lock.lock();
      try {
        SolrCore newCore = null;

        if (core != null) {

          // The underlying core properties files may have changed, we don't really know. So we have a (perhaps) stale
          // CoreDescriptor and we need to reload it from the disk files
          CoreDescriptor cd = core.getCoreDescriptor();
          //        if (core.getDirectoryFactory().isPersistent()) {
          //          cd = reloadCoreDescriptor(core.getCoreDescriptor());
          //        } else {
          //          cd = core.getCoreDescriptor();
          //        }
          //        solrCores.addCoreDescriptor(cd);

          boolean success = false;
          try {

            ConfigSet coreConfig = coreConfigService.loadConfigSet(cd);
            log.debug("Reloading SolrCore '{}' using configuration from {}", name, coreConfig.getName());
            DocCollection docCollection = null;
            if (isShutDown) {
              throw new AlreadyClosedException();
            }

            // Commit any pending (uncommitted) changes on the existing core BEFORE reloading,
            // so the docs buffered in the old IndexWriter are flushed to the index directory and
            // are visible to the new core's NRT reader opened during reload. Otherwise a reload
            // (e.g. triggered by MODIFYCOLLECTION READ_ONLY=true) can drop uncommitted docs.
            try {
              RefCounted<IndexWriter> preReloadIw = core.getSolrCoreState().getIndexWriter(null);
              if (preReloadIw != null) {
                try {
                  IndexWriter iw = preReloadIw.get();
                  if (iw != null) {
                    // Stamp commit metadata (commitTimeMSec) so this pre-reload flush produces a commit
                    // point with a non-zero replication version. Without it, a bare iw.commit() here
                    // leaves empty userData -> getCommitTimestamp()==0 -> ReplicationHandler reports
                    // index version 0 -> a recovering replica treats this (data-bearing) leader as empty
                    // and never completes recovery (TestCloudSearcherWarming). Only stamp when there is
                    // something to flush, so we don't create a spurious commit or overwrite a properly
                    // stamped existing commit.
                    if (iw.hasUncommittedChanges()) {
                      // Only stamp fresh commit metadata when the writer doesn't already carry valid
                      // commit data. Re-stamping here would mint a new commitTimeMSec and bump the
                      // replication index version on every reload (even when the pending changes are
                      // just an async flush/merge and no docs were added), breaking the
                      // reload-preserves-version invariant and triggering spurious follower
                      // re-replication. When no valid metadata exists yet, stamping is still required
                      // so the flush produces a non-zero replication version (see
                      // SolrIndexWriter.hasValidCommitData).
                      if (!SolrIndexWriter.hasValidCommitData(iw)) {
                        SolrIndexWriter.setCommitData(iw, -1);
                      }
                      iw.commit();
                    }
                  }
                } finally {
                  preReloadIw.decref();
                }
              }
            } catch (Exception e) {
              log.warn("Failed pre-reload commit of pending changes for core {}", name, e);
            }

            newCore = core.reload(coreConfig);

            if (isShutDown) {
              throw new AlreadyClosedException();
            }

            try {
              if (getZkController() != null) {
                docCollection = getZkController().getClusterState().getCollection(cd.getCollectionName());
                // The node-local cached cluster state can lag the ZK write that toggled READ_ONLY
                // (the watch may not have been processed yet on this node). Fetch the authoritative
                // collection state from ZK so the reloaded core's read-only mode matches the intent;
                // otherwise turning read-only OFF can leave replica cores stuck read-only (403).
                try {
                  DocCollection live = getZkController().getZkStateReader().getCollectionLive(cd.getCollectionName()).get(10, TimeUnit.SECONDS);
                  if (live != null) {
                    docCollection = live;
                  }
                } catch (Exception e) {
                  log.warn("Failed fetching live collection state for read-only determination on reload of {}", name, e);
                }
                // turn off indexing now, before the new core is registered
                if (docCollection.getBool(ZkStateReader.READ_ONLY, false)) {
                  newCore.readOnly = true;
                }
              }

              registerCore(cd, newCore, true);

              success = true;
              corestate.successReloads.increment();
            } catch (Exception e) {
              log.error("Exception registering reloaded core", e);
              throw new SolrException(ErrorCode.SERVER_ERROR, e);
            }

            // force commit on old core if the new one is readOnly and prevent any new updates
            if (newCore.readOnly) {
              RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(null);
              if (iwRef != null) {
                IndexWriter iw = iwRef.get();
                // switch old core to readOnly
                core.readOnly = true;
                try {
                  if (iw != null) {
                    if (iw.hasUncommittedChanges()) {
                      // Only stamp fresh commit metadata when the writer doesn't already carry valid
                      // commit data; re-stamping would mint a new commitTimeMSec and bump the
                      // replication index version unnecessarily (see SolrIndexWriter.hasValidCommitData).
                      if (!SolrIndexWriter.hasValidCommitData(iw)) {
                        SolrIndexWriter.setCommitData(iw, -1);
                      }
                      iw.commit();
                    }
                  }
                } finally {
                  iwRef.decref();
                }
              }
            }

            if (docCollection != null) {
              Replica replica = docCollection.getReplica(cd.getName());
              if (replica.getType() == Replica.Type.TLOG) { // TODO: needed here?
                getZkController().stopReplicationFromLeader(core.getName());
                Replica leader = getZkController().zkStateReader.getLeader(docCollection.getName(), replica.getSlice());
                if (cd.getName().equals(leader.getName())) {
                  getZkController().startReplicationFromLeader(newCore.getName(), true);
                }

              } else if (replica.getType() == Replica.Type.PULL) {
                getZkController().startReplicationFromLeader(newCore.getName(), false);
              }
            }

          } catch (SolrCoreState.CoreIsClosedException e) {
            log.error("Core is closed", e);
            throw e;
          } catch (Exception e) {
            ParWork.propagateInterrupt("Exception reloading SolrCore", e);
            SolrException exp = new SolrException(ErrorCode.SERVER_ERROR, "Unable to reload core [" + cd.getName() + "]", e);
            try {
              coreInitFailures.put(cd.getName(), new CoreLoadFailure(cd, e));

            } catch (Exception e1) {
              ParWork.propagateInterrupt(e1);
              exp.addSuppressed(e1);
            }
            throw exp;
          } finally {
            corestate.reloads.increment();
            if (!success && newCore != null) {
              log.warn("Failed reloading core, cleaning up new core");
              SolrCore finalNewCore = newCore;
              try {
                solrCoreExecutor.submit(() -> {
                  log.error("Closing failed new SolrCore on reload");
                  finalNewCore.close();
                });
              } catch (RejectedExecutionException e) {
                finalNewCore.doClose();
              }
            }
          }

        } else {
          CoreLoadFailure clf = coreInitFailures.get(name);
          if (clf != null) {
            //   createFromDescriptor(clf.cd, false);
          } else {
            throw new SolrException(ErrorCode.BAD_REQUEST, "No such core: " + name);
          }
        }

      } finally {
        if (lock.isHeldByCurrentThread()) lock.unlock();
      }
    }
  }

  /**
   * Swaps two SolrCore descriptors.
   */
  public void swap(String n0, String n1) {
    if (n0 == null || n1 == null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Can not swap unnamed cores.");
    }
    solrCores.swap(n0, n1);

    coresLocator.swap(this, solrCores.getCoreDescriptor(n0), solrCores.getCoreDescriptor(n1));

    log.info("swapped: {} with {}", n0, n1);
  }

  /**
   * Unload a core from this container, leaving all files on disk
   *
   * @param name the name of the core to unload
   */
  public void unload(String name) {
    unload(name, false, false, false);
  }


  public void unload(String name, boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir) {
    unload(null, name, deleteIndexDir, deleteDataDir, deleteInstanceDir, true);
  }

  /**
   * Unload a core from this container, optionally removing the core's data and configuration
   *
   * @param name              the name of the core to unload
   * @param deleteIndexDir    if true, delete the core's index on close
   * @param deleteDataDir     if true, delete the core's data directory on close
   * @param deleteInstanceDir if true, delete the core's instance directory on close
   */
  public void unload(CoreDescriptor cd, String name, boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir, boolean removeFromZk) {
    log.info("Unload SolrCore {} deleteIndexDir={} deleteDataDir={} deleteInstanceDir={}", name, deleteIndexDir, deleteDataDir, deleteInstanceDir);
    if (cd == null) {
      cd = solrCores.getCoreDescriptor(name);
    }

    SolrException exception = null;
    try {

      if (isZooKeeperAware()) {
        if (cd != null) {
          try {
            zkSys.getZkController().unregister(name, cd.getCollectionName(), cd.getCloudDescriptor().getShardId(), removeFromZk);
          } catch (AlreadyClosedException e) {

          } catch (Exception e) {
            log.error("Error unregistering core [{}] from cloud state", name, e);
            exception = new SolrException(ErrorCode.SERVER_ERROR, "Error unregistering core [" + name + "] from cloud state", e);
          }
        }
      }

      if (name != null) {
        log.info("Remove load failures for core {}", name);
        CoreLoadFailure loadFailure = coreInitFailures.remove(name);
        if (loadFailure != null) {

          // getting the index directory requires opening a DirectoryFactory with a SolrConfig, etc,
          // which we may not be able to do because of the init error.  So we just go with what we
          // can glean from the CoreDescriptor - datadir and instancedir
          try {
            SolrCore.deleteUnloadedCore(loadFailure.cd, deleteDataDir, deleteInstanceDir);
            // If last time around we didn't successfully load, make sure that all traces of the coreDescriptor are gone.
            solrCores.remove(name);
            if (cd != null) {
              coresLocator.delete(this, cd);
            }
          } catch (Exception e) {
            SolrException.log(log, "Failed try to unload failed core:" + name + " dir:" + (cd == null ? "null cd" : cd.getInstanceDir()));
          }
          return;
        }
      }

      SolrCore core = null;
      if (name != null) {
        log.info("Remove the core from SolrCores {}", name);
        core = solrCores.remove(name);
      }
      if (core != null) {
        if (cd == null) {
          cd = core.getCoreDescriptor();
        }
        try {
          core.getSolrCoreState().cancelRecovery(false, true);
        } catch (Exception e) {
          SolrException.log(log, "Failed canceling recovery for core:" + cd.getName() + " dir:" + cd.getInstanceDir());
        }
      } else {
        SolrException ex = new SolrException(ErrorCode.BAD_REQUEST, "Cannot unload non-existent core [" + name + "]");
        if (isZooKeeperAware()) {
          log.warn("SolrCore does not exist", ex);
          return;
        } else {
          throw ex;
        }
      }

      // delete metrics specific to this core
      if (metricManager != null) {
        metricManager.removeRegistry(core.getCoreMetricManager().getRegistryName());
      }

      core.unloadOnClose(deleteIndexDir, deleteDataDir);

      if (isZooKeeperAware()) {
        getZkController().stopReplicationFromLeader(name);
      }
      try {
        core.doClose();
      } catch (Exception e) {
        SolrException.log(log, "Failed closing or waiting for closed core:" + cd.getName() + " dir:" + cd.getInstanceDir());
      }

      log.info("Done unloading SolrCore {}", name);
      if (exception != null) {
        throw exception;
      }
    } finally {
      if (deleteInstanceDir && cd != null) {
        Path dir = cd.getInstanceDir();
        while (Files.exists(dir)) {
          IOUtils.deleteDirectory(dir);
        }
      }
    }
  }

  public void rename(String name, String toName) {
    SolrIdentifierValidator.validateCoreName(toName);
    try (SolrCore core = getCore(name)) {
      if (core != null) {
        String oldRegistryName = core.getCoreMetricManager().getRegistryName();
        String newRegistryName = SolrCoreMetricManager.createRegistryName(core, toName);
        metricManager.swapRegistries(oldRegistryName, newRegistryName);
        // The old coreDescriptor is obsolete, so remove it. registerCore will put it back.
        CoreDescriptor cd = core.getCoreDescriptor();
        cd.setProperty("name", toName);
        core.setName(toName);
        registerCore(cd, core, false);
        solrCores.addCoreDescriptor(cd);  // registerCore only updates the cores map; we must also update the descriptor map
        SolrCore old = solrCores.remove(name);

        coresLocator.rename(this, old.getCoreDescriptor(), core.getCoreDescriptor());
      }
    }
  }

  /**
   * Get the CoreDescriptors for all cores managed by this container
   *
   * @return a List of CoreDescriptors
   */
  public Collection<CoreDescriptor> getCoreDescriptors() {
    return solrCores.getCoreDescriptors();
  }

  public CoreDescriptor getCoreDescriptor(String coreName) {
    return solrCores.getCoreDescriptor(coreName);
  }

  /** Where cores are created (absolute). */
  public Path getCoreRootDirectory() {
    return cfg.getCoreRootDirectory();
  }


  /**
   * Identity-only peek at the currently-registered core for {@code name} (no reference-count change;
   * may return null). Used to detect a superseded SolrCore instance after a reload. The returned core
   * must NOT be used to service requests, only compared by identity.
   */
  public SolrCore peekCore(String name) {
    return solrCores.peekCore(name);
  }

  /**
   * Gets a core by name and increase its refcount.
   *
   * @param name the core name
   * @return the core if found, null if a SolrCore by this name does not exist
   * @throws SolrCoreInitializationException if a SolrCore with this name failed to be initialized
   * @see SolrCore#close()
   */
  public SolrCore getCore(String name) {

    if (name == null) {
      throw new IllegalArgumentException("SolrCore name cannot be null");
    }
    SolrCore core = null;
    CoreDescriptor desc = null;

    // Do this in two phases since we don't want to lock access to the cores over a load.
    // getCoreFromAnyList() calls core.open(), which throws AlreadyClosedException if the core began
    // closing between the map lookup and the open(). getCore's contract is "return null if not
    // present" (callers use try (SolrCore c = getCore(...)) + null-check), so a concurrent unload/close
    // racing this lookup must surface as a clean miss, not a spurious AlreadyClosedException to the client.
    try {
      core = solrCores.getCoreFromAnyList(name);
    } catch (AlreadyClosedException e) {
      return null;
    }

    // If a core is loaded, we're done just return it.
    if (core != null) {
      return core;
    }

    // If it's not yet loaded, we can check if it's had a core init failure and "do the right thing"

    // if there was an error initializing this core, throw a 500
    // error with the details for clients attempting to access it.
    CoreLoadFailure loadFailure = getCoreInitFailures().get(name);
    if (null != loadFailure) {
      throw new SolrCoreInitializationException(name, loadFailure.exception);
    }

    // This will put an entry in pending core ops if the core isn't loaded. Here's where moving the
    // waitAddPendingCoreOps to createFromDescriptor would introduce a race condition.

    // Lazy-load non-startup cores on first access
    desc = solrCores.getCoreDescriptor(name);
    if (desc != null && !desc.isLoadOnStartup()) {
      try {
        core = createFromDescriptor(desc, false, false);
        // createFromDescriptor returns the core holding only its registration reference (refCount 1).
        // The loaded path above returns via getCoreFromAnyList(), which open()s an extra ref for the
        // caller. Match that contract here, otherwise the first accessor's try-with-resources close()
        // decrefs the registration ref to 0 and starts closing a freshly-registered core.
        if (core != null) {
          core.open();
        }
      } catch (Exception e) {
        log.error("Error lazy-loading core {}", name, e);
      }
    }
    return core;
  }

  /**
   * If using asyncSolrCoreLoad=true, calling this after {@link #load()} will
   * not return until all cores have finished loading.
   *
   * @param timeoutMs timeout, upon which method simply returns
   */
  public void waitForLoadingCoresToFinish(long timeoutMs) {
    solrCores.waitForLoadingCoresToFinish(timeoutMs);
  }

  // ---------------- CoreContainer request handlers --------------

  protected <T> T createHandler(String path, String handlerClass, Class<T> clazz) {
    T handler = loader.newInstance(handlerClass, clazz, new String[]{"handler.admin."}, new Class[]{CoreContainer.class}, new Object[]{this});
    if (handler instanceof SolrRequestHandler) {
      containerHandlers.put(path, (SolrRequestHandler) handler);
    }
    if (handler instanceof SolrMetricProducer) {
      ((SolrMetricProducer) handler).initializeMetrics(solrMetricsContext, path);
    }
    return handler;
  }

  public CoreAdminHandler getMultiCoreHandler() {
    return coreAdminHandler;
  }

  public CollectionsHandler getCollectionsHandler() {
    return collectionsHandler;
  }

  public HealthCheckHandler getHealthCheckHandler() {
    return healthCheckHandler;
  }

  public InfoHandler getInfoHandler() {
    return infoHandler;
  }

  public ConfigSetsHandler getConfigSetsHandler() {
    return configSetsHandler;
  }

  public ConfigSetService getConfigSetService() {
    return coreConfigService;
  }

  public String getHostName() {
    return this.hostName;
  }

  /**
   * Gets the alternate path for multicore handling:
   * This is used in case there is a registered unnamed core (aka name is "") to
   * declare an alternate way of accessing named cores.
   * This can also be used in a pseudo single-core environment so admins can prepare
   * a new version before swapping.
   */
  public String getManagementPath() {
    return cfg.getManagementPath();
  }

  @SuppressWarnings({"rawtypes"})
  public LogWatcher getLogging() {
    return logging;
  }

  /**
   * Determines whether the core is already loaded or not but does NOT load the core
   */
  public boolean isLoaded(String name) {
    return solrCores.isLoaded(name);
  }

  // Primarily for transient cores when a core is aged out.
  //  public void queueCoreToClose(SolrCore coreToClose) {
  //    solrCores.queueCoreToClose(coreToClose);
  //  }

  /**
   * Gets a solr core descriptor for a core that is not loaded. Note that if the caller calls this on a
   * loaded core, the unloaded descriptor will be returned.
   *
   * @param cname - name of the unloaded core descriptor to load. NOTE:
   * @return a coreDescriptor. May return null
   */
  public CoreDescriptor getUnloadedCoreDescriptor(String cname) {
    return solrCores.getUnloadedCoreDescriptor(cname);
  }

  /** The primary path of a Solr server's config, cores, and misc things. Absolute. */
  //TODO return Path
  public String getSolrHome() {
    return solrHome.toString();
  }

  public boolean isZooKeeperAware() {
    return zkSys != null && zkSys.zkController != null;
  }

  public ZkController getZkController() {
    return zkSys == null ? null : zkSys.getZkController();
  }

  public NodeConfig getConfig() {
    return cfg;
  }

  /**
   * The default ShardHandlerFactory used to communicate with other solr instances
   */
  public ShardHandlerFactory getShardHandlerFactory() {
    return shardHandlerFactory;
  }

  public UpdateShardHandler getUpdateShardHandler() {
    return updateShardHandler;
  }

  public SolrResourceLoader getResourceLoader() {
    return loader;
  }

  public AuthorizationPlugin getAuthorizationPlugin() {
    return authorizationPlugin == null ? null : authorizationPlugin.plugin;
  }

  public AuthenticationPlugin getAuthenticationPlugin() {
    return authenticationPlugin == null ? null : authenticationPlugin.plugin;
  }

  public AuditLoggerPlugin getAuditLoggerPlugin() {
    return auditloggerPlugin == null ? null : auditloggerPlugin.plugin;
  }

  public NodeConfig getNodeConfig() {
    return cfg;
  }

  public long getStatus() {
    return status;
  }

  /**
   * @param solrCore the core against which we check if there has been a tragic exception
   * @return whether this Solr core has tragic exception
   * @see IndexWriter#getTragicException()
   */
  public boolean checkTragicException(SolrCore solrCore) {
    Throwable tragicException;
    try {
      tragicException = solrCore.getSolrCoreState().getTragicException();
    } catch (IOException e) {
      // failed to open an indexWriter
      tragicException = e;
    }

    if (tragicException != null && isZooKeeperAware()) {
      getZkController().giveupLeadership(solrCore.getCoreDescriptor(), tragicException);
    }

    return tragicException != null;
  }

//  static {
//    ExecutorUtil.addThreadLocalProvider(SolrRequestInfo.getInheritableThreadLocalProvider());
//  }

  private static class CredentialsProviderProvider extends SolrHttpClientContextBuilder.CredentialsProviderProvider {

    private final SolrHttpClientBuilder builder;

    public CredentialsProviderProvider(SolrHttpClientBuilder builder) {
      this.builder = builder;
    }

    @Override
    public CredentialsProvider getCredentialsProvider() {
      return builder.getCredentialsProviderProvider().getCredentialsProvider();
    }
  }

  private static class AuthSchemeRegistryProvider extends SolrHttpClientContextBuilder.AuthSchemeRegistryProvider {

    private final SolrHttpClientBuilder builder;

    public AuthSchemeRegistryProvider(SolrHttpClientBuilder builder) {
      this.builder = builder;
    }

    @Override
    public Lookup<AuthSchemeProvider> getAuthSchemeRegistry() {
      return builder.getAuthSchemeRegistryProvider().getAuthSchemeRegistry();
    }
  }

  private class EmbeddedSolrServer extends org.apache.solr.client.solrj.embedded.EmbeddedSolrServer {
    public EmbeddedSolrServer() {
      super(CoreContainer.this, null);
    }

    @Override
    public void close() throws IOException {
      // do nothing - we close the container ourselves
    }
  }
}

