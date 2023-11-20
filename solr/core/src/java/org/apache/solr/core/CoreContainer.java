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

import com.github.benmanes.caffeine.cache.Interner;
import com.google.common.annotations.VisibleForTesting;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracer;
import io.opentracing.noop.NoopTracerFactory;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.Lookup;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.solr.api.ContainerPluginsRegistry;
import org.apache.solr.api.JerseyResource;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.SolrHttpClientBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder.AuthSchemeRegistryProvider;
import org.apache.solr.client.solrj.impl.SolrHttpClientContextBuilder.CredentialsProviderProvider;
import org.apache.solr.client.solrj.io.SolrClientCache;
import org.apache.solr.client.solrj.util.SolrIdentifierValidator;
import org.apache.solr.cloud.CloudDescriptor;
import org.apache.solr.cloud.ClusterSingleton;
import org.apache.solr.cloud.OverseerTaskQueue;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.cloud.api.collections.DistributedCollectionConfigSetCommandRunner;
import org.apache.solr.cluster.events.ClusterEventProducer;
import org.apache.solr.cluster.events.impl.ClusterEventProducerFactory;
import org.apache.solr.cluster.placement.PlacementPluginConfig;
import org.apache.solr.cluster.placement.PlacementPluginFactory;
import org.apache.solr.cluster.placement.impl.DelegatingPlacementPluginFactory;
import org.apache.solr.cluster.placement.impl.PlacementPluginFactoryLoader;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.Aliases;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Replica.State;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.ObjectCache;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.DirectoryFactory.DirContext;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.filestore.FileStoreAPI;
import org.apache.solr.handler.ClusterAPI;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.SnapShooter;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.admin.ConfigSetsHandler;
import org.apache.solr.handler.admin.ContainerPluginsApi;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.CoreAdminHandler.CoreAdminOp;
import org.apache.solr.handler.admin.HealthCheckHandler;
import org.apache.solr.handler.admin.InfoHandler;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.handler.admin.SecurityConfHandler;
import org.apache.solr.handler.admin.SecurityConfHandlerLocal;
import org.apache.solr.handler.admin.SecurityConfHandlerZk;
import org.apache.solr.handler.admin.ZookeeperInfoHandler;
import org.apache.solr.handler.admin.ZookeeperReadAPI;
import org.apache.solr.handler.admin.ZookeeperStatusHandler;
import org.apache.solr.handler.api.V2ApiUtils;
import org.apache.solr.handler.component.ShardHandlerFactory;
import org.apache.solr.handler.designer.SchemaDesignerAPI;
import org.apache.solr.jersey.InjectionFactories;
import org.apache.solr.jersey.JerseyAppHandlerCache;
import org.apache.solr.logging.LogWatcher;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.pkg.SolrPackageLoader;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.search.CacheConfig;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrFieldCacheBean;
import org.apache.solr.security.AllowListUrlChecker;
import org.apache.solr.security.AuditLoggerPlugin;
import org.apache.solr.security.AuthenticationPlugin;
import org.apache.solr.security.AuthorizationPlugin;
import org.apache.solr.security.HttpClientBuilderPlugin;
import org.apache.solr.security.PKIAuthenticationPlugin;
import org.apache.solr.security.PublicKeyHandler;
import org.apache.solr.security.SecurityPluginHolder;
import org.apache.solr.security.SolrNodeKeyPair;
import org.apache.solr.update.SolrCoreState;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.solr.util.OrderedExecutor;
import org.apache.solr.util.RefCounted;
import org.apache.solr.util.StartupLoggingUtils;
import org.apache.solr.util.stats.MetricUtils;
import org.apache.zookeeper.KeeperException;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ApplicationHandler;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @since solr 1.3
 */
public class CoreContainer {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  {
    // Declared up top to ensure this is present before anything else.
    // note: will not be re-added if already there
    ExecutorUtil.addThreadLocalProvider(SolrRequestInfo.getInheritableThreadLocalProvider());
  }

  final SolrCores solrCores;

  public static class CoreLoadFailure {

    public final CoreDescriptor cd;
    public final Exception exception;

    public CoreLoadFailure(CoreDescriptor cd, Exception loadFailure) {
      this.cd = new CoreDescriptor(cd.getName(), cd);
      this.exception = loadFailure;
    }
  }

  private volatile PluginBag<SolrRequestHandler> containerHandlers =
      new PluginBag<>(SolrRequestHandler.class, null);

  private volatile ApplicationHandler jerseyAppHandler;
  private volatile JerseyAppHandlerCache appHandlersByConfigSetId;

  public ApplicationHandler getJerseyApplicationHandler() {
    return jerseyAppHandler;
  }

  public JerseyAppHandlerCache getJerseyAppHandlerCache() {
    return appHandlersByConfigSetId;
  }

  /** Minimize exposure to CoreContainer. Mostly only ZK interface is required */
  public final Supplier<SolrZkClient> zkClientSupplier = () -> getZkController().getZkClient();

  private final ContainerPluginsRegistry containerPluginsRegistry =
      new ContainerPluginsRegistry(this, containerHandlers.getApiBag());

  protected final Map<String, CoreLoadFailure> coreInitFailures = new ConcurrentHashMap<>();

  protected volatile CoreAdminHandler coreAdminHandler = null;
  protected volatile CollectionsHandler collectionsHandler = null;
  protected volatile HealthCheckHandler healthCheckHandler = null;

  private volatile InfoHandler infoHandler;
  protected volatile ConfigSetsHandler configSetsHandler = null;

  private volatile PKIAuthenticationPlugin pkiAuthenticationSecurityBuilder;

  protected volatile Properties containerProperties;

  private volatile ConfigSetService coreConfigService;

  protected final ZkContainer zkSys = new ZkContainer();
  protected volatile ShardHandlerFactory shardHandlerFactory;

  private volatile UpdateShardHandler updateShardHandler;

  private volatile ExecutorService coreContainerWorkExecutor =
      ExecutorUtil.newMDCAwareCachedThreadPool(
          new SolrNamedThreadFactory("coreContainerWorkExecutor"));

  private final OrderedExecutor replayUpdatesExecutor;

  protected volatile LogWatcher<?> logging = null;

  private volatile CloserThread backgroundCloser = null;
  protected final NodeConfig cfg;
  protected final SolrResourceLoader loader;

  protected final Path solrHome;

  protected final SolrNodeKeyPair nodeKeyPair;

  protected final CoresLocator coresLocator;

  private volatile String hostName;

  private final BlobRepository blobRepository = new BlobRepository(this);

  private volatile boolean asyncSolrCoreLoad;

  protected volatile SecurityConfHandler securityConfHandler;

  private volatile SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin;

  private volatile SecurityPluginHolder<AuthenticationPlugin> authenticationPlugin;

  private volatile SecurityPluginHolder<AuditLoggerPlugin> auditloggerPlugin;

  private volatile BackupRepositoryFactory backupRepoFactory;

  protected volatile SolrMetricManager metricManager;

  protected volatile String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);

  protected volatile SolrMetricsContext solrMetricsContext;

  protected volatile Tracer tracer = NoopTracerFactory.create();

  protected MetricsHandler metricsHandler;

  private volatile SolrClientCache solrClientCache;

  private volatile Map<String, SolrCache<?, ?>> caches;

  private final ObjectCache objectCache = new ObjectCache();

  public final NodeRoles nodeRoles = new NodeRoles(System.getProperty(NodeRoles.NODE_ROLES_PROP));

  private final ClusterSingletons clusterSingletons =
      new ClusterSingletons(
          () ->
              getZkController() != null
                  && getZkController().getOverseer() != null
                  && !getZkController().getOverseer().isClosed(),
          (r) -> this.runAsync(r));

  private volatile ClusterEventProducer clusterEventProducer;
  private DelegatingPlacementPluginFactory placementPluginFactory;

  private FileStoreAPI fileStoreAPI;
  private SolrPackageLoader packageLoader;

  private final Set<Path> allowPaths;

  private final AllowListUrlChecker allowListUrlChecker;

  // Bits for the state variable.
  public static final long LOAD_COMPLETE = 0x1L;
  public static final long CORE_DISCOVERY_COMPLETE = 0x2L;
  public static final long INITIAL_CORE_LOAD_COMPLETE = 0x4L;
  private volatile long status = 0L;

  private ExecutorService coreContainerAsyncTaskExecutor =
      ExecutorUtil.newMDCAwareCachedThreadPool("Core Container Async Task");

  /**
   * Non-empty if the Collection API is executed in a distributed way and not on Overseer, once the
   * CoreContainer has been initialized properly, i.e. method {@link #load()} called. Until then it
   * is null, and it is not expected to be read.
   */
  private volatile Optional<DistributedCollectionConfigSetCommandRunner>
      distributedCollectionCommandRunner;

  private enum CoreInitFailedAction {
    fromleader,
    none
  }

  /**
   * This method instantiates a new instance of {@linkplain BackupRepository}.
   *
   * @param repositoryName The name of the backup repository (Optional). If not specified, a default
   *     implementation is used.
   * @return a new instance of {@linkplain BackupRepository}.
   */
  public BackupRepository newBackupRepository(String repositoryName) {
    BackupRepository repository;
    if (repositoryName != null) {
      repository = backupRepoFactory.newInstance(getResourceLoader(), repositoryName);
    } else {
      repository = backupRepoFactory.newInstance(getResourceLoader());
    }
    return repository;
  }

  public ExecutorService getCoreZkRegisterExecutorService() {
    return zkSys.getCoreZkRegisterExecutorService();
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
   * Create a new CoreContainer using the given solr home directory. The container's cores are not
   * loaded.
   *
   * @param solrHome a String containing the path to the solr home directory
   * @param properties substitutable properties (alternative to Sys props)
   * @see #load()
   */
  public CoreContainer(Path solrHome, Properties properties) {
    this(SolrXmlConfig.fromSolrHome(solrHome, properties));
  }

  /**
   * Create a new CoreContainer using the given configuration. The container's cores are not loaded.
   *
   * @param config a ConfigSolr representation of this container's configuration
   * @see #load()
   */
  public CoreContainer(NodeConfig config) {
    this(config, CoresLocator.instantiate(config));
  }

  public CoreContainer(NodeConfig config, boolean asyncSolrCoreLoad) {
    this(config, CoresLocator.instantiate(config), asyncSolrCoreLoad);
  }

  /**
   * Create a new CoreContainer using the given configuration and locator.
   *
   * <p>The container's cores are not loaded. This constructor should be used only in tests, as it
   * overrides {@link CoresLocator}'s instantiation process.
   *
   * @param config a ConfigSolr representation of this container's configuration
   * @param locator a CoresLocator
   * @see #load()
   */
  @VisibleForTesting
  public CoreContainer(NodeConfig config, CoresLocator locator) {
    this(config, locator, false);
  }

  public CoreContainer(NodeConfig config, CoresLocator locator, boolean asyncSolrCoreLoad) {
    this.cfg = requireNonNull(config);
    this.loader = config.getSolrResourceLoader();
    this.solrHome = config.getSolrHome();
    this.solrCores = SolrCores.newSolrCores(this);
    this.nodeKeyPair = new SolrNodeKeyPair(cfg.getCloudConfig());
    containerHandlers.put(PublicKeyHandler.PATH, new PublicKeyHandler(nodeKeyPair));
    if (null != this.cfg.getBooleanQueryMaxClauseCount()) {
      IndexSearcher.setMaxClauseCount(this.cfg.getBooleanQueryMaxClauseCount());
    }
    setWeakStringInterner();
    this.coresLocator = locator;
    this.containerProperties = new Properties(config.getSolrProperties());
    this.asyncSolrCoreLoad = asyncSolrCoreLoad;
    this.replayUpdatesExecutor =
        new OrderedExecutor(
            cfg.getReplayUpdatesThreads(),
            ExecutorUtil.newMDCAwareCachedThreadPool(
                cfg.getReplayUpdatesThreads(),
                new SolrNamedThreadFactory("replayUpdatesExecutor")));
    this.appHandlersByConfigSetId = new JerseyAppHandlerCache();

    SolrPaths.AllowPathBuilder allowPathBuilder = new SolrPaths.AllowPathBuilder();
    allowPathBuilder.addPath(cfg.getSolrHome());
    allowPathBuilder.addPath(cfg.getCoreRootDirectory());
    if (cfg.getSolrDataHome() != null) {
      allowPathBuilder.addPath(cfg.getSolrDataHome());
    }
    if (!cfg.getAllowPaths().isEmpty()) {
      cfg.getAllowPaths().forEach(allowPathBuilder::addPath);
      if (log.isInfoEnabled()) {
        log.info("Allowing use of paths: {}", cfg.getAllowPaths());
      }
    }
    this.allowPaths = allowPathBuilder.build();

    this.allowListUrlChecker = AllowListUrlChecker.create(config);
  }

  @SuppressWarnings({"unchecked"})
  private synchronized void initializeAuthorizationPlugin(Map<String, Object> authorizationConf) {
    authorizationConf = Utils.getDeepCopy(authorizationConf, 4);
    int newVersion = readVersion(authorizationConf);
    // Initialize the Authorization module
    SecurityPluginHolder<AuthorizationPlugin> old = authorizationPlugin;
    SecurityPluginHolder<AuthorizationPlugin> authorizationPlugin = null;
    if (authorizationConf != null) {
      String klas = (String) authorizationConf.get("class");
      if (klas == null) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "class is required for authorization plugin");
      }
      if (old != null && old.getZnodeVersion() == newVersion && newVersion > 0) {
        log.debug("Authorization config not modified");
        return;
      }
      log.info("Initializing authorization plugin: {}", klas);
      authorizationPlugin =
          new SecurityPluginHolder<>(
              newVersion,
              getResourceLoader()
                  .newInstance(
                      klas,
                      AuthorizationPlugin.class,
                      null,
                      new Class<?>[] {CoreContainer.class},
                      new Object[] {this}));

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
        log.error("Exception while attempting to close old authorization plugin", e);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private void initializeAuditloggerPlugin(Map<String, Object> auditConf) {
    auditConf = Utils.getDeepCopy(auditConf, 4);
    int newVersion = readVersion(auditConf);
    // Initialize the Auditlog module
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
      newAuditloggerPlugin =
          new SecurityPluginHolder<>(
              newVersion, getResourceLoader().newInstance(klas, AuditLoggerPlugin.class));

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
        log.error("Exception while attempting to close old auditlogger plugin", e);
      }
    }
  }

  @SuppressWarnings({"unchecked"})
  private synchronized void initializeAuthenticationPlugin(
      Map<String, Object> authenticationConfig) {
    authenticationConfig = Utils.getDeepCopy(authenticationConfig, 4);
    int newVersion = readVersion(authenticationConfig);
    String pluginClassName = null;
    if (authenticationConfig != null) {
      if (authenticationConfig.containsKey("class")) {
        pluginClassName = String.valueOf(authenticationConfig.get("class"));
      } else {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "No 'class' specified for authentication in ZK.");
      }
    }

    if (pluginClassName != null) {
      log.debug("Authentication plugin class obtained from security.json: {}", pluginClassName);
    } else if (System.getProperty(AUTHENTICATION_PLUGIN_PROP) != null) {
      pluginClassName = System.getProperty(AUTHENTICATION_PLUGIN_PROP);
      log.debug(
          "Authentication plugin class obtained from system property '{}': {}",
          AUTHENTICATION_PLUGIN_PROP,
          pluginClassName);
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
      authenticationPlugin =
          new SecurityPluginHolder<>(
              newVersion,
              getResourceLoader()
                  .newInstance(
                      pluginClassName,
                      AuthenticationPlugin.class,
                      null,
                      new Class<?>[] {CoreContainer.class},
                      new Object[] {this}));
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
      log.error("Exception while attempting to close old authentication plugin", e);
    }
  }

  private void setupHttpClientForAuthPlugin(Object authcPlugin) {
    if (authcPlugin instanceof HttpClientBuilderPlugin) {
      // Setup HttpClient for internode communication
      HttpClientBuilderPlugin builderPlugin = ((HttpClientBuilderPlugin) authcPlugin);
      SolrHttpClientBuilder builder =
          builderPlugin.getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());

      // this caused plugins like KerberosPlugin to register its intercepts, but this intercept
      // logic is also handled by the pki authentication code when it decides to let the plugin
      // handle auth via its intercept - so you would end up with two intercepts
      // -->
      //  shardHandlerFactory.setSecurityBuilder(builderPlugin); // calls setup for the authcPlugin
      //  updateShardHandler.setSecurityBuilder(builderPlugin);
      // <--

      // This should not happen here at all - it's only currently required due to its effect on
      // http1 clients in a test or two incorrectly counting on it for their configuration.
      // -->

      SolrHttpClientContextBuilder httpClientBuilder = new SolrHttpClientContextBuilder();
      if (builder.getCredentialsProviderProvider() != null) {
        httpClientBuilder.setDefaultCredentialsProvider(
            new CredentialsProviderProvider() {

              @Override
              public CredentialsProvider getCredentialsProvider() {
                return builder.getCredentialsProviderProvider().getCredentialsProvider();
              }
            });
      }
      if (builder.getAuthSchemeRegistryProvider() != null) {
        httpClientBuilder.setAuthSchemeRegistryProvider(
            new AuthSchemeRegistryProvider() {

              @Override
              public Lookup<AuthSchemeProvider> getAuthSchemeRegistry() {
                return builder.getAuthSchemeRegistryProvider().getAuthSchemeRegistry();
              }
            });
      }

      HttpClientUtil.setHttpClientRequestContextBuilder(httpClientBuilder);

      // <--
    }

    // Always register PKI auth interceptor, which will then delegate the decision of who should
    // secure each request to the configured authentication plugin.
    if (pkiAuthenticationSecurityBuilder != null
        && !pkiAuthenticationSecurityBuilder.isInterceptorRegistered()) {
      pkiAuthenticationSecurityBuilder.getHttpClientBuilder(HttpClientUtil.getHttpClientBuilder());
      shardHandlerFactory.setSecurityBuilder(pkiAuthenticationSecurityBuilder);
      updateShardHandler.setSecurityBuilder(pkiAuthenticationSecurityBuilder);
    }
  }

  private static int readVersion(Map<String, Object> conf) {
    if (conf == null) return -1;
    Map<?, ?> meta = (Map<?, ?>) conf.get("");
    if (meta == null) return -1;
    Number v = (Number) meta.get("v");
    return v == null ? -1 : v.intValue();
  }

  /**
   * This method allows subclasses to construct a CoreContainer without any default init behavior.
   *
   * @param testConstructor pass (Object)null.
   * @lucene.experimental
   */
  protected CoreContainer(Object testConstructor) {
    solrHome = null;
    solrCores = null;
    nodeKeyPair = null;
    loader = null;
    coresLocator = null;
    cfg = null;
    containerProperties = null;
    replayUpdatesExecutor = null;
    distributedCollectionCommandRunner = Optional.empty();
    allowPaths = null;
    allowListUrlChecker = null;
  }

  public static CoreContainer createAndLoad(Path solrHome) {
    return createAndLoad(solrHome, solrHome.resolve(SolrXmlConfig.SOLR_XML_FILE));
  }

  /**
   * Create a new CoreContainer and load its cores
   *
   * @param solrHome the solr home directory
   * @param configFile the file containing this container's configuration
   * @return a loaded CoreContainer
   */
  public static CoreContainer createAndLoad(Path solrHome, Path configFile) {
    CoreContainer cc =
        new CoreContainer(SolrXmlConfig.fromFile(solrHome, configFile, new Properties()));
    try {
      cc.load();
    } catch (Exception e) {
      cc.shutdown();
      throw e;
    }
    return cc;
  }

  public Properties getContainerProperties() {
    return containerProperties;
  }

  public PKIAuthenticationPlugin getPkiAuthenticationSecurityBuilder() {
    return pkiAuthenticationSecurityBuilder;
  }

  public SolrMetricManager getMetricManager() {
    return metricManager;
  }

  public MetricsHandler getMetricsHandler() {
    return metricsHandler;
  }

  /** Never null but may implement {@link NoopTracer}. */
  public Tracer getTracer() {
    return tracer;
  }

  public OrderedExecutor getReplayUpdatesExecutor() {
    return replayUpdatesExecutor;
  }

  public SolrPackageLoader getPackageLoader() {
    return packageLoader;
  }

  public FileStoreAPI getFileStoreAPI() {
    return fileStoreAPI;
  }

  public SolrCache<?, ?> getCache(String name) {
    return caches.get(name);
  }

  public SolrClientCache getSolrClientCache() {
    return solrClientCache;
  }

  public ObjectCache getObjectCache() {
    return objectCache;
  }

  private void registerV2ApiIfEnabled(Object apiObject) {
    if (containerHandlers.getApiBag() == null) {
      return;
    }

    containerHandlers.getApiBag().registerObject(apiObject);
  }

  private void registerV2ApiIfEnabled(Class<? extends JerseyResource> clazz) {
    if (containerHandlers.getJerseyEndpoints() == null) {
      return;
    }

    containerHandlers.getJerseyEndpoints().register(clazz);
  }

  // -------------------------------------------------------------------
  // Initialization / Cleanup
  // -------------------------------------------------------------------

  /** Load the cores defined for this CoreContainer */
  @SuppressForbidden(
      reason =
          "Set the thread contextClassLoader for all 3rd party dependencies that we cannot control")
  public void load() {
    final ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Set the thread's contextClassLoader for any plugins that are loaded via Modules or Packages
      // and have dependencies that use the thread's contextClassLoader
      Thread.currentThread().setContextClassLoader(loader.getClassLoader());
      loadInternal();
    } finally {
      Thread.currentThread().setContextClassLoader(originalContextClassLoader);
    }
  }

  /** Load the cores defined for this CoreContainer */
  private void loadInternal() {
    if (log.isDebugEnabled()) {
      log.debug("Loading cores into CoreContainer [instanceDir={}]", getSolrHome());
    }
    logging = LogWatcher.newRegisteredLogWatcher(cfg.getLogWatcherConfig(), loader);

    ClusterEventProducerFactory clusterEventProducerFactory = new ClusterEventProducerFactory(this);
    clusterEventProducer = clusterEventProducerFactory;

    placementPluginFactory =
        new DelegatingPlacementPluginFactory(
            PlacementPluginFactoryLoader.getDefaultPlacementPluginFactory(cfg, loader));

    containerPluginsRegistry.registerListener(clusterSingletons.getPluginRegistryListener());
    containerPluginsRegistry.registerListener(
        clusterEventProducerFactory.getPluginRegistryListener());

    metricManager = new SolrMetricManager(loader, cfg.getMetricsConfig());
    String registryName = SolrMetricManager.getRegistryName(SolrInfoBean.Group.node);
    solrMetricsContext = new SolrMetricsContext(metricManager, registryName, metricTag);

    tracer = TracerConfigurator.loadTracer(loader, cfg.getTracerConfiguratorPluginInfo());

    coreContainerWorkExecutor =
        MetricUtils.instrumentedExecutorService(
            coreContainerWorkExecutor,
            null,
            metricManager.registry(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node)),
            SolrMetricManager.mkName(
                "coreContainerWorkExecutor",
                SolrInfoBean.Category.CONTAINER.toString(),
                "threadPool"));

    shardHandlerFactory =
        ShardHandlerFactory.newInstance(cfg.getShardHandlerFactoryPluginInfo(), loader);
    if (shardHandlerFactory instanceof SolrMetricProducer) {
      SolrMetricProducer metricProducer = (SolrMetricProducer) shardHandlerFactory;
      metricProducer.initializeMetrics(solrMetricsContext, "httpShardHandler");
    }

    updateShardHandler = new UpdateShardHandler(cfg.getUpdateShardHandlerConfig());
    updateShardHandler.initializeMetrics(solrMetricsContext, "updateShardHandler");

    solrClientCache = new SolrClientCache(updateShardHandler.getDefaultHttpClient());

    Map<String, CacheConfig> cachesConfig = cfg.getCachesConfig();
    if (cachesConfig.isEmpty()) {
      this.caches = Collections.emptyMap();
    } else {
      Map<String, SolrCache<?, ?>> m = CollectionUtil.newHashMap(cachesConfig.size());
      for (Map.Entry<String, CacheConfig> e : cachesConfig.entrySet()) {
        SolrCache<?, ?> c = e.getValue().newInstance();
        String cacheName = e.getKey();
        c.initializeMetrics(solrMetricsContext, "nodeLevelCache/" + cacheName);
        m.put(cacheName, c);
      }
      this.caches = Collections.unmodifiableMap(m);
    }

    StartupLoggingUtils.checkRequestLogging();

    hostName = cfg.getNodeName();

    zkSys.initZooKeeper(this, cfg.getCloudConfig());
    if (isZooKeeperAware()) {
      // initialize ZkClient metrics
      zkSys.getZkMetricsProducer().initializeMetrics(solrMetricsContext, "zkClient");
      pkiAuthenticationSecurityBuilder =
          new PKIAuthenticationPlugin(
              this,
              zkSys.getZkController().getNodeName(),
              (PublicKeyHandler) containerHandlers.get(PublicKeyHandler.PATH));
      pkiAuthenticationSecurityBuilder.initializeMetrics(solrMetricsContext, "/authentication/pki");

      fileStoreAPI = new FileStoreAPI(this);
      registerV2ApiIfEnabled(fileStoreAPI.readAPI);
      registerV2ApiIfEnabled(fileStoreAPI.writeAPI);

      packageLoader = new SolrPackageLoader(this);
      registerV2ApiIfEnabled(packageLoader.getPackageAPI().editAPI);
      registerV2ApiIfEnabled(packageLoader.getPackageAPI().readAPI);
      registerV2ApiIfEnabled(ZookeeperReadAPI.class);
    }

    MDCLoggingContext.setNode(this);

    securityConfHandler =
        isZooKeeperAware() ? new SecurityConfHandlerZk(this) : new SecurityConfHandlerLocal(this);
    reloadSecurityProperties();
    warnUsersOfInsecureSettings();
    this.backupRepoFactory = new BackupRepositoryFactory(cfg.getBackupRepositoryPlugins());
    coreConfigService = ConfigSetService.createConfigSetService(this);
    createHandler(ZK_PATH, ZookeeperInfoHandler.class.getName(), ZookeeperInfoHandler.class);
    createHandler(
        ZK_STATUS_PATH, ZookeeperStatusHandler.class.getName(), ZookeeperStatusHandler.class);

    // CoreContainer is initialized enough at this stage so we can set
    // distributedCollectionCommandRunner (the construction of
    // DistributedCollectionConfigSetCommandRunner uses Zookeeper so can't be done from the
    // CoreContainer constructor because there Zookeeper is not yet ready). Given this is used in
    // the CollectionsHandler created next line, this is the latest point where
    // distributedCollectionCommandRunner can be initialized without refactoring this method...
    // TODO: manage to completely build CoreContainer in the constructor and not in the load()
    // method... Requires some test refactoring.
    this.distributedCollectionCommandRunner =
        isZooKeeperAware() && cfg.getCloudConfig().getDistributedCollectionConfigSetExecution()
            ? Optional.of(new DistributedCollectionConfigSetCommandRunner(this))
            : Optional.empty();

    collectionsHandler =
        createHandler(
            COLLECTIONS_HANDLER_PATH, cfg.getCollectionsHandlerClass(), CollectionsHandler.class);
    configSetsHandler =
        createHandler(
            CONFIGSETS_HANDLER_PATH, cfg.getConfigSetsHandlerClass(), ConfigSetsHandler.class);
    ClusterAPI clusterAPI = new ClusterAPI(collectionsHandler, configSetsHandler);
    registerV2ApiIfEnabled(clusterAPI);
    registerV2ApiIfEnabled(clusterAPI.commands);

    if (isZooKeeperAware()) {
      registerV2ApiIfEnabled(new SchemaDesignerAPI(this));
    } // else Schema Designer not available in standalone (non-cloud) mode

    /*
     * HealthCheckHandler needs to be initialized before InfoHandler, since the later one will call CoreContainer.getHealthCheckHandler().
     * We don't register the handler here because it'll be registered inside InfoHandler
     */
    healthCheckHandler =
        loader.newInstance(
            cfg.getHealthCheckHandlerClass(),
            HealthCheckHandler.class,
            null,
            new Class<?>[] {CoreContainer.class},
            new Object[] {this});
    infoHandler = createHandler(INFO_HANDLER_PATH, cfg.getInfoHandlerClass(), InfoHandler.class);
    coreAdminHandler =
        createHandler(CORES_HANDLER_PATH, cfg.getCoreAdminHandlerClass(), CoreAdminHandler.class);

    Map<String, CoreAdminOp> coreAdminHandlerActions =
        cfg.getCoreAdminHandlerActions().entrySet().stream()
            .collect(
                Collectors.toMap(
                    item -> item.getKey(),
                    item -> loader.newInstance(item.getValue(), CoreAdminOp.class)));

    // Register custom actions for CoreAdminHandler
    coreAdminHandler.registerCustomActions(coreAdminHandlerActions);

    metricsHandler = new MetricsHandler(this);
    containerHandlers.put(METRICS_PATH, metricsHandler);
    metricsHandler.initializeMetrics(solrMetricsContext, METRICS_PATH);

    containerHandlers.put(AUTHZ_PATH, securityConfHandler);
    securityConfHandler.initializeMetrics(solrMetricsContext, AUTHZ_PATH);
    containerHandlers.put(AUTHC_PATH, securityConfHandler);

    PluginInfo[] metricReporters = cfg.getMetricsConfig().getMetricReporters();
    metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.node);
    metricManager.loadReporters(metricReporters, loader, this, null, null, SolrInfoBean.Group.jvm);
    metricManager.loadReporters(
        metricReporters, loader, this, null, null, SolrInfoBean.Group.jetty);

    containerProperties.putAll(cfg.getSolrProperties());

    // initialize gauges for reporting the number of cores and disk total/free

    solrMetricsContext.gauge(
        solrCores::getNumLoadedPermanentCores,
        true,
        "loaded",
        SolrInfoBean.Category.CONTAINER.toString(),
        "cores");
    solrMetricsContext.gauge(
        solrCores::getNumLoadedTransientCores,
        true,
        "lazy",
        SolrInfoBean.Category.CONTAINER.toString(),
        "cores");
    solrMetricsContext.gauge(
        solrCores::getNumUnloadedCores,
        true,
        "unloaded",
        SolrInfoBean.Category.CONTAINER.toString(),
        "cores");
    Path dataHome =
        cfg.getSolrDataHome() != null ? cfg.getSolrDataHome() : cfg.getCoreRootDirectory();
    solrMetricsContext.gauge(
        () -> dataHome.toFile().getTotalSpace(),
        true,
        "totalSpace",
        SolrInfoBean.Category.CONTAINER.toString(),
        "fs");
    solrMetricsContext.gauge(
        () -> dataHome.toFile().getUsableSpace(),
        true,
        "usableSpace",
        SolrInfoBean.Category.CONTAINER.toString(),
        "fs");
    solrMetricsContext.gauge(
        dataHome::toString, true, "path", SolrInfoBean.Category.CONTAINER.toString(), "fs");
    solrMetricsContext.gauge(
        () -> cfg.getCoreRootDirectory().toFile().getTotalSpace(),
        true,
        "totalSpace",
        SolrInfoBean.Category.CONTAINER.toString(),
        "fs",
        "coreRoot");
    solrMetricsContext.gauge(
        () -> cfg.getCoreRootDirectory().toFile().getUsableSpace(),
        true,
        "usableSpace",
        SolrInfoBean.Category.CONTAINER.toString(),
        "fs",
        "coreRoot");
    solrMetricsContext.gauge(
        () -> cfg.getCoreRootDirectory().toString(),
        true,
        "path",
        SolrInfoBean.Category.CONTAINER.toString(),
        "fs",
        "coreRoot");
    // add version information
    solrMetricsContext.gauge(
        () -> this.getClass().getPackage().getSpecificationVersion(),
        true,
        "specification",
        SolrInfoBean.Category.CONTAINER.toString(),
        "version");
    solrMetricsContext.gauge(
        () -> this.getClass().getPackage().getImplementationVersion(),
        true,
        "implementation",
        SolrInfoBean.Category.CONTAINER.toString(),
        "version");

    SolrFieldCacheBean fieldCacheBean = new SolrFieldCacheBean();
    fieldCacheBean.initializeMetrics(solrMetricsContext, null);

    if (isZooKeeperAware()) {
      metricManager.loadClusterReporters(metricReporters, this);
    }

    // setup executor to load cores in parallel
    ExecutorService coreLoadExecutor =
        MetricUtils.instrumentedExecutorService(
            ExecutorUtil.newMDCAwareFixedThreadPool(
                cfg.getCoreLoadThreadCount(isZooKeeperAware()),
                new SolrNamedThreadFactory("coreLoadExecutor")),
            null,
            metricManager.registry(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node)),
            SolrMetricManager.mkName(
                "coreLoadExecutor", SolrInfoBean.Category.CONTAINER.toString(), "threadPool"));
    final List<Future<SolrCore>> futures = new ArrayList<>();
    try {
      List<CoreDescriptor> cds = coresLocator.discover(this);
      cds = CoreSorter.sortCores(this, cds);
      checkForDuplicateCoreNames(cds);
      status |= CORE_DISCOVERY_COMPLETE;

      for (final CoreDescriptor cd : cds) {
        if (cd.isTransient() || !cd.isLoadOnStartup()) {
          solrCores.addCoreDescriptor(cd);
        } else if (asyncSolrCoreLoad) {
          solrCores.markCoreAsLoading(cd);
        }
        if (cd.isLoadOnStartup()) {
          futures.add(
              coreLoadExecutor.submit(
                  () -> {
                    SolrCore core;
                    try {
                      if (zkSys.getZkController() != null) {
                        zkSys.getZkController().throwErrorIfReplicaReplaced(cd);
                      }
                      solrCores.waitAddPendingCoreOps(cd.getName());
                      core = createFromDescriptor(cd, false, false);
                    } finally {
                      solrCores.removeFromPendingOps(cd.getName());
                      if (asyncSolrCoreLoad) {
                        solrCores.markCoreAsNotLoading(cd);
                      }
                    }
                    try {
                      zkSys.registerInZk(core, true, false);
                    } catch (RuntimeException e) {
                      log.error("Error registering SolrCore", e);
                    }
                    return core;
                  }));
        }
      }

      // Start the background thread
      backgroundCloser = new CloserThread(this, solrCores, cfg);
      backgroundCloser.start();

    } finally {
      if (asyncSolrCoreLoad && futures != null) {

        coreContainerWorkExecutor.submit(
            () -> {
              try {
                for (Future<SolrCore> future : futures) {
                  try {
                    future.get();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (ExecutionException e) {
                    log.error("Error waiting for SolrCore to be loaded on startup", e);
                  }
                }
              } finally {
                ExecutorUtil.shutdownAndAwaitTermination(coreLoadExecutor);
              }
            });
      } else {
        ExecutorUtil.shutdownAndAwaitTermination(coreLoadExecutor);
      }
    }

    if (isZooKeeperAware()) {
      containerPluginsRegistry.refresh();
      getZkController().zkStateReader.registerClusterPropertiesListener(containerPluginsRegistry);
      ContainerPluginsApi containerPluginsApi = new ContainerPluginsApi(this);
      registerV2ApiIfEnabled(containerPluginsApi.readAPI);
      registerV2ApiIfEnabled(containerPluginsApi.editAPI);

      // initialize the placement plugin factory wrapper
      // with the plugin configuration from the registry
      PlacementPluginFactoryLoader.load(placementPluginFactory, containerPluginsRegistry);

      // create target ClusterEventProducer (possibly from plugins)
      clusterEventProducer = clusterEventProducerFactory.create(containerPluginsRegistry);

      // init ClusterSingleton-s

      // register the handlers that are also ClusterSingleton
      containerHandlers
          .keySet()
          .forEach(
              handlerName -> {
                SolrRequestHandler handler = containerHandlers.get(handlerName);
                if (handler instanceof ClusterSingleton) {
                  ClusterSingleton singleton = (ClusterSingleton) handler;
                  clusterSingletons.getSingletons().put(singleton.getName(), singleton);
                }
              });
    }

    if (V2ApiUtils.isEnabled()) {
      final CoreContainer thisCCRef = this;
      // Init the Jersey app once all CC endpoints have been registered
      containerHandlers
          .getJerseyEndpoints()
          .register(
              new AbstractBinder() {
                @Override
                protected void configure() {
                  bindFactory(new InjectionFactories.SingletonFactory<>(thisCCRef))
                      .to(CoreContainer.class)
                      .in(Singleton.class);
                }
              })
          .register(
              new AbstractBinder() {
                @Override
                protected void configure() {
                  bindFactory(new InjectionFactories.SingletonFactory<>(nodeKeyPair))
                      .to(SolrNodeKeyPair.class)
                      .in(Singleton.class);
                }
              })
          .register(
              new AbstractBinder() {
                @Override
                protected void configure() {
                  bindFactory(
                          new InjectionFactories.SingletonFactory<>(
                              coreAdminHandler.getCoreAdminAsyncTracker()))
                      .to(CoreAdminHandler.CoreAdminAsyncTracker.class)
                      .in(Singleton.class);
                }
              });
      jerseyAppHandler = new ApplicationHandler(containerHandlers.getJerseyEndpoints());
    }

    // Do Node setup logic after all handlers have been registered.
    if (isZooKeeperAware()) {
      clusterSingletons.setReady();
      if (NodeRoles.MODE_PREFERRED.equals(nodeRoles.getRoleMode(NodeRoles.Role.OVERSEER))) {
        try {
          log.info("This node has been started as a preferred overseer");
          zkSys.getZkController().setPreferredOverseer();
        } catch (KeeperException | InterruptedException e) {
          throw new SolrException(ErrorCode.SERVER_ERROR, e);
        }
      }
      if (!distributedCollectionCommandRunner.isPresent()) {
        zkSys.getZkController().checkOverseerDesignate();
      }
    }

    // This is a bit redundant but these are two distinct concepts for all they're accomplished at
    // the same time.
    status |= LOAD_COMPLETE | INITIAL_CORE_LOAD_COMPLETE;
  }

  public void securityNodeChanged() {
    log.info("Security node changed, reloading security.json");
    reloadSecurityProperties();
  }

  /** Make sure securityConfHandler is initialized */
  @SuppressWarnings({"unchecked"})
  private void reloadSecurityProperties() {
    SecurityConfHandler.SecurityConfig securityConfig =
        securityConfHandler.getSecurityConfig(false);
    initializeAuthorizationPlugin(
        (Map<String, Object>) securityConfig.getData().get("authorization"));
    initializeAuthenticationPlugin(
        (Map<String, Object>) securityConfig.getData().get("authentication"));
    initializeAuditloggerPlugin((Map<String, Object>) securityConfig.getData().get("auditlogging"));
  }

  private void warnUsersOfInsecureSettings() {
    if (authenticationPlugin == null || authorizationPlugin == null) {
      log.warn(
          "Not all security plugins configured!  authentication={} authorization={}.  Solr is only as secure as "
              + "you make it. Consider configuring authentication/authorization before exposing Solr to users internal or "
              + "external.  See https://s.apache.org/solrsecurity for more info",
          (authenticationPlugin != null) ? "enabled" : "disabled",
          (authorizationPlugin != null) ? "enabled" : "disabled");
    }

    if (authenticationPlugin != null
        && StrUtils.isNullOrEmpty(System.getProperty("solr.jetty.https.port"))) {
      log.warn(
          "Solr authentication is enabled, but SSL is off.  Consider enabling SSL to protect user credentials and data with encryption.");
    }
  }

  private static void checkForDuplicateCoreNames(List<CoreDescriptor> cds) {
    Map<String, Path> addedCores = new HashMap<>();
    for (CoreDescriptor cd : cds) {
      final String name = cd.getName();
      if (addedCores.containsKey(name))
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            String.format(
                Locale.ROOT,
                "Found multiple cores with the name [%s], with instancedirs [%s] and [%s]",
                name,
                addedCores.get(name),
                cd.getInstanceDir()));
      addedCores.put(name, cd.getInstanceDir());
    }
  }

  private volatile boolean isShutDown = false;

  public boolean isShutDown() {
    return isShutDown;
  }

  public void shutdown() {

    ZkController zkController = getZkController();
    if (zkController != null) {
      if (distributedCollectionCommandRunner.isPresent()) {
        // Local (i.e. distributed) Collection API processing
        distributedCollectionCommandRunner.get().stopAndWaitForPendingTasksToComplete();
      } else {
        // Overseer based processing
        OverseerTaskQueue overseerCollectionQueue = zkController.getOverseerCollectionQueue();
        overseerCollectionQueue.allowOverseerPendingTasksToComplete();
      }
    }
    if (log.isInfoEnabled()) {
      log.info("Shutting down CoreContainer instance={}", System.identityHashCode(this));
    }

    ExecutorUtil.shutdownAndAwaitTermination(coreContainerAsyncTaskExecutor);
    ExecutorService customThreadPool =
        ExecutorUtil.newMDCAwareCachedThreadPool(new SolrNamedThreadFactory("closeThreadPool"));

    isShutDown = true;
    try {
      if (isZooKeeperAware()) {
        cancelCoreRecoveries();
        zkSys.zkController.preClose();
      }
      pauseUpdatesAndAwaitInflightRequests();
      if (isZooKeeperAware()) {
        zkSys.zkController.tryCancelAllElections();
      }

      ExecutorUtil.shutdownAndAwaitTermination(coreContainerWorkExecutor);

      // First wake up the closer thread, it'll terminate almost immediately since it checks
      // isShutDown.
      synchronized (solrCores.getModifyLock()) {
        solrCores.getModifyLock().notifyAll(); // wake up anyone waiting
      }
      if (backgroundCloser
          != null) { // Doesn't seem right, but tests get in here without initializing the core.
        try {
          while (true) {
            backgroundCloser.join(15000);
            if (backgroundCloser.isAlive()) {
              synchronized (solrCores.getModifyLock()) {
                solrCores.getModifyLock().notifyAll(); // there is a race we have to protect against
              }
            } else {
              break;
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          if (log.isDebugEnabled()) {
            log.debug("backgroundCloser thread was interrupted before finishing");
          }
        }
      }
      // Now clear all the cores that are being operated upon.
      solrCores.close();

      final Map<String, SolrCache<?, ?>> closeCaches = caches;
      if (closeCaches != null) {
        for (Map.Entry<String, SolrCache<?, ?>> e : caches.entrySet()) {
          try {
            e.getValue().close();
          } catch (Exception ex) {
            log.warn("error closing node-level cache: {}", e.getKey(), ex);
          }
        }
      }

      objectCache.clear();

      // It's still possible that one of the pending dynamic load operation is waiting, so wake it
      // up if so. Since all the pending operations queues have been drained, there should be
      // nothing to do.
      synchronized (solrCores.getModifyLock()) {
        solrCores.getModifyLock().notifyAll(); // wake up the thread
      }

      customThreadPool.submit(
          () -> {
            replayUpdatesExecutor.shutdownAndAwaitTermination();
          });

      if (metricManager != null) {
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.node));
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm));
        metricManager.closeReporters(SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty));

        metricManager.unregisterGauges(
            SolrMetricManager.getRegistryName(SolrInfoBean.Group.node), metricTag);
        metricManager.unregisterGauges(
            SolrMetricManager.getRegistryName(SolrInfoBean.Group.jvm), metricTag);
        metricManager.unregisterGauges(
            SolrMetricManager.getRegistryName(SolrInfoBean.Group.jetty), metricTag);
      }

      if (isZooKeeperAware()) {
        cancelCoreRecoveries();

        if (metricManager != null) {
          metricManager.closeReporters(
              SolrMetricManager.getRegistryName(SolrInfoBean.Group.cluster));
        }
      }

      try {
        if (coreAdminHandler != null) {
          customThreadPool.submit(
              () -> {
                coreAdminHandler.shutdown();
              });
        }
      } catch (Exception e) {
        log.warn("Error shutting down CoreAdminHandler. Continuing to close CoreContainer.", e);
      }
      if (solrClientCache != null) {
        solrClientCache.close();
      }
      if (containerPluginsRegistry != null) {
        IOUtils.closeQuietly(containerPluginsRegistry);
      }

    } finally {
      try {
        if (shardHandlerFactory != null) {
          customThreadPool.submit(
              () -> {
                shardHandlerFactory.close();
              });
        }
      } finally {
        try {
          if (updateShardHandler != null) {
            customThreadPool.submit(updateShardHandler::close);
          }
        } finally {
          try {
            // we want to close zk stuff last
            zkSys.close();
          } finally {
            ExecutorUtil.shutdownAndAwaitTermination(customThreadPool);
          }
        }
      }
    }

    // It should be safe to close the authorization plugin at this point.
    try {
      if (authorizationPlugin != null) {
        authorizationPlugin.plugin.close();
      }
    } catch (IOException e) {
      log.warn("Exception while closing authorization plugin.", e);
    }

    // It should be safe to close the authentication plugin at this point.
    try {
      if (authenticationPlugin != null) {
        authenticationPlugin.plugin.close();
        authenticationPlugin = null;
      }
    } catch (Exception e) {
      log.warn("Exception while closing authentication plugin.", e);
    }

    // It should be safe to close the auditlogger plugin at this point.
    try {
      if (auditloggerPlugin != null) {
        auditloggerPlugin.plugin.close();
        auditloggerPlugin = null;
      }
    } catch (Exception e) {
      log.warn("Exception while closing auditlogger plugin.", e);
    }

    if (packageLoader != null) {
      org.apache.lucene.util.IOUtils.closeWhileHandlingException(packageLoader);
    }
    org.apache.lucene.util.IOUtils.closeWhileHandlingException(loader); // best effort

    tracer.close();
  }

  public void cancelCoreRecoveries() {

    List<SolrCore> cores = solrCores.getCores();

    // we must cancel without holding the cores sync
    // make sure we wait for any recoveries to stop
    for (SolrCore core : cores) {
      try {
        core.getSolrCoreState().cancelRecovery();
      } catch (Exception e) {
        log.error("Error canceling recovery for core", e);
      }
    }
  }

  /**
   * Pause updates for all cores on this node and wait for all in-flight update requests to finish.
   * Here, we (slightly) delay leader election so that in-flight update requests succeed and we can
   * preserve consistency.
   *
   * <p>Jetty already allows a grace period for in-flight requests to complete and our solr cores,
   * searchers, etc., are reference counted to allow for graceful shutdown. So we don't worry about
   * any other kind of requests.
   *
   * <p>We do not need to unpause ever because the node is being shut down.
   */
  private void pauseUpdatesAndAwaitInflightRequests() {
    getCores().parallelStream()
        .forEach(
            solrCore -> {
              SolrCoreState solrCoreState = solrCore.getSolrCoreState();
              try {
                solrCoreState.pauseUpdatesAndAwaitInflightRequests();
              } catch (TimeoutException e) {
                log.warn(
                    "Timed out waiting for in-flight update requests to complete for core: {}",
                    solrCore.getName());
              } catch (InterruptedException e) {
                log.warn(
                    "Interrupted while waiting for in-flight update requests to complete for core: {}",
                    solrCore.getName());
                Thread.currentThread().interrupt();
              }
            });
  }

  public CoresLocator getCoresLocator() {
    return coresLocator;
  }

  protected SolrCore registerCore(
      CoreDescriptor cd, SolrCore core, boolean registerInZk, boolean skipRecovery) {
    if (core == null) {
      throw new RuntimeException("Can not register a null core.");
    }

    if (isShutDown) {
      core.close();
      throw new IllegalStateException("This CoreContainer has been closed");
    }

    assert core.getName().equals(cd.getName())
        : "core name " + core.getName() + " != cd " + cd.getName();

    SolrCore old = solrCores.putCore(cd, core);

    coreInitFailures.remove(cd.getName());

    if (old == null || old == core) {
      if (log.isDebugEnabled()) {
        log.debug("registering core: {}", cd.getName());
      }
      if (registerInZk) {
        zkSys.registerInZk(core, false, skipRecovery);
      }
      return null;
    } else {
      if (log.isDebugEnabled()) {
        log.debug("replacing core: {}", cd.getName());
      }
      old.close();
      if (registerInZk) {
        zkSys.registerInZk(core, false, skipRecovery);
      }
      return old;
    }
  }

  /**
   * Creates a new core, publishing the core state to the cluster
   *
   * @param coreName the core name
   * @param parameters the core parameters
   * @return the newly created core
   */
  public SolrCore create(String coreName, Map<String, String> parameters) {
    return create(coreName, cfg.getCoreRootDirectory().resolve(coreName), parameters, false);
  }

  final Set<String> inFlightCreations = ConcurrentHashMap.newKeySet(); // See SOLR-14969

  /**
   * Creates a new core in a specified instance directory, publishing the core state to the cluster
   *
   * @param coreName the core name
   * @param instancePath the instance directory
   * @param parameters the core parameters
   * @return the newly created core
   */
  public SolrCore create(
      String coreName, Path instancePath, Map<String, String> parameters, boolean newCollection) {
    boolean iAdded = false;
    try {
      iAdded = inFlightCreations.add(coreName);
      if (!iAdded) {
        String msg = "Already creating a core with name '" + coreName + "', call aborted '";
        log.warn(msg);
        throw new SolrException(ErrorCode.CONFLICT, msg);
      }
      CoreDescriptor cd =
          new CoreDescriptor(
              coreName, instancePath, parameters, getContainerProperties(), getZkController());

      // Since the core descriptor is removed when a core is unloaded, it should never be anywhere
      // when a core is created.
      if (getCoreDescriptor(coreName) != null) {
        log.warn("Creating a core with existing name is not allowed: '{}'", coreName);
        // TODO: Shouldn't this be a BAD_REQUEST?
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Core with name '" + coreName + "' already exists.");
      }

      // Validate paths are relative to known locations to avoid path traversal
      assertPathAllowed(cd.getInstanceDir());
      assertPathAllowed(Paths.get(cd.getDataDir()));

      boolean preExistingZkEntry = false;
      try {
        if (getZkController() != null) {
          if (cd.getCloudDescriptor().getCoreNodeName() == null) {
            throw new SolrException(
                ErrorCode.SERVER_ERROR, "coreNodeName missing " + parameters.toString());
          }
          preExistingZkEntry = getZkController().checkIfCoreNodeNameAlreadyExists(cd);
        }

        // Much of the logic in core handling pre-supposes that the core.properties file already
        // exists, so create it first and clean it up if there's an error.
        coresLocator.create(this, cd);

        SolrCore core;
        try {
          solrCores.waitAddPendingCoreOps(cd.getName());
          core = createFromDescriptor(cd, true, newCollection);
          // Write out the current core properties in case anything changed when the core was
          // created
          coresLocator.persist(this, cd);
        } finally {
          solrCores.removeFromPendingOps(cd.getName());
        }

        return core;
      } catch (Exception ex) {
        // First clean up any core descriptor, there should never be an existing core.properties
        // file for any core that failed to be created on-the-fly.
        coresLocator.delete(this, cd);
        if (isZooKeeperAware() && !preExistingZkEntry) {
          try {
            getZkController().unregister(coreName, cd);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("interrupted", e);
          } catch (KeeperException e) {
            log.error("KeeperException unregistering core {}", coreName, e);
          } catch (Exception e) {
            log.error("Exception unregistering core {}", coreName, e);
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

        throw new SolrException(
            SolrException.ErrorCode.BAD_REQUEST,
            "Error CREATEing SolrCore '" + coreName + "': " + ex.getMessage() + rootMsg,
            ex);
      }
    } finally {
      if (iAdded) {
        inFlightCreations.remove(coreName);
      }
    }
  }

  /**
   * Checks that the given path is relative to SOLR_HOME, SOLR_DATA_HOME, coreRootDirectory or one
   * of the paths specified in solr.xml's allowPaths element. Delegates to {@link
   * SolrPaths#assertPathAllowed(Path, Set)}
   *
   * @param pathToAssert path to check
   * @throws SolrException if path is outside allowed paths
   */
  public void assertPathAllowed(Path pathToAssert) throws SolrException {
    SolrPaths.assertPathAllowed(pathToAssert, allowPaths);
  }

  /**
   * Return the file system paths that should be allowed for various API requests. This list is
   * compiled at startup from SOLR_HOME, SOLR_DATA_HOME and the <code>allowPaths</code>
   * configuration of solr.xml. These paths are used by the {@link #assertPathAllowed(Path)} method
   * call.
   *
   * <p><b>NOTE:</b> This method is currently only in use in tests in order to modify the mutable
   * Set directly. Please treat this as a private method.
   */
  @VisibleForTesting
  public Set<Path> getAllowPaths() {
    return allowPaths;
  }

  /** Gets the URLs checker based on the {@code allowUrls} configuration of solr.xml. */
  public AllowListUrlChecker getAllowListUrlChecker() {
    return allowListUrlChecker;
  }

  /**
   * Creates a new core based on a CoreDescriptor.
   *
   * @param dcore a core descriptor
   * @param publishState publish core state to the cluster if true
   *     <p>WARNING: Any call to this method should be surrounded by a try/finally block that calls
   *     solrCores.waitAddPendingCoreOps(...) and solrCores.removeFromPendingOps(...)
   *     <pre>
   *                                                               <code>
   *                                                               try {
   *                                                                  solrCores.waitAddPendingCoreOps(dcore.getName());
   *                                                                  createFromDescriptor(...);
   *                                                               } finally {
   *                                                                  solrCores.removeFromPendingOps(dcore.getName());
   *                                                               }
   *                                                               </code>
   *                                                             </pre>
   *     <p>Trying to put the waitAddPending... in this method results in Bad Things Happening due
   *     to race conditions. getCore() depends on getting the core returned _if_ it's in the pending
   *     list due to some other thread opening it. If the core is not in the pending list and not
   *     loaded, then getCore() calls this method. Anything that called to check if the core was
   *     loaded _or_ in pending ops and, based on the return called createFromDescriptor would
   *     introduce a race condition, see getCore() for the place it would be a problem
   * @return the newly created core
   */
  @SuppressWarnings("resource")
  private SolrCore createFromDescriptor(
      CoreDescriptor dcore, boolean publishState, boolean newCollection) {

    if (isShutDown) {
      throw new SolrException(ErrorCode.SERVICE_UNAVAILABLE, "Solr has been shutdown.");
    }

    SolrCore core = null;
    try {
      MDCLoggingContext.setCoreDescriptor(this, dcore);
      SolrIdentifierValidator.validateCoreName(dcore.getName());
      if (zkSys.getZkController() != null) {
        zkSys.getZkController().preRegister(dcore, publishState);
      }

      ConfigSet coreConfig = coreConfigService.loadConfigSet(dcore);
      dcore.setConfigSetTrusted(coreConfig.isTrusted());
      if (log.isInfoEnabled()) {
        log.info(
            "Creating SolrCore '{}' using configuration from {}, trusted={}",
            dcore.getName(),
            coreConfig.getName(),
            dcore.isConfigSetTrusted());
      }
      try {
        core = new SolrCore(this, dcore, coreConfig);
      } catch (SolrException e) {
        core = processCoreCreateException(e, dcore, coreConfig);
      }

      // always kick off recovery if we are in non-Cloud mode
      if (!isZooKeeperAware() && core.getUpdateHandler().getUpdateLog() != null) {
        core.getUpdateHandler().getUpdateLog().recoverFromLog();
      }

      registerCore(dcore, core, publishState, newCollection);

      return core;
    } catch (Exception e) {
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      if (e instanceof ZkController.NotInClusterStateException && !newCollection) {
        // this mostly happens when the core is deleted when this node is down
        // but it can also happen if connecting to the wrong zookeeper
        final boolean deleteUnknownCores =
            Boolean.parseBoolean(System.getProperty("solr.deleteUnknownCores", "false"));
        log.error(
            "SolrCore {} in {} is not in cluster state.{}",
            dcore.getName(),
            dcore.getInstanceDir(),
            (deleteUnknownCores
                ? " It will be deleted. See SOLR-13396 for more information."
                : ""));
        unload(dcore.getName(), deleteUnknownCores, deleteUnknownCores, deleteUnknownCores);
        throw e;
      }
      solrCores.removeCoreDescriptor(dcore);
      final SolrException solrException =
          new SolrException(
              ErrorCode.SERVER_ERROR, "Unable to create core [" + dcore.getName() + "]", e);
      if (core != null && !core.isClosed()) IOUtils.closeQuietly(core);
      throw solrException;
    } catch (Throwable t) {
      SolrException e =
          new SolrException(
              ErrorCode.SERVER_ERROR,
              "JVM Error creating core [" + dcore.getName() + "]: " + t.getMessage(),
              t);
      coreInitFailures.put(dcore.getName(), new CoreLoadFailure(dcore, e));
      solrCores.removeCoreDescriptor(dcore);
      if (core != null && !core.isClosed()) IOUtils.closeQuietly(core);
      throw t;
    } finally {
      MDCLoggingContext.clear();
    }
  }

  public boolean isSharedFs(CoreDescriptor cd) {
    try (SolrCore core = this.getCore(cd.getName())) {
      if (core != null) {
        return core.getDirectoryFactory().isSharedStorage();
      } else {
        ConfigSet configSet = coreConfigService.loadConfigSet(cd);
        return DirectoryFactory.loadDirectoryFactory(configSet.getSolrConfig(), this, null)
            .isSharedStorage();
      }
    }
  }

  /**
   * Take action when we failed to create a SolrCore. If error is due to corrupt index, try to
   * recover. Various recovery strategies can be specified via system properties
   * "-DCoreInitFailedAction={fromleader, none}"
   *
   * @param original the problem seen when loading the core the first time.
   * @param dcore core descriptor for the core to create
   * @param coreConfig core config for the core to create
   * @return if possible
   * @throws SolrException rethrows the original exception if we will not attempt to recover, throws
   *     a new SolrException with the original exception as a suppressed exception if there is a
   *     second problem creating the solr core.
   * @see CoreInitFailedAction
   */
  private SolrCore processCoreCreateException(
      SolrException original, CoreDescriptor dcore, ConfigSet coreConfig) {
    // Traverse full chain since CIE may not be root exception
    Throwable cause = original;
    while ((cause = cause.getCause()) != null) {
      if (cause instanceof CorruptIndexException) {
        break;
      }
    }

    // If no CorruptIndexException, nothing we can try here
    if (cause == null) throw original;

    CoreInitFailedAction action =
        CoreInitFailedAction.valueOf(
            System.getProperty(CoreInitFailedAction.class.getSimpleName(), "none"));
    log.debug("CorruptIndexException while creating core, will attempt to repair via {}", action);

    switch (action) {
      case fromleader: // Recovery from leader on a CorruptedIndexException
        if (isZooKeeperAware()) {
          CloudDescriptor desc = dcore.getCloudDescriptor();
          try {
            Replica leader =
                getZkController()
                    .getClusterState()
                    .getCollection(desc.getCollectionName())
                    .getSlice(desc.getShardId())
                    .getLeader();
            if (leader != null && leader.getState() == State.ACTIVE) {
              log.info("Found active leader, will attempt to create fresh core and recover.");
              resetIndexDirectory(dcore, coreConfig);
              // the index of this core is emptied, its term should be set to 0
              getZkController()
                  .getShardTerms(desc.getCollectionName(), desc.getShardId())
                  .setTermToZero(desc.getCoreNodeName());
              return new SolrCore(this, dcore, coreConfig);
            }
          } catch (SolrException se) {
            se.addSuppressed(original);
            throw se;
          }
        }
        throw original;
      case none:
        throw original;
      default:
        log.warn(
            "Failed to create core, and did not recognize specified 'CoreInitFailedAction': [{}]. Valid options are {}.",
            action,
            Arrays.asList(CoreInitFailedAction.values()));
        throw original;
    }
  }

  /** Write a new index directory for the SolrCore, but do so without loading it. */
  private void resetIndexDirectory(CoreDescriptor dcore, ConfigSet coreConfig) {
    SolrConfig config = coreConfig.getSolrConfig();

    String registryName =
        SolrMetricManager.getRegistryName(SolrInfoBean.Group.core, dcore.getName());
    DirectoryFactory df = DirectoryFactory.loadDirectoryFactory(config, this, registryName);
    String dataDir = SolrCore.findDataDir(df, null, config, dcore);

    String tmpIdxDirName =
        "index." + new SimpleDateFormat(SnapShooter.DATE_FMT, Locale.ROOT).format(new Date());
    SolrCore.modifyIndexProps(df, dataDir, config, tmpIdxDirName);

    // Free the directory object that we had to create for this
    Directory dir = null;
    try {
      dir = df.get(dataDir, DirContext.META_DATA, config.indexConfig.lockType);
    } catch (IOException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, e);
    } finally {
      try {
        df.release(dir);
        df.doneWithDirectory(dir);
      } catch (IOException e) {
        log.error("Exception releasing {}", dir, e);
      }
    }
  }

  /**
   * Gets all loaded cores, consistent with {@link #getLoadedCoreNames()}. Caller doesn't need to
   * close.
   *
   * <p>NOTE: rather dangerous API because each core is not reserved (could in theory be closed).
   * Prefer {@link #getLoadedCoreNames()} and then call {@link #getCore(String)} then close it.
   *
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it
   *     can be sorted). Don't need to close them.
   */
  @Deprecated
  public List<SolrCore> getCores() {
    return solrCores.getCores();
  }

  /**
   * Gets the permanent and transient cores that are currently loaded, i.e. cores that have 1:
   * loadOnStartup=true and are either not-transient or, if transient, have been loaded and have not
   * been aged out 2: loadOnStartup=false and have been loaded but are either non-transient or have
   * not been aged out.
   *
   * <p>Put another way, this will not return any names of cores that are lazily loaded but have not
   * been called for yet or are transient and either not loaded or have been swapped out.
   *
   * <p>For efficiency, prefer to check {@link #isLoaded(String)} instead of {@link
   * #getLoadedCoreNames()}.contains(coreName).
   *
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it
   *     can be sorted).
   */
  public List<String> getLoadedCoreNames() {
    return solrCores.getLoadedCoreNames();
  }

  /**
   * Gets a collection of all the cores, permanent and transient, that are currently known, whether
   * they are loaded or not.
   *
   * <p>For efficiency, prefer to check {@link #getCoreDescriptor(String)} != null instead of {@link
   * #getAllCoreNames()}.contains(coreName).
   *
   * @return An unsorted list. This list is a new copy, it can be modified by the caller (e.g. it
   *     can be sorted).
   */
  public List<String> getAllCoreNames() {
    return solrCores.getAllCoreNames();
  }

  /**
   * Gets the total number of cores, including permanent and transient cores, loaded and unloaded
   * cores. Faster equivalent for {@link #getAllCoreNames()}.size().
   */
  public int getNumAllCores() {
    return solrCores.getNumAllCores();
  }

  /**
   * Returns an immutable Map of Exceptions that occurred when initializing SolrCores (either at
   * startup, or do to runtime requests to create cores) keyed off of the name (String) of the
   * SolrCore that had the Exception during initialization.
   *
   * <p>While the Map returned by this method is immutable and will not change once returned to the
   * client, the source data used to generate this Map can be changed as various SolrCore operations
   * are performed:
   *
   * <ul>
   *   <li>Failed attempts to create new SolrCores will add new Exceptions.
   *   <li>Failed attempts to re-create a SolrCore using a name already contained in this Map will
   *       replace the Exception.
   *   <li>Failed attempts to reload a SolrCore will cause an Exception to be added to this list --
   *       even though the existing SolrCore with that name will continue to be available.
   *   <li>Successful attempts to re-created a SolrCore using a name already contained in this Map
   *       will remove the Exception.
   *   <li>Registering an existing SolrCore with a name already contained in this Map (ie: ALIAS or
   *       SWAP) will remove the Exception.
   * </ul>
   */
  public Map<String, CoreLoadFailure> getCoreInitFailures() {
    return Map.copyOf(coreInitFailures);
  }

  // ---------------- Core name related methods ---------------

  private CoreDescriptor reloadCoreDescriptor(CoreDescriptor oldDesc) {
    if (oldDesc == null) {
      return null;
    }

    CoreDescriptor ret = getCoresLocator().reload(oldDesc, this);

    // Ok, this little jewel is all because we still create core descriptors on the fly from lists
    // of properties in tests particularly. Theoretically, there should be _no_ way to create a
    // CoreDescriptor in the new world of core discovery without writing the core.properties file
    // out first.
    //
    // TODO: remove core.properties from the conf directory in test files, it's in a bad place there
    // anyway.
    if (ret == null) {
      // there may be changes to extra properties that we need to pick up.
      oldDesc.loadExtraProperties();
      return oldDesc;
    }
    // The CloudDescriptor bit here is created in a very convoluted way, requiring access to private
    // methods in ZkController. When reloading, this behavior is identical to what used to happen
    // where a copy of the old CoreDescriptor was just re-used.

    if (ret.getCloudDescriptor() != null) {
      ret.getCloudDescriptor().reload(oldDesc.getCloudDescriptor());
    }

    return ret;
  }

  /** reloads a core refer {@link CoreContainer#reload(String, UUID)} for details */
  public void reload(String name) {
    reload(name, null);
  }

  /**
   * Recreates a SolrCore. While the new core is loading, requests will continue to be dispatched to
   * and processed by the old core
   *
   * @param name the name of the SolrCore to reload
   * @param coreId The unique Id of the core {@link SolrCore#uniqueId}. If this is null, it's
   *     reloaded anyway. If the current core has a different id, this is a no-op
   */
  public void reload(String name, UUID coreId) {
    if (isShutDown) {
      throw new AlreadyClosedException();
    }
    SolrCore newCore = null;
    SolrCore core = solrCores.getCoreFromAnyList(name, false, coreId);
    if (core != null) {
      // The underlying core properties files may have changed, we don't really know. So we have a
      // (perhaps) stale CoreDescriptor and we need to reload it from the disk files
      CoreDescriptor cd = reloadCoreDescriptor(core.getCoreDescriptor());
      solrCores.addCoreDescriptor(cd);
      boolean success = false;
      try {
        solrCores.waitAddPendingCoreOps(cd.getName());
        ConfigSet coreConfig = coreConfigService.loadConfigSet(cd);
        if (log.isInfoEnabled()) {
          log.info(
              "Reloading SolrCore '{}' using configuration from {}",
              cd.getName(),
              coreConfig.getName());
        }
        newCore = core.reload(coreConfig);

        DocCollection docCollection = null;
        if (getZkController() != null) {
          docCollection = getZkController().getClusterState().getCollection(cd.getCollectionName());
          // turn off indexing now, before the new core is registered
          if (docCollection.getBool(ZkStateReader.READ_ONLY, false)) {
            newCore.readOnly = true;
          }
        }

        registerCore(cd, newCore, false, false);

        // force commit on old core if the new one is readOnly and prevent any new updates
        if (newCore.readOnly) {
          RefCounted<IndexWriter> iwRef = core.getSolrCoreState().getIndexWriter(null);
          if (iwRef != null) {
            IndexWriter iw = iwRef.get();
            // switch old core to readOnly
            core.readOnly = true;
            try {
              if (iw != null) {
                iw.commit();
              }
            } finally {
              iwRef.decref();
            }
          }
        }

        if (docCollection != null) {
          Replica replica = docCollection.getReplica(cd.getCloudDescriptor().getCoreNodeName());
          assert replica != null : cd.getCloudDescriptor().getCoreNodeName() + " had no replica";
          if (replica.getType() == Replica.Type.TLOG) { // TODO: needed here?
            getZkController().stopReplicationFromLeader(core.getName());
            if (!cd.getCloudDescriptor().isLeader()) {
              getZkController().startReplicationFromLeader(newCore.getName(), true);
            }

          } else if (replica.getType() == Replica.Type.PULL) {
            getZkController().stopReplicationFromLeader(core.getName());
            getZkController().startReplicationFromLeader(newCore.getName(), false);
          }
        }
        success = true;
      } catch (SolrCoreState.CoreIsClosedException e) {
        throw e;
      } catch (Exception e) {
        coreInitFailures.put(cd.getName(), new CoreLoadFailure(cd, e));
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Unable to reload core [" + cd.getName() + "]", e);
      } finally {
        if (!success && newCore != null && newCore.getOpenCount() > 0) {
          IOUtils.closeQuietly(newCore);
        }
        solrCores.removeFromPendingOps(cd.getName());
      }
    } else {
      if (coreId != null) return; // yeah, this core is already reloaded/unloaded return right away
      CoreLoadFailure clf = coreInitFailures.get(name);
      if (clf != null) {
        try {
          solrCores.waitAddPendingCoreOps(clf.cd.getName());
          createFromDescriptor(clf.cd, true, false);
        } finally {
          solrCores.removeFromPendingOps(clf.cd.getName());
        }
      } else {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "No such core: " + name);
      }
    }
  }

  /** Swaps two SolrCore descriptors. */
  public void swap(String n0, String n1) {
    apiAssumeStandalone();
    if (n0 == null || n1 == null) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Can not swap unnamed cores.");
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

  /**
   * Unload a core from this container, optionally removing the core's data and configuration
   *
   * @param name the name of the core to unload
   * @param deleteIndexDir if true, delete the core's index on close
   * @param deleteDataDir if true, delete the core's data directory on close
   * @param deleteInstanceDir if true, delete the core's instance directory on close
   */
  public void unload(
      String name, boolean deleteIndexDir, boolean deleteDataDir, boolean deleteInstanceDir) {

    CoreDescriptor cd = solrCores.getCoreDescriptor(name);

    if (name != null) {
      // check for core-init errors first
      CoreLoadFailure loadFailure = coreInitFailures.remove(name);
      if (loadFailure != null) {
        // getting the index directory requires opening a DirectoryFactory with a SolrConfig, etc.,
        // which we may not be able to do because of the init error.  So we just go with what we
        // can glean from the CoreDescriptor - datadir and instancedir
        SolrCore.deleteUnloadedCore(loadFailure.cd, deleteDataDir, deleteInstanceDir);
        // If last time around we didn't successfully load, make sure that all traces of the
        // coreDescriptor are gone.
        if (cd != null) {
          solrCores.removeCoreDescriptor(cd);
          coresLocator.delete(this, cd);
        }
        return;
      }
    }

    if (cd == null) {
      log.warn("Cannot unload non-existent core '{}'", name);
      throw new SolrException(
          ErrorCode.BAD_REQUEST, "Cannot unload non-existent core [" + name + "]");
    }

    boolean close = solrCores.isLoadedNotPendingClose(name);
    SolrCore core = solrCores.remove(name);

    solrCores.removeCoreDescriptor(cd);
    coresLocator.delete(this, cd);
    if (core == null) {
      // transient core
      SolrCore.deleteUnloadedCore(cd, deleteDataDir, deleteInstanceDir);
      return;
    }

    // delete metrics specific to this core
    metricManager.removeRegistry(core.getCoreMetricManager().getRegistryName());

    if (zkSys.getZkController() != null) {
      // cancel recovery in cloud mode
      core.getSolrCoreState().cancelRecovery();
      if (cd.getCloudDescriptor().getReplicaType() == Replica.Type.PULL
          || cd.getCloudDescriptor().getReplicaType() == Replica.Type.TLOG) {
        // Stop replication if this is part of a pull/tlog replica before closing the core
        zkSys.getZkController().stopReplicationFromLeader(name);
      }
    }

    core.unloadOnClose(cd, deleteIndexDir, deleteDataDir, deleteInstanceDir);
    if (close) core.closeAndWait();

    if (zkSys.getZkController() != null) {
      try {
        zkSys.getZkController().unregister(name, cd);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SolrException(
            ErrorCode.SERVER_ERROR,
            "Interrupted while unregistering core [" + name + "] from cloud state");
      } catch (KeeperException e) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Error unregistering core [" + name + "] from cloud state", e);
      } catch (Exception e) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Error unregistering core [" + name + "] from cloud state", e);
      }
    }
  }

  public void rename(String name, String toName) {
    apiAssumeStandalone();
    SolrIdentifierValidator.validateCoreName(toName);
    try (SolrCore core = getCore(name)) {
      if (core != null) {
        String oldRegistryName = core.getCoreMetricManager().getRegistryName();
        String newRegistryName = SolrCoreMetricManager.createRegistryName(core, toName);
        metricManager.swapRegistries(oldRegistryName, newRegistryName);
        // The old coreDescriptor is obsolete, so remove it. registerCore will put it back.
        CoreDescriptor cd = core.getCoreDescriptor();
        solrCores.removeCoreDescriptor(cd);
        cd.setProperty("name", toName);
        solrCores.addCoreDescriptor(cd);
        core.setName(toName);
        registerCore(cd, core, true, false);
        SolrCore old = solrCores.remove(name);

        coresLocator.rename(this, old.getCoreDescriptor(), core.getCoreDescriptor());
      }
    }
  }

  private void apiAssumeStandalone() {
    if (getZkController() != null) {
      throw new SolrException(ErrorCode.BAD_REQUEST, "Not supported in SolrCloud");
    }
  }

  /**
   * Get the CoreDescriptors for all cores managed by this container
   *
   * @return a List of CoreDescriptors
   */
  public List<CoreDescriptor> getCoreDescriptors() {
    return solrCores.getCoreDescriptors();
  }

  public CoreDescriptor getCoreDescriptor(String coreName) {
    return solrCores.getCoreDescriptor(coreName);
  }

  /** Where cores are created (absolute). */
  public Path getCoreRootDirectory() {
    return cfg.getCoreRootDirectory();
  }

  public SolrCore getCore(String name) {
    return getCore(name, null);
  }

  /**
   * Gets a core by name and increase its refcount.
   *
   * @param name the core name
   * @return the core if found, null if a SolrCore by this name does not exist
   * @throws SolrCoreInitializationException if a SolrCore with this name failed to be initialized
   * @see SolrCore#close()
   */
  public SolrCore getCore(String name, UUID id) {
    if (name == null) {
      return null;
    }

    // Do this in two phases since we don't want to lock access to the cores over a load.
    SolrCore core = solrCores.getCoreFromAnyList(name, true, id);

    // If a core is loaded, we're done just return it.
    if (core != null) {
      return core;
    }

    // If it's not yet loaded, we can check if it's had a core init failure and "do the right thing"
    CoreDescriptor desc = solrCores.getCoreDescriptor(name);

    // if there was an error initializing this core, throw a 500
    // error with the details for clients attempting to access it.
    CoreLoadFailure loadFailure = getCoreInitFailures().get(name);
    if (null != loadFailure) {
      throw new SolrCoreInitializationException(name, loadFailure.exception);
    }
    // This is a bit of awkwardness where SolrCloud and transient cores don't play nice together.
    // For transient cores, we have to allow them to be created at any time there hasn't been a core
    // load failure (use reload to cure that). But for
    // TestConfigSetsAPI.testUploadWithScriptUpdateProcessor, this needs to _not_ try to load the
    // core if the core is null and there was an error. If you change this, be sure to run both
    // TestConfigSetsAPI and TestLazyCores
    if (desc == null || zkSys.getZkController() != null) return null;

    // This will put an entry in pending core ops if the core isn't loaded. Here's where moving the
    // waitAddPendingCoreOps to createFromDescriptor would introduce a race condition.
    core = solrCores.waitAddPendingCoreOps(name);

    if (isShutDown) {
      // We're quitting, so stop. This needs to be after the wait above since we may come off the
      // wait as a consequence of shutting down.
      return null;
    }
    try {
      if (core == null) {
        if (zkSys.getZkController() != null) {
          zkSys.getZkController().throwErrorIfReplicaReplaced(desc);
        }
        core = createFromDescriptor(desc, true, false); // This should throw an error if it fails.
      }
      core.open();
    } finally {
      solrCores.removeFromPendingOps(name);
    }

    return core;
  }

  public BlobRepository getBlobRepository() {
    return blobRepository;
  }

  /**
   * If using asyncSolrCoreLoad=true, calling this after {@link #load()} will not return until all
   * cores have finished loading.
   *
   * @param timeoutMs timeout, upon which method simply returns
   */
  public void waitForLoadingCoresToFinish(long timeoutMs) {
    solrCores.waitForLoadingCoresToFinish(timeoutMs);
  }

  public void waitForLoadingCore(String name, long timeoutMs) {
    solrCores.waitForLoadingCoreToFinish(name, timeoutMs);
  }

  // ---------------- CoreContainer request handlers --------------

  protected <T> T createHandler(String path, String handlerClass, Class<T> clazz) {
    T handler =
        loader.newInstance(
            handlerClass, clazz, null, new Class<?>[] {CoreContainer.class}, new Object[] {this});
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
    return this.coreConfigService;
  }

  public void setCoreConfigService(ConfigSetService configSetService) {
    this.coreConfigService = configSetService;
  }

  public String getHostName() {
    return this.hostName;
  }

  /**
   * Gets the alternate path for multicore handling: This is used in case there is a registered
   * unnamed core (aka name is "") to declare an alternate way of accessing named cores. This can
   * also be used in a pseudo single-core environment so admins can prepare a new version before
   * swapping.
   */
  public String getManagementPath() {
    return cfg.getManagementPath();
  }

  public LogWatcher<?> getLogging() {
    return logging;
  }

  /** Determines whether the core is already loaded or not but does NOT load the core */
  public boolean isLoaded(String name) {
    return solrCores.isLoaded(name);
  }

  /** The primary path of a Solr server's config, cores, and misc things. Absolute. */
  // TODO return Path
  public String getSolrHome() {
    return solrHome.toString();
  }

  /**
   * A path where Solr users can retrieve arbitrary files from. Absolute.
   *
   * <p>Files located in this directory can be manipulated using select Solr features (e.g.
   * streaming expressions).
   */
  public Path getUserFilesPath() {
    return solrHome.resolve("userfiles");
  }

  public boolean isZooKeeperAware() {
    return zkSys.getZkController() != null;
  }

  public ZkController getZkController() {
    return zkSys.getZkController();
  }

  public NodeConfig getConfig() {
    return cfg;
  }

  /** The default ShardHandlerFactory used to communicate with other solr instances */
  public ShardHandlerFactory getShardHandlerFactory() {
    return shardHandlerFactory;
  }

  public UpdateShardHandler getUpdateShardHandler() {
    return updateShardHandler;
  }

  public SolrResourceLoader getResourceLoader() {
    return loader;
  }

  public boolean isCoreLoading(String name) {
    return solrCores.isCoreLoading(name);
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

  public boolean hideStackTrace() {
    return cfg.hideStackTraces();
  }

  /**
   * Retrieve the aliases from zookeeper. This is typically cached and does not hit zookeeper after
   * the first use.
   *
   * @return an immutable instance of {@code Aliases} accurate as of at the time this method is
   *     invoked, less any zookeeper update lag.
   * @throws RuntimeException if invoked on a {@code CoreContainer} where {@link
   *     #isZooKeeperAware()} returns false
   */
  public Aliases getAliases() {
    if (isZooKeeperAware()) {
      return getZkController().getZkStateReader().getAliases();
    } else {
      // fail fast because it's programmer error, but give slightly more info than NPE.
      throw new IllegalStateException(
          "Aliases don't exist in a non-cloud context, check isZookeeperAware() before calling this method.");
    }
  }

  /**
   * @param solrCore the core against which we check if there has been a tragic exception
   * @return whether this Solr core has tragic exception
   * @see org.apache.lucene.index.IndexWriter#getTragicException()
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
      getZkController().giveupLeadership(solrCore.getCoreDescriptor());

      try {
        // If the error was something like a full file system disconnect, this probably won't help
        // But if it is a transient disk failure then it's worth a try
        solrCore.getSolrCoreState().newIndexWriter(solrCore, false); // should we rollback?
      } catch (IOException e) {
        log.warn("Could not roll index writer after tragedy");
      }
    }

    return tragicException != null;
  }

  public ContainerPluginsRegistry getContainerPluginsRegistry() {
    return containerPluginsRegistry;
  }

  public ClusterSingletons getClusterSingletons() {
    return clusterSingletons;
  }

  public ClusterEventProducer getClusterEventProducer() {
    return clusterEventProducer;
  }

  public PlacementPluginFactory<? extends PlacementPluginConfig> getPlacementPluginFactory() {
    return placementPluginFactory;
  }

  public Optional<DistributedCollectionConfigSetCommandRunner>
      getDistributedCollectionCommandRunner() {
    return this.distributedCollectionCommandRunner;
  }

  /**
   * Run an arbitrary task in its own thread. This is an expert option and is a method you should
   * use with great care. It would be bad to run something that never stopped or run something that
   * took a very long time. Typically this is intended for actions that take a few seconds, and
   * therefore would be bad to wait for within a request, or actions that need to happen when a core
   * has zero references, but would not pose a significant hindrance to server shut down times. It
   * is not intended for long-running tasks and if you are using a Runnable with a loop in it, you
   * are almost certainly doing it wrong.
   *
   * <p><br>
   * WARNING: Solr wil not be able to shut down gracefully until this task completes!
   *
   * <p><br>
   * A significant upside of using this method vs creating your own ExecutorService is that your
   * code does not have to properly shutdown executors which typically is risky from a unit testing
   * perspective since the test framework will complain if you don't carefully ensure the executor
   * shuts down before the end of the test. Also the threads running this task are sure to have a
   * proper MDC for logging.
   *
   * <p><br>
   * Normally, one uses {@link SolrCore#runAsync(Runnable)} if possible, but in some cases you might
   * need to execute a task asynchronously when you could be running on a node with no cores, and
   * then use of this method is indicated.
   *
   * @param r the task to run
   */
  public void runAsync(Runnable r) {
    coreContainerAsyncTaskExecutor.submit(r);
  }

  public static void setWeakStringInterner() {
    boolean enable = "true".equals(System.getProperty("solr.use.str.intern", "true"));
    if (!enable) return;
    Interner<String> interner = Interner.newWeakInterner();
    ClusterState.setStrInternerParser(
        new Function<>() {
          @Override
          public ObjectBuilder apply(JSONParser p) {
            try {
              return new ObjectBuilder(p) {
                @Override
                public void addKeyVal(Object map, Object key, Object val) throws IOException {
                  if (key != null) {
                    key = interner.intern(key.toString());
                  }
                  if (val instanceof String) {
                    val = interner.intern((String) val);
                  }
                  super.addKeyVal(map, key, val);
                }
              };
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        });
  }
}

class CloserThread extends Thread {
  CoreContainer container;
  SolrCores solrCores;
  NodeConfig cfg;

  CloserThread(CoreContainer container, SolrCores solrCores, NodeConfig cfg) {
    super("CloserThread");
    this.container = container;
    this.solrCores = solrCores;
    this.cfg = cfg;
  }

  // It's important that this be the _only_ thread removing things from pendingDynamicCloses!
  // This is single-threaded, but I tried a multithreaded approach and didn't see any performance
  // gains, so there's no good justification for the complexity. I suspect that the locking on
  // things like DefaultSolrCoreState essentially create a single-threaded process anyway.
  @Override
  public void run() {
    while (!container.isShutDown()) {
      synchronized (solrCores.getModifyLock()) { // need this so we can wait and be awoken.
        try {
          solrCores.getModifyLock().wait();
        } catch (InterruptedException e) {
          // Well, if we've been told to stop, we will. Otherwise, continue on and check to see if
          // there are any cores to close.
        }
      }

      SolrCore core;
      while (!container.isShutDown() && (core = solrCores.getCoreToClose()) != null) {
        assert core.getOpenCount() == 1;
        try {
          MDCLoggingContext.setCore(core);
          core.close(); // will clear MDC
        } finally {
          solrCores.removeFromPendingOps(core.getName());
        }
      }
    }
  }
}
