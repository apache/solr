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
package org.apache.solr.servlet;

import static org.apache.solr.core.NodeConfig.loadNodeConfig;
import static org.apache.solr.servlet.SolrDispatchFilter.PROPERTIES_ATTRIBUTE;
import static org.apache.solr.servlet.SolrDispatchFilter.SOLRHOME_ATTRIBUTE;
import static org.apache.solr.servlet.SolrDispatchFilter.SOLR_INSTALL_DIR_ATTRIBUTE;
import static org.apache.solr.servlet.SolrDispatchFilter.SOLR_LOG_LEVEL;
import static org.apache.solr.servlet.SolrDispatchFilter.SOLR_LOG_MUTECONSOLE;

import com.codahale.metrics.jvm.CachedThreadStatesGaugeSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.google.common.annotations.VisibleForTesting;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.NoInitialContextException;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.UnavailableException;
import org.apache.http.client.HttpClient;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.VectorUtil;
import org.apache.solr.client.api.util.SolrVersion;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.MetricsConfig;
import org.apache.solr.core.NodeConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrInfoBean.Group;
import org.apache.solr.core.SolrXmlConfig;
import org.apache.solr.metrics.AltBufferPoolMetricSet;
import org.apache.solr.metrics.MetricsMap;
import org.apache.solr.metrics.OperatingSystemMetricSet;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.metrics.SolrMetricManager.ResolutionStrategy;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.servlet.RateLimitManager.Builder;
import org.apache.solr.util.StartupLoggingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that can provide access to solr cores. This allows us to have multiple filters and
 * servlets that depend on SolrCore and CoreContainer, while still only having one CoreContainer per
 * instance of solr.
 */
public class CoreContainerProvider implements ServletContextListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);
  private CoreContainer cores;
  private Properties extraProperties;
  private HttpClient httpClient;
  private SolrMetricManager metricManager;
  private RateLimitManager rateLimitManager;
  private final CountDownLatch init = new CountDownLatch(1);
  private String registryName;
  // AFAIK the only reason we need this is to support JettySolrRunner for tests. In tests we might
  // have multiple CoreContainers in the same JVM, but I *think* that doesn't happen in a real
  // server.
  private static final Map<ContextInitializationKey, ServiceHolder> services =
      Collections.synchronizedMap(new WeakHashMap<>());

  // todo: dependency injection instead, but for now this method and the associated map will have
  // to suffice.
  // Note that this relies on ServletContext.equals() not implementing anything significantly
  // different than Object.equals for its .equals method (I've found no implementation that even
  // implements it).
  public static ServiceHolder serviceForContext(ServletContext ctx) throws InterruptedException {
    ContextInitializationKey key = new ContextInitializationKey(ctx);
    return services.computeIfAbsent(key, ServiceHolder::new);
  }

  @Override
  public void contextInitialized(ServletContextEvent sce) {
    init(sce.getServletContext());
  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    close();
  }

  CoreContainer getCoreContainer() throws UnavailableException {
    waitForCoreContainer(() -> cores, init);
    return cores;
  }

  HttpClient getHttpClient() throws UnavailableException {
    waitForCoreContainer(() -> cores, init);
    return httpClient;
  }

  private static void waitForCoreContainer(Supplier<CoreContainer> provider, CountDownLatch latch)
      throws UnavailableException {
    CoreContainer cores = provider.get();
    if (cores == null || cores.isShutDown()) {
      long startWait = System.nanoTime();
      try {
        while (!latch.await(10, TimeUnit.SECONDS)) {
          long now = System.nanoTime();
          if (log.isInfoEnabled()) {
            log.info(
                "Still waiting for CoreContainerStartup ({} seconds elapsed)",
                (now - startWait) / 1_000_000_000);
          }
        }
      } catch (InterruptedException e) { // well, no wait then
        Thread.currentThread().interrupt();
      }
      cores = provider.get();
      if (cores == null || cores.isShutDown()) {
        final String msg =
            "Error processing the request. CoreContainer is either not initialized or shutting down.";
        log.error(msg);
        throw new UnavailableException(msg);
      }
    }
  }

  public void close() {
    CoreContainer cc = cores;

    // Mark Miller suggested that we should be publishing that we are down before anything else
    // which makes good sense, but the following causes test failures, so that improvement can be
    // the subject of another PR/issue. Also, jetty might already be refusing requests by this point
    // so that's a potential issue too. Digging slightly I see that there's a whole mess of code
    // looking up collections and calculating state changes associated with this call, which smells
    // a lot like we're duplicating node state in collection stuff, but it will take a lot of code
    // reading to figure out if that's really what it is, why we did it and if there's room for
    // improvement.
    //    if (cc != null) {
    //      ZkController zkController = cc.getZkController();
    //      if (zkController != null) {
    //        zkController.publishNodeAsDown(zkController.getNodeName());
    //      }
    //    }

    cores = null;
    try {
      if (metricManager != null) {
        try {
          metricManager.unregisterGauges(registryName, metricTag);
        } catch (NullPointerException e) {
          // okay
        } catch (Exception e) {
          log.warn("Exception closing FileCleaningTracker", e);
        } finally {
          metricManager = null;
        }
      }
    } finally {
      if (cc != null) {
        httpClient = null;
        cc.shutdown();
      }
    }
  }

  public void init(ServletContext servletContext) {
    if (log.isTraceEnabled()) {
      log.trace("CoreService.init(): {}", this.getClass().getClassLoader());
    }
    CoreContainer coresInit = null;
    try {
      // "extra" properties must be initialized first, so we know things like "do we have a zkHost"
      // wrap as defaults (if set) so we can modify w/o polluting the Properties provided by our
      // caller
      this.extraProperties =
          SolrXmlConfig.wrapAndSetZkHostFromSysPropIfNeeded(
              (Properties) servletContext.getAttribute(PROPERTIES_ATTRIBUTE));

      StartupLoggingUtils.checkLogDir();
      if (log.isInfoEnabled()) {
        log.info("Using logger factory {}", StartupLoggingUtils.getLoggerImplStr());
      }

      logWelcomeBanner();

      String muteConsole = System.getProperty(SOLR_LOG_MUTECONSOLE);
      if (muteConsole != null
          && !Arrays.asList("false", "0", "off", "no")
              .contains(muteConsole.toLowerCase(Locale.ROOT))) {
        StartupLoggingUtils.muteConsole();
      }
      String logLevel = System.getProperty(SOLR_LOG_LEVEL);
      if (logLevel != null) {
        log.info("Log level override, property solr.log.level={}", logLevel);
        StartupLoggingUtils.changeLogLevel(logLevel);
      }

      // Do initial logs for experimental Lucene classes.
      // TODO: Use "MethodHandles.lookup().ensureClassInitialized()" instead of "Class.forName()"
      //   once JDK 15+ is mandatory
      Stream.of(MMapDirectory.class, VectorUtil.class)
          .forEach(
              cls -> {
                try {
                  Class.forName(cls.getName());
                } catch (ReflectiveOperationException re) {
                  throw new SolrException(
                      ErrorCode.SERVER_ERROR, "Could not load Lucene class: " + cls.getName());
                }
              });

      coresInit = createCoreContainer(computeSolrHome(servletContext), extraProperties);
      this.httpClient = coresInit.getUpdateShardHandler().getDefaultHttpClient();
      setupJvmMetrics(coresInit, coresInit.getNodeConfig().getMetricsConfig());

      SolrZkClient zkClient = null;
      ZkController zkController = coresInit.getZkController();

      if (zkController != null) {
        zkClient = zkController.getZkClient();
      }

      Builder builder = new Builder(zkClient);

      this.rateLimitManager = builder.build();

      if (zkController != null) {
        zkController.zkStateReader.registerClusterPropertiesListener(this.rateLimitManager);
      }

      if (log.isDebugEnabled()) {
        log.debug("user.dir={}", System.getProperty("user.dir"));
      }
    } catch (Throwable t) {
      // catch this so our filter still works
      log.error("Could not start Solr. Check solr/home property and the logs", t);
      if (t instanceof Error) {
        throw (Error) t;
      }
    } finally {
      log.trace("SolrDispatchFilter.init() done");
      this.cores = coresInit; // crucially final assignment
      services
          .computeIfAbsent(new ContextInitializationKey(servletContext), ServiceHolder::new)
          .setService(this);
      init.countDown();
    }
  }

  private void logWelcomeBanner() {
    // _Really_ sorry about how clumsy this is as a result of the logging call checker, but this is
    // the only one that's so ugly so far.
    if (log.isInfoEnabled()) {
      log.info(" ___      _       Welcome to Apache Solr™ version {}", solrVersion());
    }
    if (log.isInfoEnabled()) {
      log.info(
          "/ __| ___| |_ _   Starting in {} mode on port {}",
          isCloudMode() ? "cloud" : "standalone",
          getSolrPort());
    }
    if (log.isInfoEnabled()) {
      log.info(
          "\\__ \\/ _ \\ | '_|  Install dir: {}", System.getProperty(SOLR_INSTALL_DIR_ATTRIBUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("|___/\\___/_|_|    Start time: {}", Instant.now());
    }
    try {
      RuntimeMXBean mx = ManagementFactory.getRuntimeMXBean();
      Optional<String> crashOnOutOfMemoryErrorArg =
          mx.getInputArguments().stream()
              .filter(x -> x.startsWith("-XX:+CrashOnOutOfMemoryError"))
              .findFirst();
      if (crashOnOutOfMemoryErrorArg.isPresent()) {
        String errorFileArg =
            mx.getInputArguments().stream()
                .filter(x -> x.startsWith("-XX:ErrorFile"))
                .findFirst()
                .orElse("-XX:ErrorFile=hs_err_%p.log");
        String errorFilePath =
            errorFileArg
                .substring(errorFileArg.indexOf('=') + 1)
                .replace("%p", String.valueOf(mx.getPid()));
        String logMessage =
            "Solr started with \"-XX:+CrashOnOutOfMemoryError\" that will crash on any OutOfMemoryError exception. "
                + "The cause of the OOME will be logged in the crash file at the following path: {}";
        log.info(logMessage, errorFilePath);
      }
    } catch (Exception e) {
      String logMessage =
          String.format(
              Locale.ROOT,
              "Solr typically starts with \"-XX:+CrashOnOutOfMemoryError\" that will crash on any OutOfMemoryError exception. "
                  + "Unable to get the specific file due to an exception."
                  + "The cause of the OOME will be logged in a crash file in the logs directory: %s",
              System.getProperty("solr.log.dir"));
      log.info(logMessage, e);
    }
  }

  private String solrVersion() {
    String specVer = SolrVersion.LATEST.toString();
    try {
      String implVer = SolrCore.class.getPackage().getImplementationVersion();
      return (specVer.equals(implVer.split(" ")[0])) ? specVer : implVer;
    } catch (Exception e) {
      return specVer;
    }
  }

  private String getSolrPort() {
    return System.getProperty("jetty.port");
  }

  /**
   * We are in cloud mode if Java option zkRun exists OR zkHost exists and is non-empty
   *
   * @see SolrXmlConfig#wrapAndSetZkHostFromSysPropIfNeeded
   * @see #extraProperties
   * @see #init
   */
  private boolean isCloudMode() {
    assert null != extraProperties; // we should never be called w/o this being initialized
    return (null != extraProperties.getProperty(SolrXmlConfig.ZK_HOST))
        || (null != System.getProperty("zkRun"));
  }

  /**
   * Returns the effective Solr Home to use for this node, based on looking up the value in this
   * order:
   *
   * <ol>
   *   <li>attribute in the FilterConfig
   *   <li>JNDI: via java:comp/env/solr/home
   *   <li>The system property solr.solr.home
   *   <li>Look in the current working directory for a solr/ directory
   * </ol>
   *
   * <p>
   *
   * @return the Solr home, absolute and normalized.
   */
  @SuppressWarnings("BanJNDI")
  private static Path computeSolrHome(ServletContext servletContext) {

    // start with explicit check of servlet config...
    String source = "servlet config: " + SOLRHOME_ATTRIBUTE;
    String home = (String) servletContext.getAttribute(SOLRHOME_ATTRIBUTE);

    if (null == home) {
      final String lookup = "java:comp/env/solr/home";
      // Try JNDI
      source = "JNDI: " + lookup;
      try {
        Context c = new InitialContext();
        home = (String) c.lookup(lookup);
      } catch (NoInitialContextException e) {
        log.debug("JNDI not configured for solr (NoInitialContextEx)");
      } catch (NamingException e) {
        log.debug("No /solr/home in JNDI");
      } catch (RuntimeException ex) {
        log.warn("Odd RuntimeException while testing for JNDI: ", ex);
      }
    }

    if (null == home) {
      // Now try system property
      final String prop = "solr.solr.home";
      source = "system property: " + prop;
      home = System.getProperty(prop);
    }

    if (null == home) {
      // if all else fails, assume default dir
      home = "solr/";
      source = "defaulted to '" + home + "' ... could not find system property or JNDI";
    }
    final Path solrHome = Paths.get(home).toAbsolutePath().normalize();
    log.info("Solr Home: {} (source: {})", solrHome, source);

    return solrHome;
  }

  /**
   * CoreContainer initialization
   *
   * @return a CoreContainer to hold this server's cores
   */
  protected CoreContainer createCoreContainer(Path solrHome, Properties nodeProps) {
    NodeConfig nodeConfig = loadNodeConfig(solrHome, nodeProps);
    final CoreContainer coreContainer = new CoreContainer(nodeConfig, true);
    coreContainer.load();
    return coreContainer;
  }

  private void setupJvmMetrics(CoreContainer coresInit, MetricsConfig config) {
    metricManager = coresInit.getMetricManager();
    registryName = SolrMetricManager.getRegistryName(Group.jvm);
    final NodeConfig nodeConfig = coresInit.getConfig();
    try {
      metricManager.registerAll(
          registryName, new AltBufferPoolMetricSet(), ResolutionStrategy.IGNORE, "buffers");
      metricManager.registerAll(
          registryName, new ClassLoadingGaugeSet(), ResolutionStrategy.IGNORE, "classes");
      metricManager.registerAll(
          registryName, new OperatingSystemMetricSet(), ResolutionStrategy.IGNORE, "os");
      metricManager.registerAll(
          registryName, new GarbageCollectorMetricSet(), ResolutionStrategy.IGNORE, "gc");
      metricManager.registerAll(
          registryName, new MemoryUsageGaugeSet(), ResolutionStrategy.IGNORE, "memory");

      if (config.getCacheConfig() != null
          && config.getCacheConfig().threadsIntervalSeconds != null) {
        if (log.isInfoEnabled()) {
          log.info(
              "Threads metrics will be cached for {} seconds",
              config.getCacheConfig().threadsIntervalSeconds);
        }
        metricManager.registerAll(
            registryName,
            new CachedThreadStatesGaugeSet(
                config.getCacheConfig().threadsIntervalSeconds, TimeUnit.SECONDS),
            SolrMetricManager.ResolutionStrategy.IGNORE,
            "threads");
      } else {
        metricManager.registerAll(
            registryName,
            new ThreadStatesGaugeSet(),
            SolrMetricManager.ResolutionStrategy.IGNORE,
            "threads");
      }

      MetricsMap sysprops =
          new MetricsMap(
              map ->
                  System.getProperties()
                      .forEach(
                          (k, v) -> {
                            if (!nodeConfig.isSysPropHidden(String.valueOf(k))) {
                              map.putNoEx(String.valueOf(k), v);
                            }
                          }));
      metricManager.registerGauge(
          null,
          registryName,
          sysprops,
          metricTag,
          ResolutionStrategy.IGNORE,
          "properties",
          "system");
    } catch (Exception e) {
      log.warn("Error registering JVM metrics", e);
    }
  }

  public RateLimitManager getRateLimitManager() {
    return rateLimitManager;
  }

  @VisibleForTesting
  void setRateLimitManager(RateLimitManager rateLimitManager) {
    this.rateLimitManager = rateLimitManager;
  }

  private static class ContextInitializationKey {
    private final ServletContext ctx;
    private final CountDownLatch initializing = new CountDownLatch(1);

    private ContextInitializationKey(ServletContext ctx) {
      if (ctx == null) {
        throw new IllegalArgumentException("Context must not be null");
      }
      // if one of these is reachable both must be to avoid collection from weak hashmap, so
      // set an attribute holding this object to ensure we never get collected until the
      // ServletContext is eligible for collection too.
      ctx.setAttribute(this.getClass().getName(), this);
      this.ctx = ctx;
    }

    public synchronized ServletContext getCtx() {
      return ctx;
    }

    synchronized void makeReady() {
      this.initializing.countDown();
    }

    // NOT synchronized :)
    public void waitForReadyService() throws InterruptedException {
      initializing.await();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ContextInitializationKey)) return false;
      ContextInitializationKey that = (ContextInitializationKey) o;
      return ctx.equals(that.ctx);
    }

    @Override
    public int hashCode() {
      return Objects.hash(ctx);
    }
  }

  static class ServiceHolder {
    private volatile CoreContainerProvider service;
    private volatile ContextInitializationKey key;

    private ServiceHolder(ContextInitializationKey key) {
      if (key == null) {
        throw new IllegalArgumentException(
            "Key for accessing this service holder must be supplied");
      }
      this.key = key;
    }

    public void setService(CoreContainerProvider service) {
      this.service = service;
      key.makeReady();
      key = null; // be sure not to hold a reference to the context via the key
    }

    public CoreContainerProvider getService() {
      try {
        if (key != null) {
          try {
            key.waitForReadyService();
          } catch (NullPointerException e) {
            // ignore, means we raced with set service and lost, but that's fine since null implies
            // we are ready.
          }
        }
      } catch (InterruptedException e) {
        throw new SolrException(
            ErrorCode.SERVER_ERROR, "Interrupted while obtaining reference to CoreService");
      }
      return service;
    }
  }
}
