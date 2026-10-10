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
package org.apache.solr.embedded;

import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import jakarta.servlet.DispatcherType;
import jakarta.servlet.Filter;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletException;
import jakarta.servlet.UnavailableException;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrBackend;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.jetty.SSLConfig;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.SolrMetricManager;
import org.apache.solr.servlet.AuthenticationFilter;
import org.apache.solr.servlet.CoreContainerProvider;
import org.apache.solr.servlet.LoadAdminUiServlet;
import org.apache.solr.servlet.RateLimitFilter;
import org.apache.solr.servlet.RequiredSolrRequestFilter;
import org.apache.solr.servlet.SolrServlet;
import org.apache.solr.servlet.TracingFilter;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.RestTestHarness;
import org.apache.solr.util.SocketProxy;
import org.apache.solr.util.TimeOut;
import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.FilterMapping;
import org.eclipse.jetty.ee10.servlet.ResourceServlet;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.ee10.servlet.Source;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.GracefulHandler;
import org.eclipse.jetty.session.DefaultSessionIdManager;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ReservedThreadExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Run solr using jetty
 *
 * @since solr 1.3
 */
public class JettySolrRunner implements SolrBackend {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final int THREAD_POOL_MAX_THREADS = 10000;
  // NOTE: needs to be larger than SolrHttpClient.threadPoolSweeperMaxIdleTime
  private static final int THREAD_POOL_MAX_IDLE_TIME_MS = 260000;

  // Ports held on behalf of stopped runners, so that no other process can take a stopped
  // runner's port before a runner starts on it again. Static because a replacement runner
  // can be created for the same port (SSLMigrationTest does this); the reservation has to
  // be visible across instances. An entry is released in exactly three ways: a runner
  // starts on the port; MiniSolrCloudCluster.shutdown() releases the runners still in the
  // cluster at shutdown; or the runner holding the entry is closed. In every other case
  // the entry is held until the JVM exits. That includes a runner stopped and removed
  // from its cluster and never closed, and a runner restarted on a different port than
  // the one it reserved: a release only ever names the port a runner starts on or the
  // port it currently holds, never a port it held earlier.
  private static final Map<Integer, ServerSocket> RESERVED_PORTS = new ConcurrentHashMap<>();

  private Server server;

  private volatile ServletHolder solrServlet;

  private int jettyPort = -1;

  private final JettyConfig config;
  private final String solrHome;
  private final Properties nodeProperties;

  private volatile boolean startedBefore = false;

  private int proxyPort = -1;

  private final boolean enableProxy;

  private SocketProxy proxy;

  private String protocol;

  private String host;

  private volatile HttpJettySolrClient jettySolrClient;

  private volatile boolean started = false;

  /**
   * Create a new JettySolrRunner.
   *
   * <p>After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the solr home directory to use
   * @param port the port to run on
   */
  public JettySolrRunner(String solrHome, int port) {
    this(solrHome, JettyConfig.builder().setPort(port).build());
  }

  /**
   * Construct a JettySolrRunner
   *
   * <p>After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the base path to run from
   * @param config the configuration
   */
  public JettySolrRunner(String solrHome, JettyConfig config) {
    this(solrHome, new Properties(), config);
  }

  /**
   * Construct a JettySolrRunner
   *
   * <p>After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the solrHome to use
   * @param nodeProperties the container properties
   * @param config the configuration
   */
  public JettySolrRunner(String solrHome, Properties nodeProperties, JettyConfig config) {
    this(solrHome, nodeProperties, config, false);
  }

  /**
   * Construct a JettySolrRunner
   *
   * <p>After construction, you must start the jetty with {@link #start()}
   *
   * @param solrHome the solrHome to use
   * @param nodeProperties the container properties
   * @param config the configuration
   * @param enableProxy enables proxy feature to disable connections
   */
  public JettySolrRunner(
      String solrHome, Properties nodeProperties, JettyConfig config, boolean enableProxy) {
    this.enableProxy = enableProxy;
    this.solrHome = solrHome;
    this.config = config;
    this.nodeProperties = nodeProperties;

    if (enableProxy) {
      try {
        proxy = new SocketProxy(0, config.sslConfig != null && config.sslConfig.isSSLMode());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      setProxyPort(proxy.getListenPort());
    }

    this.init(this.config.port);
  }

  private void init(int port) {

    QueuedThreadPool qtp = new QueuedThreadPool();
    qtp.setMaxThreads(THREAD_POOL_MAX_THREADS);
    qtp.setIdleTimeout(THREAD_POOL_MAX_IDLE_TIME_MS);
    qtp.setReservedThreads(0);
    server = new Server(qtp);
    server.manage(qtp);
    server.setStopAtShutdown(config.stopAtShutdown);

    if (System.getProperty("jetty.testMode") != null) {
      // if this property is true, then jetty will be configured to use SSL
      // leveraging the same system properties as java to specify
      // the keystore/truststore if they are set unless specific config
      // is passed via the constructor.
      //
      // This means we will use the same truststore, keystore (and keys) for
      // the server as well as any client actions taken by this JVM in
      // talking to that server, but for the purposes of testing that should
      // be good enough
      final SslContextFactory.Server sslcontext = SSLConfig.createContextFactory(config.sslConfig);

      HttpConfiguration configuration = new HttpConfiguration();
      ServerConnector connector;
      if (sslcontext != null) {
        configuration.setSecureScheme("https");
        SecureRequestCustomizer customizer = new SecureRequestCustomizer(false);
        sslcontext.setSniRequired(false);
        customizer.setSniHostCheck(false);

        configuration.addCustomizer(customizer);
        HttpConnectionFactory http1ConnectionFactory = new HttpConnectionFactory(configuration);

        if (config.onlyHttp1) {
          connector =
              new ServerConnector(
                  server,
                  new SslConnectionFactory(sslcontext, http1ConnectionFactory.getProtocol()),
                  http1ConnectionFactory);
        } else {
          sslcontext.setCipherComparator(HTTP2Cipher.COMPARATOR);

          connector = new ServerConnector(server);
          SslConnectionFactory sslConnectionFactory = new SslConnectionFactory(sslcontext, "alpn");
          connector.addConnectionFactory(sslConnectionFactory);
          connector.setDefaultProtocol(sslConnectionFactory.getProtocol());

          HTTP2ServerConnectionFactory http2ConnectionFactory =
              new HTTP2ServerConnectionFactory(configuration);

          ALPNServerConnectionFactory alpn =
              new ALPNServerConnectionFactory(
                  http2ConnectionFactory.getProtocol(), http1ConnectionFactory.getProtocol());
          alpn.setDefaultProtocol(http1ConnectionFactory.getProtocol());
          connector.addConnectionFactory(alpn);
          connector.addConnectionFactory(http1ConnectionFactory);
          connector.addConnectionFactory(http2ConnectionFactory);
        }
      } else {
        if (config.onlyHttp1) {
          connector = new ServerConnector(server, new HttpConnectionFactory(configuration));
        } else {
          connector =
              new ServerConnector(
                  server,
                  new HttpConnectionFactory(configuration),
                  new HTTP2CServerConnectionFactory(configuration));
        }
      }

      connector.setReuseAddress(true);
      connector.setPort(port);
      connector.setHost("127.0.0.1");
      connector.setIdleTimeout(THREAD_POOL_MAX_IDLE_TIME_MS);

      server.setConnectors(new Connector[] {connector});
      server.addBean(new DefaultSessionIdManager(server, new Random()), true);
    } else {
      HttpConfiguration configuration = new HttpConfiguration();
      ServerConnector connector =
          new ServerConnector(
              server,
              new HttpConnectionFactory(configuration),
              new HTTP2CServerConnectionFactory(configuration));
      connector.setReuseAddress(true);
      connector.setPort(port);
      connector.setHost("127.0.0.1");
      connector.setIdleTimeout(THREAD_POOL_MAX_IDLE_TIME_MS);
      server.setConnectors(new Connector[] {connector});
    }

    Handler.Wrapper chain;
    {
      // Initialize the servlets
      final ServletContextHandler root =
          new ServletContextHandler("/solr", ServletContextHandler.NO_SESSIONS);
      root.setServer(server);
      if (config.enableAdminUi) {
        Path webappDir = ExternalPaths.WEBAPP_HOME;
        if (webappDir == null || !Files.exists(webappDir.resolve("index.html"))) {
          throw new IllegalStateException(
              "enableAdminUi requires the Admin UI webapp sources at <source>/solr/webapp/web, "
                  + "but they could not be located (ExternalPaths.WEBAPP_HOME="
                  + webappDir
                  + ")");
        }
        root.setBaseResource(ResourceFactory.of(server).newResource(webappDir));
      } else {
        root.setBaseResource(ResourceFactory.of(server).newResource("."));
      }
      root.addEventListener(
          // Install CCP first.  Subclass CCP to do some pre-initialization
          new CoreContainerProvider() {
            @Override
            public void contextInitialized(ServletContextEvent event) {
              // awkwardly, parts of Solr want to know the port, but we don't know that until now
              jettyPort = getFirstConnectorPort();
              int port = jettyPort;
              if (proxyPort != -1) port = proxyPort;
              nodeProperties.setProperty("hostPort", Integer.toString(port));

              root.getServletContext()
                  .setAttribute(CoreContainerProvider.SOLR_PROPERTIES, nodeProperties);
              root.getServletContext().setAttribute(CoreContainerProvider.SOLR_SOLR_HOME, solrHome);

              SSLConfigurationsFactory.current().init(); // normally happens in jetty-ssl.xml

              log.info("Jetty properties: {}", nodeProperties);

              super.contextInitialized(event);
            }
          });

      for (Map.Entry<Class<? extends Filter>, String> entry : config.extraFilters.entrySet()) {
        root.addFilter(entry.getKey(), entry.getValue(), EnumSet.of(DispatcherType.REQUEST));
      }

      for (Map.Entry<ServletHolder, String> entry : config.extraServlets.entrySet()) {
        root.addServlet(entry.getKey(), entry.getValue());
      }
      // TODO: This needs to be driven by a parsing of web.xml eventually
      //  though we still want to avoid classpath scanning.

      if (config.enableAdminUi) {
        // Serve the Admin UI like production web.xml does: static assets + LoadAdminUiServlet
        ServletHolder staticHolder = root.getServletHandler().newServletHolder(Source.EMBEDDED);
        staticHolder.setName("static");
        staticHolder.setHeldClass(ResourceServlet.class);
        staticHolder.setInitParameter("pathInfoOnly", "false");
        staticHolder.setInitParameter("dirAllowed", "false");
        for (String pathSpec :
            new String[] {
              "/partials/*", "/libs/*", "/css/*", "/js/*", "/img/*", "/templates/*", "/ui/*"
            }) {
          root.addServlet(staticHolder, pathSpec);
        }
        ServletHolder adminUiHolder = root.getServletHandler().newServletHolder(Source.EMBEDDED);
        adminUiHolder.setName("LoadAdminUI");
        adminUiHolder.setHeldClass(LoadAdminUiServlet.class);
        root.addServlet(adminUiHolder, "/index.html");
      }

      // This is our main workhorse - now a servlet instead of filter
      solrServlet = root.getServletHandler().newServletHolder(Source.EMBEDDED);
      solrServlet.setName("SolrServlet");
      solrServlet.setHeldClass(SolrServlet.class);
      root.addServlet(solrServlet, "/*");

      // Map filters to SolrServlet by name (same order as web.xml)
      for (var filterClass :
          List.<Class<? extends Filter>>of(
              RequiredSolrRequestFilter.class,
              RateLimitFilter.class,
              TracingFilter.class,
              AuthenticationFilter.class)) {
        FilterHolder fh = root.getServletHandler().newFilterHolder(Source.EMBEDDED);
        fh.setName(filterClass.getSimpleName());
        fh.setHeldClass(filterClass);
        root.getServletHandler().addFilter(fh);
        FilterMapping fm = new FilterMapping();
        fm.setFilterName(fh.getName());
        fm.setServletNames(new String[] {"SolrServlet"});
        fm.setDispatcherTypes(EnumSet.of(DispatcherType.REQUEST));
        root.getServletHandler().addFilterMapping(fm);
      }

      // TODO: end area that should be driven by web.xml and webdefault.xml
      chain = root;
    }

    chain = injectJettyHandlers(chain);

    if (config.enableV2) {
      RewriteHandler rwh = new RewriteHandler();
      rwh.setHandler(chain);
      rwh.setOriginalPathAttribute("requestedPath");
      rwh.addRule(new RewritePatternRule("/api/*", "/solr/____v2"));
      chain = rwh;
    }

    server.setHandler(chain);

    if (config.enableGracefulShutdown) {
      // Mimic "graceful.mod"
      GracefulHandler graceful = new GracefulHandler();
      server.insertHandler(graceful);
      server.setStopTimeout(15 * 1000);
    }
  }

  /**
   * descendants may inject own handler chaining it to the given root and then returning that own
   * one
   */
  protected Handler.Wrapper injectJettyHandlers(Handler.Wrapper chain) {
    return chain;
  }

  /**
   * @return the first filter implemented by the specified class, or throws an exception
   */
  public <T extends Filter> T getFilter(Class<T> filterClass) {
    return Arrays.stream(solrServlet.getServletHandler().getFilters())
        .filter(fh -> fh.getHeldClass() == filterClass)
        .map(fh -> filterClass.cast(fh.getFilter()))
        .findFirst()
        .orElseThrow(
            () -> new NoSuchElementException("No filter of class: " + filterClass.getName()));
  }

  @Override
  public CoreContainer getCoreContainer() {
    SolrServlet servlet;
    try {
      servlet = (SolrServlet) solrServlet.getServlet();
    } catch (ServletException e1) {
      throw new RuntimeException(e1);
    }
    if (servlet == null) {
      return null;
    }
    try {
      return servlet.getCores();
    } catch (UnavailableException e) {
      return null;
    }
  }

  public String getNodeName() {
    if (getCoreContainer() == null) {
      return null;
    }
    return getCoreContainer().getZkController().getNodeName();
  }

  public boolean isRunning() {
    return server.isRunning() && solrServlet != null && solrServlet.isRunning();
  }

  public boolean isStopped() {
    return (server.isStopped() && solrServlet == null)
        || (server.isStopped()
            && solrServlet.isStopped()
            && ((QueuedThreadPool) server.getThreadPool()).isStopped());
  }

  // ------------------------------------------------------------------------------------------------
  // ------------------------------------------------------------------------------------------------

  /**
   * Start the Jetty server
   *
   * <p>If the server has been started before, it will restart using the same port
   *
   * @throws Exception if an error occurs on startup
   */
  public void start() throws Exception {
    start(true);
  }

  /**
   * Start the Jetty server
   *
   * @param reusePort when true, will start up on the same port as used by any previous runs of this
   *     JettySolrRunner. If false, will use the port specified by the server's JettyConfig.
   * @throws Exception if an error occurs on startup
   */
  public synchronized void start(boolean reusePort) throws Exception {
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();

    try {
      int port = reusePort && jettyPort != -1 ? jettyPort : this.config.port;
      log.info("Start Jetty (configured port={}, binding port={})", this.config.port, port);

      // Release the reservation on the port we are about to bind, whether this runner's
      // stop() left it or another runner was stopped on the same port.
      releasePortReservation(port);

      // if started before, make a new server
      if (startedBefore) {
        init(port);
      } else {
        startedBefore = true;
      }

      if (!server.isRunning()) {
        if (config.portRetryTime > 0) {
          retryOnPortBindFailure(config.portRetryTime, port);
        } else {
          server.start();
        }
      }
      assert solrServlet.isRunning();

      if (config.waitForLoadingCoresToFinishMs != null
          && config.waitForLoadingCoresToFinishMs > 0L) {
        waitForLoadingCoresToFinish(config.waitForLoadingCoresToFinishMs);
      }

      setProtocolAndHost();

      IOUtils.closeQuietly(jettySolrClient);
      jettySolrClient = null;

      if (enableProxy) {
        if (started) {
          proxy.reopen();
        } else {
          proxy.open(getBaseUrl().toURI());
        }
      }

    } finally {
      started = true;
      if (prevContext != null) {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  private void setProtocolAndHost() {
    String protocol;

    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new IllegalStateException("Jetty Server has no Connectors");
    }
    ServerConnector c = (ServerConnector) conns[0];

    protocol = c.getDefaultProtocol().toLowerCase(Locale.ROOT).startsWith("ssl") ? "https" : "http";

    this.protocol = protocol;
    this.host = c.getHost();
  }

  private void retryOnPortBindFailure(int portRetryTime, int port) throws Exception {
    TimeOut timeout = new TimeOut(portRetryTime, TimeUnit.SECONDS, TimeSource.NANO_TIME);
    int tryCnt = 1;
    while (true) {
      try {
        tryCnt++;
        log.info("Trying to start Jetty on port {} try number {} ...", port, tryCnt);
        server.start();
        break;
      } catch (IOException ioe) {
        Exception e = lookForBindException(ioe);
        if (e instanceof BindException) {
          log.info("Port is in use, will try again until timeout of {}", timeout);
          server.stop();
          Thread.sleep(3000);
          if (!timeout.hasTimedOut()) {
            continue;
          }
        }

        throw e;
      }
    }
  }

  /**
   * Traverses the cause chain looking for a BindException. Returns either a bind exception that was
   * found in the chain or the original argument.
   *
   * @param ioe An IOException that might wrap a BindException
   * @return A bind exception if present otherwise ioe
   */
  @SuppressWarnings(
      "ReferenceEquality") // detecting a self-referencing exception cause loop, by identity
  Exception lookForBindException(IOException ioe) {
    Exception e = ioe;
    while (e.getCause() != null && !(e == e.getCause()) && !(e instanceof BindException)) {
      if (e.getCause() instanceof Exception) {
        e = (Exception) e.getCause();
        if (e instanceof BindException) {
          return e;
        }
      }
    }
    return ioe;
  }

  /**
   * Stop the Jetty server
   *
   * @throws Exception if an error occurs on shutdown
   */
  public synchronized void stop() throws Exception {
    // Do not let Jetty/Solr pollute the MDC for this thread
    Map<String, String> prevContext = MDC.getCopyOfContextMap();
    MDC.clear();
    try {
      IOUtils.closeQuietly(jettySolrClient);
      jettySolrClient = null;

      if (enableProxy) {
        proxy.close();
      }

      QueuedThreadPool qtp = (QueuedThreadPool) server.getThreadPool();
      ReservedThreadExecutor rte = qtp.getBean(ReservedThreadExecutor.class);

      try {
        server.stop();
      } catch (TimeoutException e) {
        log.warn("Jetty server graceful stop timed out; proceeding with forceful cleanup", e);
      }

      // stop timeout is 0, so we will interrupt right away
      while (!qtp.isStopped()) {
        qtp.stop();
        if (qtp.isStopped()) {
          Thread.sleep(50);
        }
      }

      // we tried to kill everything, now we wait for executor to stop
      qtp.setStopTimeout(Integer.MAX_VALUE);
      qtp.stop();
      qtp.join();

      if (rte != null) {
        // we try and wait for the reserved thread executor, but it doesn't always seem to work
        // so we actually set 0 reserved threads at creation

        rte.stop();

        TimeOut timeout = new TimeOut(30, TimeUnit.SECONDS, TimeSource.NANO_TIME);
        timeout.waitFor("Timeout waiting for reserved executor to stop.", rte::isStopped);
      }

      do {
        try {
          server.join();
        } catch (InterruptedException e) {
          // ignore
        }
      } while (!server.isStopped());

      // Hold the port until the next start on it, so that another process cannot take
      // it in the gap and make a restart fail with BindException.
      reserveJettyPort();

    } finally {
      if (prevContext != null) {
        MDC.setContextMap(prevContext);
      } else {
        MDC.clear();
      }
    }
  }

  /**
   * Holds this runner's port after stop, by binding a socket to it, until the reservation is
   * released; see {@link #RESERVED_PORTS} for the exact release rules. If the port cannot be held,
   * a restart behaves as it did without reservations: it binds the port if it is still free, and
   * the usual bind retry applies if it is not.
   */
  private void reserveJettyPort() {
    if (jettyPort <= 0) {
      return;
    }
    if (RESERVED_PORTS.containsKey(jettyPort)) {
      // A reservation for this port is already held: a second stop() on this runner
      // (close() after stop(), or a stop followed by cluster shutdown), or another
      // runner stopped on the same port. Binding again would fail against the held
      // socket and log a warning claiming the port is unprotected when it is not.
      return;
    }
    ServerSocket socket = null;
    try {
      socket = new ServerSocket();
      // Address reuse lets this bind succeed over connection sockets the stopped server
      // left in TIME_WAIT (its connectors bind with reuse set as well); without it, a
      // runner that served traffic before stopping could not reserve its port at all.
      // The listening socket held here still refuses every later bind, with or without
      // reuse, on Linux; address-reuse bind semantics differ on Windows, where the
      // same exclusion is not verified.
      socket.setReuseAddress(true);
      socket.bind(new InetSocketAddress("127.0.0.1", jettyPort));
    } catch (IOException e) {
      log.warn(
          "Could not reserve port {} after stop; a restart on this port is not protected",
          jettyPort,
          e);
      IOUtils.closeQuietly(socket);
      return;
    }
    if (RESERVED_PORTS.putIfAbsent(jettyPort, socket) != null) {
      IOUtils.closeQuietly(socket);
    }
  }

  /**
   * Releases the reservation held on this runner's current port, if any. Called by
   * MiniSolrCloudCluster.shutdown() for the runners still in the cluster at shutdown, whose runners
   * will not start again, and by {@link #close()}, after which this runner will not start again
   * either. A runner removed from its cluster by {@code stopJettySolrRunner} is not covered by the
   * shutdown call: if it is never closed, its reservation stays in place until a runner starts on
   * the port or the JVM exits. The same applies to the old port of a runner restarted on a
   * different port, because this method names only the port the runner currently holds.
   */
  public void releasePortReservation() {
    releasePortReservation(jettyPort);
  }

  private static void releasePortReservation(int port) {
    ServerSocket socket = RESERVED_PORTS.remove(port);
    if (socket != null) {
      IOUtils.closeQuietly(socket);
    }
  }

  public void outputMetrics(PrintStream out) throws IOException {
    if (getCoreContainer() != null) {
      SolrMetricManager metricsManager = getCoreContainer().getMetricManager();

      Set<String> registryNames = metricsManager.registryNames();
      for (String registryName : registryNames) {
        var prometheusReader = metricsManager.getPrometheusMetricReader(registryName);
        if (prometheusReader != null) {
          out.println();
          out.println("# Registry: " + registryName);
          out.println();
          new PrometheusTextFormatWriter(false).write(out, prometheusReader.collect());
        }
      }
    } else {
      throw new IllegalStateException("No CoreContainer found");
    }
  }

  public void dumpCoresInfo(PrintStream pw) {
    if (getCoreContainer() != null) {
      final var coreStatusReq = new CoresApi.GetAllCoreStatus();
      coreStatusReq.setIndexInfo(true);
      try (final var client = new HttpJettySolrClient.Builder(getBaseUrl().toString()).build()) {
        final var coreStatusRsp = coreStatusReq.process(client);
        Utils.writeJson(coreStatusRsp, pw, true);
      } catch (SolrServerException | IOException e) {
        // Worth logging but not re-throwing
        log.error("Unable to dump info for all cores", e);
      }
    }
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @exception RuntimeException if there is no Connector
   */
  private int getFirstConnectorPort() {
    Connector[] conns = server.getConnectors();
    if (0 == conns.length) {
      throw new RuntimeException("Jetty Server has no Connectors");
    }
    return ((ServerConnector) conns[0]).getLocalPort();
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort() {
    return getLocalPort(false);
  }

  /**
   * Returns the Local Port of the jetty Server.
   *
   * @param internalPort pass true to get the true jetty port rather than the proxy port if
   *     configured
   * @exception RuntimeException if there is no Connector
   */
  public int getLocalPort(boolean internalPort) {
    if (jettyPort == -1) {
      throw new IllegalStateException("You cannot get the port until this instance has started");
    }
    if (internalPort) {
      return jettyPort;
    }
    return (proxyPort != -1) ? proxyPort : jettyPort;
  }

  /**
   * Sets the port of a local socket proxy that sits in front of this server; if set then all client
   * traffic will flow through the proxy, giving us the ability to simulate network partitions very
   * easily.
   */
  public void setProxyPort(int proxyPort) {
    this.proxyPort = proxyPort;
  }

  private URI getBaseUri(int jettyPort, String path) {
    try {
      return new URI(protocol, null, host, jettyPort, path, null, null);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns a base URL like {@code http://localhost:8983/solr} */
  public URL getBaseUrl() {
    try {
      return getBaseUri(jettyPort, "/solr").toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  public URL getBaseURLV2() {
    try {
      return getBaseUri(jettyPort, "/api").toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a base URL consisting of the protocol, host, and port for a Connector in use by the
   * Jetty Server contained in this runner.
   */
  public URL getProxyBaseUrl() {
    try {
      return getBaseUri(getLocalPort(), "/solr").toURL();
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  // --------------------------------------------------------------
  // --------------------------------------------------------------

  /** A main class that starts jetty+solr This is useful for debugging */
  public static void main(String[] args) throws Exception {
    JettySolrRunner jetty = new JettySolrRunner(".", 8983);
    jetty.start();
  }

  /**
   * @return the Solr home directory of this JettySolrRunner
   */
  public String getSolrHome() {
    return solrHome;
  }

  /**
   * @return this node's properties
   */
  public Properties getNodeProperties() {
    return nodeProperties;
  }

  private void waitForLoadingCoresToFinish(long timeoutMs) {
    CoreContainer cores = getCoreContainer();
    if (cores == null) {
      throw new IllegalStateException("solrServlet/coreContainer is not set/available!");
    }
    cores.waitForLoadingCoresToFinish(timeoutMs);
  }

  public SocketProxy getProxy() {
    return proxy;
  }

  /**
   * Creates a REST client useful for HTTP operations. It closes when this {@link JettySolrRunner}
   * is stopped.
   */
  public RestTestHarness getRestClient(String collection) {
    String path = "/solr";
    if (collection != null) {
      path += "/" + collection;
    }
    return new RestTestHarness(getSolrClient().getHttpClient(), getBaseUri(jettyPort, path));
  }

  // ---- SolrBackend implementation ----

  @Override
  public HttpJettySolrClient newSolrClient(String collection) {
    return new HttpJettySolrClient.Builder(getBaseUrl().toString())
        .withDefaultCollection(collection)
        .build();
  }

  @Override
  public synchronized HttpJettySolrClient getSolrClient() {
    if (jettySolrClient == null) {
      jettySolrClient = new HttpJettySolrClient.Builder(getBaseUrl().toString()).build();
    }
    return jettySolrClient;
  }

  private EmbeddedSolrBackend getEmbeddedSolrBackend() {
    var container = getCoreContainer();
    if (container.isZooKeeperAware()) {
      throw new IllegalStateException(
          "Don't call SolrBackend methods in SolrCloud on JettySolrRunner");
    }
    return new EmbeddedSolrBackend(container); // cheap
  }

  @Override
  public void createCollection(CollectionAdminRequest.Create create) {
    getEmbeddedSolrBackend().createCollection(create);
  }

  @Override
  public boolean hasCollection(String name) {
    return getEmbeddedSolrBackend().hasCollection(name);
  }

  @Override
  public void reloadCollection(String name) throws SolrServerException, IOException {
    getEmbeddedSolrBackend().reloadCollection(name);
  }

  @Override
  public String getBaseUrl(Random r) {
    return getBaseUrl().toString();
  }

  @Override
  public void close() {
    try {
      stop();
    } catch (Exception e) {
      log.error(e.toString(), e); // nowarn
    }
    // close() is the end of this runner's life, unlike stop(), which is one half of the
    // stop and restart cycle the reservation protects. Give the port back instead of
    // leaving stop()'s reservation in place until the JVM exits. Runners still in a
    // MiniSolrCloudCluster get the same release from its shutdown().
    releasePortReservation();
  }
}
