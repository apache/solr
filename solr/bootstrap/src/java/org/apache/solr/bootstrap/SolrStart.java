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
package org.apache.solr.bootstrap;

import java.lang.invoke.MethodHandles;
import java.nio.file.Path;
import java.util.Random;
import org.apache.solr.common.util.EnvUtils;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.session.DefaultSessionIdManager;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Main entry point for Solr bootstrap (SIP-6 Phase 1). Replaces Jetty's start.jar with native Solr
 * code that programmatically configures and starts Jetty. This gives Solr full control over the
 * bootstrap process.
 *
 * <p>Usage: java -jar solr-start.jar [with system properties]
 *
 * <p>Required system properties:
 *
 * <ul>
 *   <li>solr.solr.home - Solr home directory
 *   <li>solr.install.dir - Solr installation directory
 *   <li>solr.logs.dir - Logs directory
 *   <li>jetty.home - Jetty home directory (usually same as server/)
 * </ul>
 *
 * <p>Optional system properties:
 *
 * <ul>
 *   <li>solr.port.listen - Port to listen on (default: 8983)
 *   <li>solr.host.bind - Host to bind to (default: 127.0.0.1)
 *   <li>STOP.PORT - Port for shutdown monitor (default: none)
 *   <li>STOP.KEY - Key for shutdown authentication
 *   <li>...and many more (see ServerConfiguration)
 * </ul>
 */
public class SolrStart {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void main(String[] args) throws Exception {
    // 1. Install SLF4J bridge for java.util.logging (from jetty.xml)
    SLF4JBridgeHandler.removeHandlersForRootLogger();
    SLF4JBridgeHandler.install();

    // 2. Log bootstrap start
    log.info("╔══════════════════════════════════════════════════════════════╗");
    log.info("║  Starting Solr via Solr bootstrap (not Jetty start.jar)     ║");
    log.info("║  SIP-6 Phase 1: Solr owns the bootstrap process              ║");
    log.info("╚══════════════════════════════════════════════════════════════╝");

    // 2.5. Configure SSL (auto-generation or user-provided keystores)
    SslCertificateGenerator.SslConfiguration sslConfig = SslCertificateGenerator.configureSsl();

    if (sslConfig != null) {
      // Set server-side SSL properties
      if (sslConfig.isGenerated()) {
        // Auto-generated certificate
        System.setProperty("solr.jetty.https.port", System.getProperty("solr.port.listen", "8983"));
        System.setProperty("solr.ssl.key.store", sslConfig.getGeneratedKeystorePath().toString());
        System.setProperty("solr.ssl.key.store.password", sslConfig.getGeneratedPassword());
        System.setProperty("solr.ssl.key.store.type", "PKCS12");
        System.setProperty("solr.ssl.trust.store", sslConfig.getGeneratedKeystorePath().toString());
        System.setProperty("solr.ssl.trust.store.password", sslConfig.getGeneratedPassword());
        System.setProperty("solr.ssl.trust.store.type", "PKCS12");
      } else {
        // User provided keystores - enable HTTPS
        System.setProperty("solr.jetty.https.port", System.getProperty("solr.port.listen", "8983"));
      }

      // Set client-side SSL properties (for outbound HTTP connections)
      configureClientSsl();
    }

    // 3. Load configuration from system properties
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();
    if (log.isInfoEnabled()) {
      log.info(
          "Solr bootstrap configuration: port={}, host={}, solrHome={}, jettyHome={}",
          config.getPort(),
          config.getHost(),
          config.getSolrHome(),
          config.getJettyHome());
      log.info(
          "Thread pool: minThreads={}, maxThreads={}, idleTimeout={}ms",
          config.getMinThreads(),
          config.getMaxThreads(),
          config.getThreadIdleTimeout());
    }

    // 4. Create thread pool
    QueuedThreadPool threadPool = new QueuedThreadPool();
    threadPool.setMinThreads(config.getMinThreads());
    threadPool.setMaxThreads(config.getMaxThreads());
    threadPool.setIdleTimeout(config.getThreadIdleTimeout());
    threadPool.setStopTimeout(config.getThreadStopTimeout());
    threadPool.setReservedThreads(0);
    threadPool.setDetailedDump(false);

    // 5. Create and configure Server
    Server server = new Server(threadPool);
    server.manage(threadPool);
    server.setStopAtShutdown(true);
    server.setStopTimeout(config.getStopTimeout());

    // 6. Create connectors (HTTP or HTTPS)
    ConnectorFactory connectorFactory = new ConnectorFactory(config);
    ServerConnector[] connectors = connectorFactory.createConnectors(server);
    server.setConnectors(connectors);

    if (log.isInfoEnabled()) {
      String host = config.getHost();
      if (config.isHttpsEnabled()) {
        int httpsPort = config.getHttpsPort();
        log.info("Created HTTPS connector on {}:{}", host, httpsPort);
      } else {
        int port = config.getPort();
        log.info("Created HTTP connector on {}:{}", host, port);
      }
    }

    // 7. Add session management
    server.addBean(new DefaultSessionIdManager(server, new Random()), true);

    // 8. Build handler chain
    HandlerChainBuilder handlerBuilder = new HandlerChainBuilder(config);
    Handler handler = handlerBuilder.buildHandlerChain(server);
    server.setHandler(handler);

    // 9. Register shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    log.info("Solr bootstrap shutdown initiated via shutdown hook");
                    server.stop();
                  } catch (Exception e) {
                    log.error("Error during shutdown", e);
                  }
                },
                "SolrBootstrapShutdownHook"));

    // 10. Start server
    server.start();

    // Log actual bound port (may differ if port was 0)
    int actualPort = ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    log.info("╔══════════════════════════════════════════════════════════════╗");
    if (log.isInfoEnabled()) {
      log.info(
          "║  Solr bootstrap started successfully on {}:{}{}║",
          config.getHost(),
          actualPort,
          actualPort < 10000 ? "                  " : "                 ");
    }
    log.info("╚══════════════════════════════════════════════════════════════╝");

    // 11. Jetty's ShutdownMonitor automatically initializes based on STOP.PORT/STOP.KEY system
    // properties
    // No manual configuration needed - Jetty handles this internally when those properties are set
    int stopPort = config.getStopPort();
    if (stopPort > 0 && log.isInfoEnabled()) {
      log.info(
          "Shutdown monitor configured via STOP.PORT={} (Jetty auto-initialization)", stopPort);
    }

    // 12. Wait for server to finish
    server.join();

    log.info("╔══════════════════════════════════════════════════════════════╗");
    log.info("║  Solr bootstrap shutdown complete                            ║");
    log.info("╚══════════════════════════════════════════════════════════════╝");
  }

  /**
   * Configure client SSL properties (javax.net.ssl.*) for outbound HTTPS connections. This is
   * required for SolrCloud node-to-node communication when SSL is enabled.
   *
   * <p>Implements fallback pattern from old bin/solr: use client-specific keystores if provided,
   * otherwise fallback to server keystores.
   */
  private static void configureClientSsl() {
    // Client keystore (fallback to server keystore if not specified)
    String clientKeyStore = EnvUtils.getProperty("solr.ssl.client.key.store");
    if (clientKeyStore == null) {
      clientKeyStore = EnvUtils.getProperty("solr.ssl.key.store");
    }
    if (clientKeyStore != null) {
      System.setProperty("javax.net.ssl.keyStore", clientKeyStore);

      String clientKeyStoreType = EnvUtils.getProperty("solr.ssl.client.key.store.type");
      if (clientKeyStoreType == null) {
        clientKeyStoreType = EnvUtils.getProperty("solr.ssl.key.store.type", "PKCS12");
      }
      System.setProperty("javax.net.ssl.keyStoreType", clientKeyStoreType);
    }

    // Client truststore (fallback to server truststore if not specified)
    String clientTrustStore = EnvUtils.getProperty("solr.ssl.client.trust.store");
    if (clientTrustStore == null) {
      clientTrustStore = EnvUtils.getProperty("solr.ssl.trust.store");
    }
    if (clientTrustStore != null) {
      System.setProperty("javax.net.ssl.trustStore", clientTrustStore);

      String clientTrustStoreType = EnvUtils.getProperty("solr.ssl.client.trust.store.type");
      if (clientTrustStoreType == null) {
        clientTrustStoreType = EnvUtils.getProperty("solr.ssl.trust.store.type", "PKCS12");
      }
      System.setProperty("javax.net.ssl.trustStoreType", clientTrustStoreType);
    }

    // Set parent paths for security manager when SSL reload is enabled
    if (EnvUtils.getPropertyAsBool("solr.keystore.reload.enabled", true)
        && EnvUtils.getPropertyAsBool("solr.security.manager.enabled", false)) {
      if (clientKeyStore != null) {
        System.setProperty(
            "javax.net.ssl.keyStoreParentPath", Path.of(clientKeyStore).getParent().toString());
      }
    }

    log.info("Client SSL configured: keyStore={}, trustStore={}", clientKeyStore, clientTrustStore);
  }
}
