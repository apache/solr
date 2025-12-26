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
import java.util.Random;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.session.DefaultSessionIdManager;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Main entry point for Solr bootstrap (SIP-6 Phase 1). Replaces Jetty's start.jar with native
 * Solr code that programmatically configures and starts Jetty. This gives Solr full control over
 * the bootstrap process.
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

    // 3. Load configuration from system properties
    ServerConfiguration config = ServerConfiguration.fromSystemProperties();
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

    if (config.isHttpsEnabled()) {
      log.info("Created HTTPS connector on {}:{}", config.getHost(), config.getHttpsPort());
    } else {
      log.info("Created HTTP connector on {}:{}", config.getHost(), config.getPort());
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
    log.info(
        "║  Solr bootstrap started successfully on {}:{}{}║",
        config.getHost(),
        actualPort,
        actualPort < 10000 ? "                  " : "                 ");
    log.info("╚══════════════════════════════════════════════════════════════╝");

    // 11. Jetty's ShutdownMonitor automatically initializes based on STOP.PORT/STOP.KEY system properties
    // No manual configuration needed - Jetty handles this internally when those properties are set
    if (config.getStopPort() > 0) {
      log.info(
          "Shutdown monitor configured via STOP.PORT={} (Jetty auto-initialization)",
          config.getStopPort());
    }

    // 12. Wait for server to finish
    server.join();

    log.info("╔══════════════════════════════════════════════════════════════╗");
    log.info("║  Solr bootstrap shutdown complete                            ║");
    log.info("╚══════════════════════════════════════════════════════════════╝");
  }
}
