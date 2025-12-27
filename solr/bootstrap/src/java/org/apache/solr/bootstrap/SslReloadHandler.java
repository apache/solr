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
import org.apache.solr.common.util.EnvUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.ssl.KeyStoreScanner;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles SSL keystore reload functionality using Jetty's KeyStoreScanner. This allows automatic
 * reloading of SSL certificates when keystore files change, without requiring a server restart.
 *
 * <p>This replaces the --module=ssl-reload functionality from the old bin/solr script.
 *
 * @since Solr 11.0 (SIP-6 Phase 1)
 */
public class SslReloadHandler {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Setup SSL keystore reload if enabled via SOLR_KEYSTORE_RELOAD_ENABLED (defaults to true).
   *
   * @param server Jetty server instance
   * @param sslContextFactory SSL context factory to monitor
   */
  public static void setupSslReload(Server server, SslContextFactory.Server sslContextFactory) {
    // Check if SSL reload is enabled (default: true, matches old bin/solr behavior)
    if (!EnvUtils.getPropertyAsBool("solr.keystore.reload.enabled", true)) {
      log.info("SSL keystore reload disabled");
      return;
    }

    // Get keystore path
    String keystorePath = EnvUtils.getProperty("solr.ssl.key.store");
    if (keystorePath == null) {
      log.warn("Cannot setup SSL reload: no keystore path configured");
      return;
    }

    // Create KeyStoreScanner
    KeyStoreScanner scanner = new KeyStoreScanner(sslContextFactory);
    // Default scan interval: 60 seconds (can be overridden via SOLR_KEYSTORE_RELOAD_INTERVAL)
    scanner.setScanInterval(EnvUtils.getPropertyAsInteger("solr.keystore.reload.interval", 60));

    // Add as server bean for lifecycle management
    server.addBean(scanner);

    if (log.isInfoEnabled()) {
      log.info(
          "SSL keystore reload enabled: watching {} every {} seconds",
          keystorePath,
          scanner.getScanInterval());
    }
  }
}
