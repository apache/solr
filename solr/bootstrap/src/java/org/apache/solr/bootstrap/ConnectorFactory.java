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

import org.apache.solr.util.configuration.SSLConfigurationsFactory;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.http2.HTTP2Cipher;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;

/**
 * Factory for creating HTTP and HTTPS ServerConnectors with HTTP/2 and ALPN support. Mirrors the
 * configuration from jetty-http.xml and jetty-https.xml.
 */
public class ConnectorFactory {

  private final ServerConfiguration config;

  public ConnectorFactory(ServerConfiguration config) {
    this.config = config;
  }

  /**
   * Create connectors based on configuration. Returns either HTTP or HTTPS connector.
   *
   * @param server the Jetty server
   * @return array of ServerConnectors
   */
  public ServerConnector[] createConnectors(Server server) {
    if (config.isHttpsEnabled()) {
      return new ServerConnector[] {createHttpsConnector(server)};
    } else {
      return new ServerConnector[] {createHttpConnector(server)};
    }
  }

  /**
   * Create HTTP connector with HTTP/2C (cleartext) support.
   *
   * @param server the Jetty server
   * @return HTTP ServerConnector
   */
  private ServerConnector createHttpConnector(Server server) {
    HttpConfiguration httpConfig = createHttpConfiguration(false);

    // HTTP/1.1
    HttpConnectionFactory http1 = new HttpConnectionFactory(httpConfig);

    // HTTP/2 cleartext
    HTTP2CServerConnectionFactory http2c = new HTTP2CServerConnectionFactory(httpConfig);

    ServerConnector connector = new ServerConnector(server, http1, http2c);
    configureConnectorBasics(connector, config.getPort());

    return connector;
  }

  /**
   * Create HTTPS connector with HTTP/2 and ALPN support.
   *
   * @param server the Jetty server
   * @return HTTPS ServerConnector
   */
  private ServerConnector createHttpsConnector(Server server) {
    // Initialize SSL configurations factory (normally done in jetty-ssl.xml)
    SSLConfigurationsFactory.current().init();

    // Create SSL context factory
    SslContextFactory.Server sslContextFactory = createSslContextFactory();

    // Create HTTPS configuration
    HttpConfiguration httpsConfig = createHttpConfiguration(true);
    httpsConfig.addCustomizer(
        new SecureRequestCustomizer(
            config.isSniRequired(),
            config.isSniHostCheck(),
            config.getStsMaxAge(),
            config.isStsIncludeSubdomains()));

    // Connection factories
    HttpConnectionFactory http1 = new HttpConnectionFactory(httpsConfig);
    HTTP2ServerConnectionFactory http2 = new HTTP2ServerConnectionFactory(httpsConfig);

    // ALPN for protocol negotiation (h2 = HTTP/2, http/1.1 = HTTP/1.1)
    ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory("h2", "http/1.1");
    alpn.setDefaultProtocol("http/1.1");

    // SSL connection factory wraps ALPN
    SslConnectionFactory ssl = new SslConnectionFactory(sslContextFactory, "alpn");

    ServerConnector connector = new ServerConnector(server, ssl, alpn, http2, http1);
    configureConnectorBasics(connector, config.getHttpsPort());

    // Setup SSL keystore reload if enabled
    SslReloadHandler.setupSslReload(server, sslContextFactory);

    return connector;
  }

  /**
   * Create HttpConfiguration with settings from ServerConfiguration.
   *
   * @param secure whether this is for HTTPS
   * @return configured HttpConfiguration
   */
  private HttpConfiguration createHttpConfiguration(boolean secure) {
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme("https");
    httpConfig.setSecurePort(config.getSecurePort());
    httpConfig.setOutputBufferSize(config.getOutputBufferSize());
    httpConfig.setOutputAggregationSize(config.getOutputAggregationSize());
    httpConfig.setRequestHeaderSize(config.getRequestHeaderSize());
    httpConfig.setResponseHeaderSize(config.getResponseHeaderSize());
    httpConfig.setSendServerVersion(config.isSendServerVersion());
    httpConfig.setSendDateHeader(config.isSendDateHeader());
    httpConfig.setHeaderCacheSize(config.getHeaderCacheSize());
    httpConfig.setDelayDispatchUntilContent(config.isDelayDispatchUntilContent());
    httpConfig.setRelativeRedirectAllowed(true);

    return httpConfig;
  }

  /**
   * Create SSL context factory with settings from ServerConfiguration.
   *
   * @return configured SslContextFactory.Server
   */
  private SslContextFactory.Server createSslContextFactory() {
    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

    // Set keystore and truststore
    if (config.getKeyStorePath() != null) {
      sslContextFactory.setKeyStorePath(config.getKeyStorePath());
    }
    if (config.getKeyStorePassword() != null) {
      sslContextFactory.setKeyStorePassword(config.getKeyStorePassword());
    }
    if (config.getTrustStorePath() != null) {
      sslContextFactory.setTrustStorePath(config.getTrustStorePath());
    }
    if (config.getTrustStorePassword() != null) {
      sslContextFactory.setTrustStorePassword(config.getTrustStorePassword());
    }

    // Client auth
    sslContextFactory.setNeedClientAuth(config.isNeedClientAuth());
    sslContextFactory.setWantClientAuth(config.isWantClientAuth());

    // Keystore/truststore types
    sslContextFactory.setKeyStoreType(config.getKeyStoreType());
    sslContextFactory.setTrustStoreType(config.getTrustStoreType());

    // HTTP/2 cipher configuration
    sslContextFactory.setCipherComparator(HTTP2Cipher.COMPARATOR);
    sslContextFactory.setUseCipherSuitesOrder(true);

    // SNI
    sslContextFactory.setSniRequired(config.isSniRequired());

    // SSL provider
    if (config.getSslProvider() != null) {
      sslContextFactory.setProvider(config.getSslProvider());
    }

    return sslContextFactory;
  }

  /**
   * Configure basic connector settings (host, port, idle timeout, reuse address).
   *
   * @param connector the ServerConnector to configure
   * @param port the port to listen on
   */
  private void configureConnectorBasics(ServerConnector connector, int port) {
    connector.setHost(config.getHost());
    connector.setPort(port);
    connector.setIdleTimeout(config.getHttpIdleTimeout());
    connector.setReuseAddress(true);
  }
}
