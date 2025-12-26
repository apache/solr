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

import org.apache.solr.common.util.EnvUtils;

/**
 * Configuration object for Solr bootstrap, parsing all system properties using EnvUtils. This
 * replaces the XML-based configuration from jetty*.xml files with a Java-based approach.
 */
public class ServerConfiguration {

  // Core properties
  private final int port;
  private final String host;
  private final String solrHome;
  private final String installDir;
  private final String logsDir;
  private final String jettyHome;

  // Thread pool
  private final int minThreads;
  private final int maxThreads;
  private final int threadIdleTimeout;
  private final int threadStopTimeout;

  // HTTP configuration
  private final int securePort;
  private final int outputBufferSize;
  private final int outputAggregationSize;
  private final int requestHeaderSize;
  private final int responseHeaderSize;
  private final boolean sendServerVersion;
  private final boolean sendDateHeader;
  private final int headerCacheSize;
  private final boolean delayDispatchUntilContent;
  private final int httpIdleTimeout;

  // HTTPS/SSL
  private final boolean httpsEnabled;
  private final int httpsPort;
  private final String keyStorePath;
  private final String keyStorePassword;
  private final String trustStorePath;
  private final String trustStorePassword;
  private final boolean needClientAuth;
  private final boolean wantClientAuth;
  private final String keyStoreType;
  private final String trustStoreType;
  private final boolean sniRequired;
  private final boolean sniHostCheck;
  private final int stsMaxAge;
  private final boolean stsIncludeSubdomains;
  private final String sslProvider;

  // GZIP
  private final boolean gzipEnabled;
  private final int gzipMinSize;
  private final String gzipIncludedMethods;
  private final int gzipDeflaterPoolCapacity;
  private final int gzipCompressionLevel;

  // IP Access Control
  private final String ipAccessIncludes;
  private final String ipAccessExcludes;

  // Shutdown
  private final int stopPort;
  private final String stopKey;
  private final int stopTimeout;

  // Request logging
  private final boolean requestLoggingEnabled;

  private ServerConfiguration(Builder builder) {
    this.port = builder.port;
    this.host = builder.host;
    this.solrHome = builder.solrHome;
    this.installDir = builder.installDir;
    this.logsDir = builder.logsDir;
    this.jettyHome = builder.jettyHome;
    this.minThreads = builder.minThreads;
    this.maxThreads = builder.maxThreads;
    this.threadIdleTimeout = builder.threadIdleTimeout;
    this.threadStopTimeout = builder.threadStopTimeout;
    this.securePort = builder.securePort;
    this.outputBufferSize = builder.outputBufferSize;
    this.outputAggregationSize = builder.outputAggregationSize;
    this.requestHeaderSize = builder.requestHeaderSize;
    this.responseHeaderSize = builder.responseHeaderSize;
    this.sendServerVersion = builder.sendServerVersion;
    this.sendDateHeader = builder.sendDateHeader;
    this.headerCacheSize = builder.headerCacheSize;
    this.delayDispatchUntilContent = builder.delayDispatchUntilContent;
    this.httpIdleTimeout = builder.httpIdleTimeout;
    this.httpsEnabled = builder.httpsEnabled;
    this.httpsPort = builder.httpsPort;
    this.keyStorePath = builder.keyStorePath;
    this.keyStorePassword = builder.keyStorePassword;
    this.trustStorePath = builder.trustStorePath;
    this.trustStorePassword = builder.trustStorePassword;
    this.needClientAuth = builder.needClientAuth;
    this.wantClientAuth = builder.wantClientAuth;
    this.keyStoreType = builder.keyStoreType;
    this.trustStoreType = builder.trustStoreType;
    this.sniRequired = builder.sniRequired;
    this.sniHostCheck = builder.sniHostCheck;
    this.stsMaxAge = builder.stsMaxAge;
    this.stsIncludeSubdomains = builder.stsIncludeSubdomains;
    this.sslProvider = builder.sslProvider;
    this.gzipEnabled = builder.gzipEnabled;
    this.gzipMinSize = builder.gzipMinSize;
    this.gzipIncludedMethods = builder.gzipIncludedMethods;
    this.gzipDeflaterPoolCapacity = builder.gzipDeflaterPoolCapacity;
    this.gzipCompressionLevel = builder.gzipCompressionLevel;
    this.ipAccessIncludes = builder.ipAccessIncludes;
    this.ipAccessExcludes = builder.ipAccessExcludes;
    this.stopPort = builder.stopPort;
    this.stopKey = builder.stopKey;
    this.stopTimeout = builder.stopTimeout;
    this.requestLoggingEnabled = builder.requestLoggingEnabled;
  }

  /** Create ServerConfiguration from system properties using EnvUtils */
  public static ServerConfiguration fromSystemProperties() {
    return new Builder()
        // Core properties
        .setPort(EnvUtils.getPropertyAsInteger("solr.port.listen", 8983))
        .setHost(EnvUtils.getProperty("solr.host.bind", "127.0.0.1"))
        .setSolrHome(EnvUtils.getProperty("solr.solr.home"))
        .setInstallDir(EnvUtils.getProperty("solr.install.dir"))
        .setLogsDir(EnvUtils.getProperty("solr.logs.dir"))
        .setJettyHome(EnvUtils.getProperty("jetty.home"))
        // Thread pool
        .setMinThreads(EnvUtils.getPropertyAsInteger("solr.jetty.threads.min", 10))
        .setMaxThreads(EnvUtils.getPropertyAsInteger("solr.jetty.threads.max", 10000))
        .setThreadIdleTimeout(
            EnvUtils.getPropertyAsInteger("solr.jetty.threads.idle.timeout", 120000))
        .setThreadStopTimeout(
            EnvUtils.getPropertyAsInteger("solr.jetty.threads.stop.timeout", 60000))
        // HTTP configuration
        .setSecurePort(EnvUtils.getPropertyAsInteger("solr.jetty.secure.port", 8443))
        .setOutputBufferSize(
            EnvUtils.getPropertyAsInteger("solr.jetty.output.buffer.size", 32768))
        .setOutputAggregationSize(
            EnvUtils.getPropertyAsInteger("solr.jetty.output.aggregation.size", 8192))
        .setRequestHeaderSize(
            EnvUtils.getPropertyAsInteger("solr.jetty.request.header.size", 8192))
        .setResponseHeaderSize(
            EnvUtils.getPropertyAsInteger("solr.jetty.response.header.size", 8192))
        .setSendServerVersion(
            EnvUtils.getPropertyAsBool("solr.jetty.send.server.version", false))
        .setSendDateHeader(EnvUtils.getPropertyAsBool("solr.jetty.send.date.header", false))
        .setHeaderCacheSize(EnvUtils.getPropertyAsInteger("solr.jetty.header.cache.size", 512))
        .setDelayDispatchUntilContent(
            EnvUtils.getPropertyAsBool("solr.jetty.delayDispatchUntilContent", false))
        .setHttpIdleTimeout(EnvUtils.getPropertyAsInteger("solr.jetty.http.idleTimeout", 120000))
        // HTTPS/SSL - enabled if https port is specified
        .setHttpsEnabled(EnvUtils.getProperty("solr.jetty.https.port") != null)
        .setHttpsPort(EnvUtils.getPropertyAsInteger("solr.jetty.https.port", 8443))
        .setKeyStorePath(
            EnvUtils.getProperty("solr.jetty.keystore", "./etc/solr-ssl.keystore.jks"))
        .setKeyStorePassword(EnvUtils.getProperty("solr.jetty.keystore.password"))
        .setTrustStorePath(
            EnvUtils.getProperty("solr.jetty.truststore", "./etc/solr-ssl.keystore.jks"))
        .setTrustStorePassword(EnvUtils.getProperty("solr.jetty.truststore.password"))
        .setNeedClientAuth(
            EnvUtils.getPropertyAsBool("solr.jetty.ssl.need.client.auth.enabled", false))
        .setWantClientAuth(
            EnvUtils.getPropertyAsBool("solr.jetty.ssl.want.client.auth.enabled", false))
        .setKeyStoreType(EnvUtils.getProperty("solr.jetty.keystore.type", "PKCS12"))
        .setTrustStoreType(EnvUtils.getProperty("solr.jetty.truststore.type", "PKCS12"))
        .setSniRequired(EnvUtils.getPropertyAsBool("solr.jetty.ssl.sniRequired", false))
        .setSniHostCheck(
            EnvUtils.getPropertyAsBool("solr.jetty.ssl.sni.host.check.enabled", true))
        .setStsMaxAge(EnvUtils.getPropertyAsInteger("solr.jetty.ssl.sts.max.age.secs", -1))
        .setStsIncludeSubdomains(
            EnvUtils.getPropertyAsBool("solr.jetty.ssl.sts.include.subdomains.enabled", false))
        .setSslProvider(EnvUtils.getProperty("solr.jetty.ssl.provider"))
        // GZIP
        .setGzipEnabled(true) // Always enabled in Phase 1
        .setGzipMinSize(EnvUtils.getPropertyAsInteger("jetty.gzip.minGzipSize", 2048))
        .setGzipIncludedMethods(EnvUtils.getProperty("jetty.gzip.includedMethodList", "GET,POST"))
        .setGzipDeflaterPoolCapacity(
            EnvUtils.getPropertyAsInteger("jetty.gzip.deflaterPoolCapacity", 1024))
        .setGzipCompressionLevel(EnvUtils.getPropertyAsInteger("jetty.gzip.compressionLevel", -1))
        // IP Access Control
        .setIpAccessIncludes(EnvUtils.getProperty("solr.jetty.inetaccess.includes", ""))
        .setIpAccessExcludes(EnvUtils.getProperty("solr.jetty.inetaccess.excludes", ""))
        // Shutdown
        .setStopPort(EnvUtils.getPropertyAsInteger("STOP.PORT", 0))
        .setStopKey(EnvUtils.getProperty("STOP.KEY", ""))
        .setStopTimeout(15000) // 15 seconds for graceful shutdown
        // Request logging
        .setRequestLoggingEnabled(
            EnvUtils.getPropertyAsBool("solr.jetty.requestlog.enabled", false))
        .build();
  }

  // Getters
  public int getPort() {
    return port;
  }

  public String getHost() {
    return host;
  }

  public String getSolrHome() {
    return solrHome;
  }

  public String getInstallDir() {
    return installDir;
  }

  public String getLogsDir() {
    return logsDir;
  }

  public String getJettyHome() {
    return jettyHome;
  }

  public int getMinThreads() {
    return minThreads;
  }

  public int getMaxThreads() {
    return maxThreads;
  }

  public int getThreadIdleTimeout() {
    return threadIdleTimeout;
  }

  public int getThreadStopTimeout() {
    return threadStopTimeout;
  }

  public int getSecurePort() {
    return securePort;
  }

  public int getOutputBufferSize() {
    return outputBufferSize;
  }

  public int getOutputAggregationSize() {
    return outputAggregationSize;
  }

  public int getRequestHeaderSize() {
    return requestHeaderSize;
  }

  public int getResponseHeaderSize() {
    return responseHeaderSize;
  }

  public boolean isSendServerVersion() {
    return sendServerVersion;
  }

  public boolean isSendDateHeader() {
    return sendDateHeader;
  }

  public int getHeaderCacheSize() {
    return headerCacheSize;
  }

  public boolean isDelayDispatchUntilContent() {
    return delayDispatchUntilContent;
  }

  public int getHttpIdleTimeout() {
    return httpIdleTimeout;
  }

  public boolean isHttpsEnabled() {
    return httpsEnabled;
  }

  public int getHttpsPort() {
    return httpsPort;
  }

  public String getKeyStorePath() {
    return keyStorePath;
  }

  public String getKeyStorePassword() {
    return keyStorePassword;
  }

  public String getTrustStorePath() {
    return trustStorePath;
  }

  public String getTrustStorePassword() {
    return trustStorePassword;
  }

  public boolean isNeedClientAuth() {
    return needClientAuth;
  }

  public boolean isWantClientAuth() {
    return wantClientAuth;
  }

  public String getKeyStoreType() {
    return keyStoreType;
  }

  public String getTrustStoreType() {
    return trustStoreType;
  }

  public boolean isSniRequired() {
    return sniRequired;
  }

  public boolean isSniHostCheck() {
    return sniHostCheck;
  }

  public int getStsMaxAge() {
    return stsMaxAge;
  }

  public boolean isStsIncludeSubdomains() {
    return stsIncludeSubdomains;
  }

  public String getSslProvider() {
    return sslProvider;
  }

  public boolean isGzipEnabled() {
    return gzipEnabled;
  }

  public int getGzipMinSize() {
    return gzipMinSize;
  }

  public String getGzipIncludedMethods() {
    return gzipIncludedMethods;
  }

  public int getGzipDeflaterPoolCapacity() {
    return gzipDeflaterPoolCapacity;
  }

  public int getGzipCompressionLevel() {
    return gzipCompressionLevel;
  }

  public String getIpAccessIncludes() {
    return ipAccessIncludes;
  }

  public String getIpAccessExcludes() {
    return ipAccessExcludes;
  }

  public boolean hasIpAccessRules() {
    return !ipAccessIncludes.isEmpty() || !ipAccessExcludes.isEmpty();
  }

  public int getStopPort() {
    return stopPort;
  }

  public String getStopKey() {
    return stopKey;
  }

  public int getStopTimeout() {
    return stopTimeout;
  }

  public boolean isRequestLoggingEnabled() {
    return requestLoggingEnabled;
  }

  /** Builder for ServerConfiguration */
  public static class Builder {
    private int port = 8983;
    private String host = "127.0.0.1";
    private String solrHome;
    private String installDir;
    private String logsDir;
    private String jettyHome;
    private int minThreads = 10;
    private int maxThreads = 10000;
    private int threadIdleTimeout = 120000;
    private int threadStopTimeout = 60000;
    private int securePort = 8443;
    private int outputBufferSize = 32768;
    private int outputAggregationSize = 8192;
    private int requestHeaderSize = 8192;
    private int responseHeaderSize = 8192;
    private boolean sendServerVersion = false;
    private boolean sendDateHeader = false;
    private int headerCacheSize = 512;
    private boolean delayDispatchUntilContent = false;
    private int httpIdleTimeout = 120000;
    private boolean httpsEnabled = false;
    private int httpsPort = 8443;
    private String keyStorePath = "./etc/solr-ssl.keystore.jks";
    private String keyStorePassword;
    private String trustStorePath = "./etc/solr-ssl.keystore.jks";
    private String trustStorePassword;
    private boolean needClientAuth = false;
    private boolean wantClientAuth = false;
    private String keyStoreType = "PKCS12";
    private String trustStoreType = "PKCS12";
    private boolean sniRequired = false;
    private boolean sniHostCheck = true;
    private int stsMaxAge = -1;
    private boolean stsIncludeSubdomains = false;
    private String sslProvider;
    private boolean gzipEnabled = true;
    private int gzipMinSize = 2048;
    private String gzipIncludedMethods = "GET,POST";
    private int gzipDeflaterPoolCapacity = 1024;
    private int gzipCompressionLevel = -1;
    private String ipAccessIncludes = "";
    private String ipAccessExcludes = "";
    private int stopPort = 0;
    private String stopKey = "";
    private int stopTimeout = 15000;
    private boolean requestLoggingEnabled = false;

    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setHost(String host) {
      this.host = host;
      return this;
    }

    public Builder setSolrHome(String solrHome) {
      this.solrHome = solrHome;
      return this;
    }

    public Builder setInstallDir(String installDir) {
      this.installDir = installDir;
      return this;
    }

    public Builder setLogsDir(String logsDir) {
      this.logsDir = logsDir;
      return this;
    }

    public Builder setJettyHome(String jettyHome) {
      this.jettyHome = jettyHome;
      return this;
    }

    public Builder setMinThreads(int minThreads) {
      this.minThreads = minThreads;
      return this;
    }

    public Builder setMaxThreads(int maxThreads) {
      this.maxThreads = maxThreads;
      return this;
    }

    public Builder setThreadIdleTimeout(int threadIdleTimeout) {
      this.threadIdleTimeout = threadIdleTimeout;
      return this;
    }

    public Builder setThreadStopTimeout(int threadStopTimeout) {
      this.threadStopTimeout = threadStopTimeout;
      return this;
    }

    public Builder setSecurePort(int securePort) {
      this.securePort = securePort;
      return this;
    }

    public Builder setOutputBufferSize(int outputBufferSize) {
      this.outputBufferSize = outputBufferSize;
      return this;
    }

    public Builder setOutputAggregationSize(int outputAggregationSize) {
      this.outputAggregationSize = outputAggregationSize;
      return this;
    }

    public Builder setRequestHeaderSize(int requestHeaderSize) {
      this.requestHeaderSize = requestHeaderSize;
      return this;
    }

    public Builder setResponseHeaderSize(int responseHeaderSize) {
      this.responseHeaderSize = responseHeaderSize;
      return this;
    }

    public Builder setSendServerVersion(boolean sendServerVersion) {
      this.sendServerVersion = sendServerVersion;
      return this;
    }

    public Builder setSendDateHeader(boolean sendDateHeader) {
      this.sendDateHeader = sendDateHeader;
      return this;
    }

    public Builder setHeaderCacheSize(int headerCacheSize) {
      this.headerCacheSize = headerCacheSize;
      return this;
    }

    public Builder setDelayDispatchUntilContent(boolean delayDispatchUntilContent) {
      this.delayDispatchUntilContent = delayDispatchUntilContent;
      return this;
    }

    public Builder setHttpIdleTimeout(int httpIdleTimeout) {
      this.httpIdleTimeout = httpIdleTimeout;
      return this;
    }

    public Builder setHttpsEnabled(boolean httpsEnabled) {
      this.httpsEnabled = httpsEnabled;
      return this;
    }

    public Builder setHttpsPort(int httpsPort) {
      this.httpsPort = httpsPort;
      return this;
    }

    public Builder setKeyStorePath(String keyStorePath) {
      this.keyStorePath = keyStorePath;
      return this;
    }

    public Builder setKeyStorePassword(String keyStorePassword) {
      this.keyStorePassword = keyStorePassword;
      return this;
    }

    public Builder setTrustStorePath(String trustStorePath) {
      this.trustStorePath = trustStorePath;
      return this;
    }

    public Builder setTrustStorePassword(String trustStorePassword) {
      this.trustStorePassword = trustStorePassword;
      return this;
    }

    public Builder setNeedClientAuth(boolean needClientAuth) {
      this.needClientAuth = needClientAuth;
      return this;
    }

    public Builder setWantClientAuth(boolean wantClientAuth) {
      this.wantClientAuth = wantClientAuth;
      return this;
    }

    public Builder setKeyStoreType(String keyStoreType) {
      this.keyStoreType = keyStoreType;
      return this;
    }

    public Builder setTrustStoreType(String trustStoreType) {
      this.trustStoreType = trustStoreType;
      return this;
    }

    public Builder setSniRequired(boolean sniRequired) {
      this.sniRequired = sniRequired;
      return this;
    }

    public Builder setSniHostCheck(boolean sniHostCheck) {
      this.sniHostCheck = sniHostCheck;
      return this;
    }

    public Builder setStsMaxAge(int stsMaxAge) {
      this.stsMaxAge = stsMaxAge;
      return this;
    }

    public Builder setStsIncludeSubdomains(boolean stsIncludeSubdomains) {
      this.stsIncludeSubdomains = stsIncludeSubdomains;
      return this;
    }

    public Builder setSslProvider(String sslProvider) {
      this.sslProvider = sslProvider;
      return this;
    }

    public Builder setGzipEnabled(boolean gzipEnabled) {
      this.gzipEnabled = gzipEnabled;
      return this;
    }

    public Builder setGzipMinSize(int gzipMinSize) {
      this.gzipMinSize = gzipMinSize;
      return this;
    }

    public Builder setGzipIncludedMethods(String gzipIncludedMethods) {
      this.gzipIncludedMethods = gzipIncludedMethods;
      return this;
    }

    public Builder setGzipDeflaterPoolCapacity(int gzipDeflaterPoolCapacity) {
      this.gzipDeflaterPoolCapacity = gzipDeflaterPoolCapacity;
      return this;
    }

    public Builder setGzipCompressionLevel(int gzipCompressionLevel) {
      this.gzipCompressionLevel = gzipCompressionLevel;
      return this;
    }

    public Builder setIpAccessIncludes(String ipAccessIncludes) {
      this.ipAccessIncludes = ipAccessIncludes;
      return this;
    }

    public Builder setIpAccessExcludes(String ipAccessExcludes) {
      this.ipAccessExcludes = ipAccessExcludes;
      return this;
    }

    public Builder setStopPort(int stopPort) {
      this.stopPort = stopPort;
      return this;
    }

    public Builder setStopKey(String stopKey) {
      this.stopKey = stopKey;
      return this;
    }

    public Builder setStopTimeout(int stopTimeout) {
      this.stopTimeout = stopTimeout;
      return this;
    }

    public Builder setRequestLoggingEnabled(boolean requestLoggingEnabled) {
      this.requestLoggingEnabled = requestLoggingEnabled;
      return this;
    }

    public ServerConfiguration build() {
      return new ServerConfiguration(this);
    }
  }
}
