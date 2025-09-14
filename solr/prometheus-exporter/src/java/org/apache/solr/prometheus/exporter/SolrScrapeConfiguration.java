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

package org.apache.solr.prometheus.exporter;

import java.nio.file.Path;
import java.util.Optional;

public class SolrScrapeConfiguration {

  public enum ConnectionType {
    CLOUD,
    STANDALONE
  }

  public static class SslConfiguration {

    public final Path keystorePath;
    public final String keystorePassword;
    public final Path trustStorePath;
    public final String trustStorePassword;

    public SslConfiguration(
        Path keystorePath,
        String keystorePassword,
        Path trustStorePath,
        String trustStorePassword) {
      this.keystorePath = keystorePath;
      this.keystorePassword = keystorePassword;
      this.trustStorePath = trustStorePath;
      this.trustStorePassword = trustStorePassword;
    }
  }

  private final ConnectionType type;
  private final String zookeeperConnectionString;
  private final String solrHost;
  private String basicAuthUser;
  private String basicAuthPwd;

  private boolean sslEnabled = false;
  private SslConfiguration sslConfiguration;

  private SolrScrapeConfiguration(
      ConnectionType type, String zookeeperConnectionString, String solrHost) {
    this.type = type;
    this.zookeeperConnectionString = zookeeperConnectionString;
    this.solrHost = solrHost;
  }

  public ConnectionType getType() {
    return type;
  }

  public Optional<String> getZookeeperConnectionString() {
    return Optional.ofNullable(zookeeperConnectionString);
  }

  public Optional<String> getSolrHost() {
    return Optional.ofNullable(solrHost);
  }

  public static SolrScrapeConfiguration solrCloud(String zookeeperConnectionString) {
    return new SolrScrapeConfiguration(ConnectionType.CLOUD, zookeeperConnectionString, null);
  }

  public static SolrScrapeConfiguration standalone(String solrHost) {
    return new SolrScrapeConfiguration(ConnectionType.STANDALONE, null, solrHost);
  }

  public SolrScrapeConfiguration withBasicAuthCredentials(String user, String password) {
    this.basicAuthUser = user;
    this.basicAuthPwd = password;
    return this;
  }

  public String getBasicAuthUser() {
    return basicAuthUser;
  }

  public String getBasicAuthPwd() {
    return basicAuthPwd;
  }

  public SolrScrapeConfiguration withSslConfiguration(
      Path keystorePath, String keystorePassword, Path trustStorePath, String trustStorePassword) {
    this.sslEnabled = true;
    this.sslConfiguration =
        new SslConfiguration(keystorePath, keystorePassword, trustStorePath, trustStorePassword);
    return this;
  }

  public boolean isSSLEnabled() {
    return this.sslEnabled;
  }

  public SslConfiguration getSslConfiguration() {
    return sslConfiguration;
  }

  @Override
  public String toString() {
    if (type == ConnectionType.CLOUD) {
      return "Solr Cloud ZK: " + zookeeperConnectionString;
    } else if (type == ConnectionType.STANDALONE) {
      return "Solr Node: " + solrHost;
    } else {
      return "None";
    }
  }
}
