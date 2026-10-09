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
package org.apache.solr.cli;

import java.io.IOException;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.util.EnvUtils;
import picocli.CommandLine;

/**
 * Picocli ArgGroup for mutually-exclusive Solr URL / ZooKeeper connection options.
 *
 * <p>Use as the type of an {@code @ArgGroup(exclusive = true, multiplicity = "0..1")} field to
 * ensure the user provides at most one of {@code --solr-connection}, {@code --solr-url} or {@code
 * --zk-host}.
 */
public class ConnectionOptions {
  @CommandLine.Option(
      names = {"-s", "--solr-connection"},
      description =
          "Zookeeper or HTTP(s) connection string; unnecessary if SOLR_CONNECTION is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + ".")
  public String solrConnection;

  @CommandLine.Option(
      names = {"--solr-url"},
      description =
          "Base Solr URL, which can be used to determine the zk-host if that's not known.")
  public String solrUrl;

  @CommandLine.Option(
      names = {"-z", "--zk-host"},
      description =
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + ".")
  public String zkHost;

  /**
   * The effective ZooKeeper connection string, taking {@code --solr-connection} into account, or
   * null if the user targeted Solr via a URL (or gave no target at all).
   */
  public String effectiveZkHost() throws IOException {
    if (solrConnection != null) {
      var connection = CloudSolrClient.CloudSolrClientConnection.parse(solrConnection);
      return connection.isZookeeper() ? solrConnection : null;
    }
    return zkHost;
  }

  /**
   * The effective Solr URL, taking {@code --solr-connection} into account, or null if the user
   * targeted ZooKeeper (or gave no target at all).
   */
  public String effectiveSolrUrl() throws IOException {
    if (solrConnection != null) {
      var connection = CloudSolrClient.CloudSolrClientConnection.parse(solrConnection);
      return connection.isZookeeper() ? null : connection.quorumItems().get(0);
    }
    return solrUrl;
  }

  public static String resolveSolrUrl(ConnectionOptions connectionOptions, String credentials)
      throws Exception {
    if (connectionOptions != null) {
      String solrUrl = connectionOptions.effectiveSolrUrl();
      if (solrUrl != null) {
        return CLIUtils.normalizeSolrUrl(solrUrl);
      }
      String zkHost = connectionOptions.effectiveZkHost();
      if (zkHost != null) {
        return CLIUtils.solrUrlFromConnection(
            CloudSolrClient.CloudSolrClientConnection.parse(zkHost), credentials);
      }
    }

    String solrConnectionProp = EnvUtils.getProperty("solr.connection");
    if (solrConnectionProp != null && !solrConnectionProp.isBlank()) {
      var connection = CloudSolrClient.CloudSolrClientConnection.parse(solrConnectionProp);
      if (connection.isZookeeper()) {
        return CLIUtils.solrUrlFromConnection(connection, credentials);
      }
      return CLIUtils.normalizeSolrUrl(connection.quorumItems().get(0));
    }

    String zkHostProp = EnvUtils.getProperty("zkHost");
    if (zkHostProp != null && !zkHostProp.isBlank()) {
      return CLIUtils.solrUrlFromConnection(
          CloudSolrClient.CloudSolrClientConnection.parse(zkHostProp), credentials);
    }

    String defaultUrl = CLIUtils.getDefaultSolrUrl();
    CLIO.err(
        "Neither --solr-connection, --zk-host or --solr-url parameters, nor SOLR_CONNECTION, ZK_HOST env var provided, so assuming solr url is "
            + defaultUrl
            + ".");
    return defaultUrl;
  }

  public static String resolveZkHost(
      ConnectionOptions connectionOptions, String solrUrl, String credentials) throws Exception {
    boolean resolveFromSolrUrl = false;
    if (connectionOptions != null) {
      String zkHost = connectionOptions.effectiveZkHost();
      if (zkHost != null) {
        return zkHost;
      }
      resolveFromSolrUrl = connectionOptions.effectiveSolrUrl() != null;
    }

    if (!resolveFromSolrUrl) {
      String solrConnectionProp = EnvUtils.getProperty("solr.connection");
      if (solrConnectionProp != null && !solrConnectionProp.isBlank()) {
        var connection = CloudSolrClient.CloudSolrClientConnection.parse(solrConnectionProp);
        if (connection.isZookeeper()) {
          return solrConnectionProp;
        }
      }

      String zkHostProp = EnvUtils.getProperty("zkHost");
      if (zkHostProp != null && !zkHostProp.isBlank()) {
        return zkHostProp;
      }
    }

    try (SolrClient solrClient = CLIUtils.getSolrClient(solrUrl, credentials)) {
      Map<String, Object> status = StatusTool.reportStatus(solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud != null) {
        String zookeeper = cloud.get("ZooKeeper").toString();
        if (zookeeper != null && zookeeper.endsWith("(embedded)")) {
          zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
        }
        return zookeeper;
      }
    }
    return null;
  }
}
