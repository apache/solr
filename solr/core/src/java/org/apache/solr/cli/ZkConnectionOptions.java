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

import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import picocli.CommandLine;

/**
 * Picocli mixin providing common ZooKeeper connection options shared across ZK sub-commands.
 *
 * <p>Use {@code @CommandLine.Mixin ZkConnectionOptions zkOpts} in a command class to inherit these
 * options. Call {@link #resolveZkHost()} to obtain a resolved ZooKeeper connection string, applying
 * the same fallback logic as the commons-cli path.
 */
public class ZkConnectionOptions {

  @CommandLine.Option(
      names = {"-z", "--zk-host"},
      description =
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + '.')
  public String zkHost;

  @CommandLine.Option(
      names = {"-s", "--solr-connection"},
      description =
          "Zookeeper or HTTP(s) connection string; unnecessary if SOLR_CONNECTION is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + '.')
  public String solrConnection;

  @CommandLine.Option(
      names = {"--solr-url"},
      description =
          "Base Solr URL, which can be used to determine the zk-host if --zk-host is not known")
  public String solrUrl;

  @CommandLine.Option(
      names = {"-u", "--credentials"},
      description =
          "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
  public String credentials;

  /**
   * Resolves the ZooKeeper connection string using the following precedence:
   *
   * <ol>
   *   <li>{@code --solr-connection} option value; used directly if it is a ZooKeeper connection
   *       string, otherwise treated as a Solr URL to query for the ZooKeeper host
   *   <li>Explicit {@code --zk-host} option value
   *   <li>ZooKeeper host derived by querying the Solr instance at {@code --solr-url}
   *   <li>ZooKeeper host derived by querying the default Solr URL ({@code http://localhost:8983}),
   *       with a warning printed to stderr
   * </ol>
   *
   * @return resolved ZooKeeper connection string, never null
   * @throws IllegalStateException if the Solr instance is not running in SolrCloud mode
   * @throws Exception if the Solr instance cannot be reached
   */
  public String resolveZkHost() throws Exception {
    if (solrConnection != null) {
      var connection = CloudSolrClient.CloudSolrClientConnection.parse(solrConnection);
      if (connection.isZookeeper()) {
        return solrConnection;
      }
      // HTTP form: the connection string names a Solr URL, so ask that Solr for its ZooKeeper
      return zkHostFromSolrUrl(CLIUtils.normalizeSolrUrl(connection.quorumItems().get(0)));
    }

    if (zkHost != null) {
      return zkHost;
    }

    String resolvedSolrUrl = solrUrl;
    if (resolvedSolrUrl == null) {
      resolvedSolrUrl = CLIUtils.getDefaultSolrUrl();
      CLIO.err(
          "Neither --solr-connection, --zk-host or --solr-url parameters, nor SOLR_CONNECTION, ZK_HOST env var provided, so assuming solr url is "
              + resolvedSolrUrl
              + ".");
    }
    return zkHostFromSolrUrl(resolvedSolrUrl);
  }

  private String zkHostFromSolrUrl(String resolvedSolrUrl) throws Exception {
    try (SolrClient solrClient = CLIUtils.getSolrClient(resolvedSolrUrl, credentials)) {
      Map<String, Object> status = StatusTool.reportStatus(solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud != null) {
        String zookeeper = (String) cloud.get("ZooKeeper");
        if (zookeeper != null && zookeeper.endsWith("(embedded)")) {
          zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
        }
        if (zookeeper != null) {
          return zookeeper;
        }
      }
    }

    throw new IllegalStateException(
        "Solr at "
            + resolvedSolrUrl
            + " is not running in SolrCloud mode. Cannot use zk commands.");
  }
}
