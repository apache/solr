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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import picocli.CommandLine;

/**
 * The {@code --solr-connection}, {@code --zk-host} and {@code --solr-url} options shared by every
 * picocli command that talks to Solr; add it to a command with {@code @CommandLine.Mixin}.
 *
 * <p>Options not given on the command line are filled from {@code SOLR_CONNECTION}, {@code ZK_HOST}
 * and {@code SOLR_URL} by {@link CliDefaultValueProvider}. The resolvers prefer an option passed
 * explicitly over those environment values, and reject more than one explicitly passed option, so
 * {@code ZK_HOST=... bin/solr create --solr-url ...} targets the URL while {@code --zk-host a
 * --solr-url b} is an error.
 */
public class ConnectionOptions {

  private static final String SOLR_CONNECTION = "--solr-connection";
  private static final String ZK_HOST = "--zk-host";
  private static final String SOLR_URL = "--solr-url";

  @CommandLine.Option(
      names = {"-s", SOLR_CONNECTION},
      description =
          "Zookeeper or HTTP(s) connection string; unnecessary if SOLR_CONNECTION is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + ".")
  public String solrConnection;

  @CommandLine.Option(
      names = {SOLR_URL},
      description =
          "Base Solr URL, which can be used to determine the zk-host if that's not known.")
  public String solrUrl;

  @CommandLine.Option(
      names = {"-z", ZK_HOST},
      description =
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + ".")
  public String zkHost;

  @CommandLine.Spec(CommandLine.Spec.Target.MIXEE)
  CommandLine.Model.CommandSpec mixee;

  private enum Kind {
    CONNECTION,
    ZK,
    URL
  }

  private record Target(Kind kind, String value) {}

  /** True if any of the three options was passed on the command line (environment excluded). */
  public boolean hasExplicitConnection() {
    return !explicitOptionNames().isEmpty();
  }

  /** Long names of the options passed on the command line, in canonical order. */
  List<String> explicitOptionNames() {
    CommandLine.ParseResult parseResult = parseResult();
    if (parseResult == null) {
      return List.of();
    }
    List<String> names = new ArrayList<>();
    for (String name : new String[] {SOLR_CONNECTION, ZK_HOST, SOLR_URL}) {
      if (parseResult.hasMatchedOption(name)) {
        names.add(name);
      }
    }
    return names;
  }

  /** The usage error for {@code given} options that may not be combined, e.g. "-s, -z or -p". */
  static String mutuallyExclusiveMessage(List<String> given, String allowed) {
    return "Options "
        + String.join(" and ", given)
        + " are mutually exclusive (specify only one of "
        + allowed
        + ")";
  }

  /**
   * Resolves the base Solr URL. ZooKeeper targets are asked for a live node; no target at all falls
   * back to {@link CLIUtils#getDefaultSolrUrl()} with a warning on stderr.
   */
  public String resolveSolrUrl(String credentials) throws Exception {
    return resolveSolrUrl(credentials, true);
  }

  /** As {@link #resolveSolrUrl(String)}, with the fallback warning optional. */
  public String resolveSolrUrl(String credentials, boolean warnOnDefault) throws Exception {
    Target target = target();
    if (target == null) {
      return defaultSolrUrl(warnOnDefault);
    }
    return switch (target.kind()) {
      case URL -> CLIUtils.normalizeSolrUrl(target.value());
      case CONNECTION ->
          CLIUtils.solrUrlFromConnection(
              CloudSolrClient.CloudSolrClientConnection.parse(target.value()), credentials);
      case ZK -> CLIUtils.solrUrlFromConnection(zkConnection(target.value()), credentials);
    };
  }

  /**
   * Resolves the ZooKeeper connection string. A ZooKeeper target is returned as is; an HTTP target
   * (or none) is asked for the ZooKeeper it uses.
   *
   * @throws IllegalStateException if the Solr instance asked is not running in SolrCloud mode
   */
  public String resolveZkHost(String credentials) throws Exception {
    Target target = target();
    String solrUrl;
    if (target == null) {
      solrUrl = defaultSolrUrl(true);
    } else {
      switch (target.kind()) {
        case ZK -> {
          zkConnection(target.value());
          return target.value();
        }
        case CONNECTION -> {
          var connection = CloudSolrClient.CloudSolrClientConnection.parse(target.value());
          if (connection.isZookeeper()) {
            return target.value();
          }
          solrUrl = CLIUtils.normalizeSolrUrl(connection.quorumItems().get(0));
        }
        case URL -> solrUrl = CLIUtils.normalizeSolrUrl(target.value());
        default -> throw new IllegalStateException("Unknown target kind " + target.kind());
      }
    }
    String zkHost = zkHostFromStatus(solrUrl, credentials);
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at " + solrUrl + " is not running in SolrCloud mode. Cannot use zk commands.");
    }
    return zkHost;
  }

  /**
   * The connection named by {@code --solr-connection} or {@code --zk-host} (option or environment),
   * without contacting Solr; null if only a URL or nothing was given.
   */
  public CloudSolrClient.CloudSolrClientConnection namedConnection() throws IOException {
    Target target = target();
    if (target == null) {
      return null;
    }
    return switch (target.kind()) {
      case CONNECTION -> CloudSolrClient.CloudSolrClientConnection.parse(target.value());
      case ZK -> zkConnection(target.value());
      case URL -> null;
    };
  }

  /**
   * Like {@link #namedConnection()}, but when only a URL (or nothing) was given the Solr instance
   * there is asked for its ZooKeeper. Returns null if that instance is not running in SolrCloud
   * mode.
   */
  public CloudSolrClient.CloudSolrClientConnection resolveSolrConnection(String credentials)
      throws Exception {
    var named = namedConnection();
    if (named != null) {
      return named;
    }
    String zkHost = zkHostFromStatus(resolveSolrUrl(credentials, false), credentials);
    return zkHost == null ? null : CloudSolrClient.CloudSolrClientConnection.parse(zkHost);
  }

  private Target target() {
    List<String> given = explicitOptionNames();
    if (given.size() > 1) {
      throw new CommandLine.ParameterException(
          mixee.commandLine(), mutuallyExclusiveMessage(given, "-s, -z or --solr-url"));
    }
    CommandLine.ParseResult parseResult = parseResult();
    if (parseResult != null) {
      if (parseResult.hasMatchedOption(SOLR_CONNECTION)) {
        return new Target(Kind.CONNECTION, solrConnection);
      }
      if (parseResult.hasMatchedOption(ZK_HOST)) {
        return new Target(Kind.ZK, zkHost);
      }
      if (parseResult.hasMatchedOption(SOLR_URL)) {
        return new Target(Kind.URL, solrUrl);
      }
    }
    if (notBlank(solrConnection)) {
      return new Target(Kind.CONNECTION, solrConnection);
    }
    if (notBlank(zkHost)) {
      return new Target(Kind.ZK, zkHost);
    }
    if (notBlank(solrUrl)) {
      return new Target(Kind.URL, solrUrl);
    }
    return null;
  }

  private CommandLine.ParseResult parseResult() {
    if (mixee == null || mixee.commandLine() == null) {
      return null;
    }
    return mixee.commandLine().getParseResult();
  }

  private static CloudSolrClient.CloudSolrClientConnection zkConnection(String zkHost)
      throws IOException {
    var connection = CloudSolrClient.CloudSolrClientConnection.parse(zkHost);
    if (!connection.isZookeeper()) {
      throw new IOException(
          String.format(
              Locale.ROOT, "Expected ZooKeeper connection string, but got: '%s'.", zkHost));
    }
    return connection;
  }

  private static String defaultSolrUrl(boolean warn) {
    String defaultUrl = CLIUtils.getDefaultSolrUrl();
    if (warn) {
      CLIO.err(
          "Neither --solr-connection, --zk-host or --solr-url parameters, nor SOLR_CONNECTION, ZK_HOST env var provided, so assuming solr url is "
              + defaultUrl
              + ".");
    }
    return defaultUrl;
  }

  /** The ZooKeeper the Solr at {@code solrUrl} reports, or null when it runs standalone. */
  private static String zkHostFromStatus(String solrUrl, String credentials) throws Exception {
    try (SolrClient solrClient = CLIUtils.getSolrClient(solrUrl, credentials)) {
      Map<String, Object> status = StatusTool.reportStatus(solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud == null) {
        return null;
      }
      String zookeeper = (String) cloud.get("ZooKeeper");
      if (zookeeper != null && zookeeper.endsWith("(embedded)")) {
        zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
      }
      return zookeeper;
    }
  }

  private static boolean notBlank(String value) {
    return value != null && !value.isBlank();
  }
}
