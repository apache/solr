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
import java.net.SocketException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.SolrException;
import static org.apache.solr.common.SolrException.ErrorCode.FORBIDDEN;
import static org.apache.solr.common.SolrException.ErrorCode.UNAUTHORIZED;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CommonParams;
import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.SYSTEM_INFO_PATH;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.NamedList;

/**
 * Utility class that holds various helper methods for the CLI.
 *
 * @since 10.0
 */
public final class CLIUtils {

  private CLIUtils() {}

  public static String RED = "\u001B[31m";

  public static String GREEN = "\u001B[32m";

  public static String YELLOW = "\u001B[33m";

  private static final long MAX_WAIT_FOR_CORE_LOAD_NANOS =
      TimeUnit.NANOSECONDS.convert(1, TimeUnit.MINUTES);

  public static String getDefaultSolrUrl() {
    // note that ENV_VAR syntax (and the env vars too) are mapped to env.var sys props
    String scheme = EnvUtils.getProperty("solr.url.scheme", "http");
    String host = EnvUtils.getProperty("solr.tool.host", "localhost");
    String port = EnvUtils.getProperty("jetty.port", "8983"); // from SOLR_PORT env
    return String.format(Locale.ROOT, "%s://%s:%s", scheme.toLowerCase(Locale.ROOT), host, port);
  }

  /**
   * Determine if a request to Solr failed due to a communication error, which is generally
   * retry-able.
   */
  public static boolean checkCommunicationError(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof SolrServerException || rootCause instanceof SocketException);
  }

  public static void checkCodeForAuthError(int code) {
    if (code == UNAUTHORIZED.code || code == FORBIDDEN.code) {
      throw new SolrException(
          SolrException.ErrorCode.getErrorCode(code),
          "Solr requires authentication for request. Please supply valid credentials. HTTP code="
              + code);
    }
  }

  public static boolean exceptionIsAuthRelated(Exception exc) {
    return (exc instanceof SolrException
        && Arrays.asList(UNAUTHORIZED.code, FORBIDDEN.code).contains(((SolrException) exc).code()));
  }

  public static SolrClient getSolrClient(String solrUrl, String credentials, boolean barePath) {
    // today we require all urls to end in /solr, however in the future we will need to support the
    // /api url end point instead.   Eventually we want to have this method always
    // return a bare url, and then individual calls decide if they are /solr or /api
    // The /solr/ check is because sometimes a full url is passed in, like
    // http://localhost:8983/solr/films_shard1_replica_n1/.
    if (!barePath && !solrUrl.endsWith("/solr") && !solrUrl.contains("/solr/")) {
      solrUrl = solrUrl + "/solr";
    }
    Http2SolrClient.Builder builder =
        new Http2SolrClient.Builder(solrUrl)
            .withMaxConnectionsPerHost(32)
            .withKeyStoreReloadInterval(-1, TimeUnit.SECONDS)
            .withOptionalBasicAuthCredentials(credentials);

    return builder.build();
  }

  /**
   * Helper method for all the places where we assume a /solr on the url.
   *
   * @param solrUrl The solr url that you want the client for
   * @param credentials The username:password for basic auth.
   * @return The SolrClient
   */
  public static SolrClient getSolrClient(String solrUrl, String credentials) {
    return getSolrClient(solrUrl, credentials, false);
  }

  public static SolrClient getSolrClient(CommandLine cli, boolean barePath) throws Exception {
    String solrUrl = normalizeSolrUrl(cli);
    // TODO Replace hard-coded string with Option object
    String credentials = cli.getOptionValue("credentials");
    return getSolrClient(solrUrl, credentials, barePath);
  }

  public static SolrClient getSolrClient(CommandLine cli) throws Exception {
    String solrUrl = normalizeSolrUrl(cli);
    // TODO Replace hard-coded string with Option object
    String credentials = cli.getOptionValue("credentials");
    return getSolrClient(solrUrl, credentials, false);
  }

  /**
   * Strips off the end of solrUrl any /solr when a legacy solrUrl like http://localhost:8983/solr
   * is used, and warns those users. In the future we'll have urls ending with /api as well.
   *
   * @param solrUrl The user supplied url to Solr.
   * @return the solrUrl in the format that Solr expects to see internally.
   */
  public static String normalizeSolrUrl(String solrUrl) {
    return normalizeSolrUrl(solrUrl, true);
  }

  /**
   * Strips off the end of solrUrl any /solr when a legacy solrUrl like http://localhost:8983/solr
   * is used, and optionally logs a warning. In the future we'll have urls ending with /api as well.
   *
   * @param solrUrl The user supplied url to Solr.
   * @param logUrlFormatWarning If a warning message should be logged about the url format
   * @return the solrUrl in the format that Solr expects to see internally.
   */
  public static String normalizeSolrUrl(String solrUrl, boolean logUrlFormatWarning) {
    if (solrUrl != null) {
      URI uri = URI.create(solrUrl);
      String urlPath = uri.getRawPath();
      if (urlPath != null && urlPath.contains("/solr")) {
        String newSolrUrl =
            uri.resolve(urlPath.substring(0, urlPath.lastIndexOf("/solr") + 1)).toString();
        if (logUrlFormatWarning) {
          CLIO.err(
              "WARNING: URLs provided to this tool needn't include Solr's context-root (e.g. \"/solr\"). Such URLs are deprecated and support for them will be removed in a future release. Correcting from ["
                  + solrUrl
                  + "] to ["
                  + newSolrUrl
                  + "].");
        }
        solrUrl = newSolrUrl;
      }
      if (solrUrl.endsWith("/")) {
        solrUrl = solrUrl.substring(0, solrUrl.length() - 1);
      }
    }
    return solrUrl;
  }

  /**
   * Get the base URL of a live Solr instance from either the --solr-url command-line option or from
   * ZooKeeper.
   */
  public static String normalizeSolrUrl(CommandLine cli) throws Exception {
    String solrUrl =
        cli.hasOption("solr-url") ? cli.getOptionValue("solr-url") : cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String zkHost =
          cli.hasOption("zk-host") ? cli.getOptionValue("zk-host") : cli.getOptionValue("zkHost");
      if (zkHost == null) {
        solrUrl = getDefaultSolrUrl();
        CLIO.err(
            "Neither --zk-host or --solr-url parameters provided so assuming solr url is "
                + solrUrl
                + ".");
      } else {
        try (CloudSolrClient cloudSolrClient = getCloudHttp2SolrClient(zkHost)) {
          cloudSolrClient.connect();
          Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
          if (liveNodes.isEmpty())
            throw new IllegalStateException(
                "No live nodes found! Cannot determine 'solrUrl' from ZooKeeper: " + zkHost);

          String firstLiveNode = liveNodes.iterator().next();
          solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
          solrUrl = normalizeSolrUrl(solrUrl, false);
        }
      }
    }
    solrUrl = normalizeSolrUrl(solrUrl);
    return solrUrl;
  }

  /**
   * Get the ZooKeeper connection string from either the zk-host command-line option or by looking
   * it up from a running Solr instance based on the solr-url option.
   */
  public static String getZkHost(CommandLine cli) throws Exception {

    String zkHost =
        cli.hasOption("zk-host") ? cli.getOptionValue("zk-host") : cli.getOptionValue("zkHost");
    if (zkHost != null && !zkHost.isBlank()) {
      return zkHost;
    }

    try (SolrClient solrClient = getSolrClient(cli)) {
      // hit Solr to get system info
      NamedList<Object> systemInfo =
          solrClient.request(
              new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH));

      // convert raw JSON into user-friendly output
      StatusTool statusTool = new StatusTool();
      Map<String, Object> status = statusTool.reportStatus(systemInfo, solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud != null) {
        String zookeeper = (String) cloud.get("ZooKeeper");
        if (zookeeper.endsWith("(embedded)")) {
          zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
        }
        zkHost = zookeeper;
      }
    }

    return zkHost;
  }

  public static SolrZkClient getSolrZkClient(CommandLine cli) throws Exception {
    return getSolrZkClient(cli, getZkHost(cli));
  }

  public static SolrZkClient getSolrZkClient(CommandLine cli, String zkHost) throws Exception {
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at "
              + cli.getOptionValue("solrUrl")
              + " is running in standalone server mode, this command can only be used when running in SolrCloud mode.\n");
    }
    return new SolrZkClient.Builder()
        .withUrl(zkHost)
        .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
        .build();
  }

  public static CloudHttp2SolrClient getCloudHttp2SolrClient(String zkHost) {
    return getCloudHttp2SolrClient(zkHost, null);
  }

  public static CloudHttp2SolrClient getCloudHttp2SolrClient(
      String zkHost, Http2SolrClient.Builder builder) {
    return new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
        .withInternalClientBuilder(builder)
        .build();
  }

  public static boolean safeCheckCollectionExists(
      String solrUrl, String collection, String credentials) {
    boolean exists = false;
    try (var solrClient = CLIUtils.getSolrClient(solrUrl, credentials)) {
      NamedList<Object> existsCheckResult = solrClient.request(new CollectionAdminRequest.List());
      @SuppressWarnings("unchecked")
      List<String> collections = (List<String>) existsCheckResult.get("collections");
      exists = collections != null && collections.contains(collection);
    } catch (Exception exc) {
      // just ignore it since we're only interested in a positive result here
    }
    return exists;
  }

  @SuppressWarnings("unchecked")
  public static boolean safeCheckCoreExists(String solrUrl, String coreName, String credentials) {
    boolean exists = false;
    try (var solrClient = CLIUtils.getSolrClient(solrUrl, credentials)) {
      boolean wait = false;
      final long startWaitAt = System.nanoTime();
      do {
        if (wait) {
          final int clamPeriodForStatusPollMs = 1000;
          Thread.sleep(clamPeriodForStatusPollMs);
        }
        NamedList<Object> existsCheckResult =
            CoreAdminRequest.getStatus(coreName, solrClient).getResponse();
        NamedList<Object> status = (NamedList<Object>) existsCheckResult.get("status");
        NamedList<Object> coreStatus = (NamedList<Object>) status.get(coreName);
        Map<String, Object> failureStatus =
            (Map<String, Object>) existsCheckResult.get("initFailures");
        String errorMsg = (String) failureStatus.get(coreName);
        final boolean hasName = coreStatus != null && coreStatus.asMap().containsKey(NAME);
        exists = hasName || errorMsg != null;
        wait = hasName && errorMsg == null && "true".equals(coreStatus.get("isLoading"));
      } while (wait && System.nanoTime() - startWaitAt < MAX_WAIT_FOR_CORE_LOAD_NANOS);
    } catch (Exception exc) {
      // just ignore it since we're only interested in a positive result here
    }
    return exists;
  }

  public static boolean isCloudMode(SolrClient solrClient) throws SolrServerException, IOException {
    NamedList<Object> systemInfo =
        solrClient.request(new GenericSolrRequest(SolrRequest.METHOD.GET, SYSTEM_INFO_PATH));
    return "solrcloud".equals(systemInfo.get("mode"));
  }

  public static Path getConfigSetsDir(Path solrInstallDir) {
    Path configSetsPath = Paths.get("server/solr/configsets/");
    return solrInstallDir.resolve(configSetsPath);
  }
}
