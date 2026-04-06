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

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.common.cloud.ZkStateReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports delete command in the bin/solr script. */
@picocli.CommandLine.Command(
    name = "delete",
    mixinStandardHelpOptions = true,
    description =
        "Deletes a collection or core depending on whether Solr is running in SolrCloud or standalone mode.")
public class DeleteTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the core / collection to delete.")
          .get();

  private static final Option DELETE_CONFIG_OPTION =
      Option.builder()
          .longOpt("delete-config")
          .desc(
              "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is false.")
          .get();

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .longOpt("force")
          .desc(
              "Skip safety checks when deleting the configuration directory used by a collection.")
          .get();

  /** Options bean shared between commons-cli and picocli paths. */
  record DeleteParams(String name, String credentials, boolean deleteConfig, boolean force) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  static class ConnectionOptions {
    @picocli.CommandLine.Option(
        names = {"-s", "--solr-url"},
        description =
            "Base Solr URL, which can be used to determine the zk-host if that's not known.")
    String solrUrl;

    @picocli.CommandLine.Option(
        names = {"-z", "--zk-host"},
        description =
            "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
                + CommonCLIOptions.DefaultValues.ZK_HOST
                + ".")
    String zkHost;
  }

  @picocli.CommandLine.Option(
      names = {"-u", "--credentials"},
      description =
          "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
  private String credentials;

  @picocli.CommandLine.Option(
      names = {"-c", "--name"},
      required = true,
      description = "Name of the core / collection to delete.")
  private String name;

  @picocli.CommandLine.Option(
      names = {"--delete-config"},
      description =
          "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is false.")
  private boolean deleteConfig;

  @picocli.CommandLine.Option(
      names = {"-f", "--force"},
      description =
          "Skip safety checks when deleting the configuration directory used by a collection.")
  private boolean force;

  public DeleteTool() {
    this(new DefaultToolRuntime());
  }

  public DeleteTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "delete";
  }

  @Override
  public String getHeader() {
    return """
        Deletes a collection or core depending on whether Solr is running in SolrCloud or standalone mode. \
        Deleting a collection does not delete it's configuration unless you pass in the --delete-config flag.

        List of options:""";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(DELETE_CONFIG_OPTION)
        .addOption(FORCE_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    try (var solrClient = CLIUtils.getSolrClient(cli)) {
      DeleteParams params =
          new DeleteParams(
              cli.getOptionValue(COLLECTION_NAME_OPTION),
              cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION),
              cli.hasOption(DELETE_CONFIG_OPTION),
              cli.hasOption(FORCE_OPTION));
      if (CLIUtils.isCloudMode(solrClient)) {
        deleteCollection(CLIUtils.getZkHost(cli), params);
      } else {
        deleteCore(params, solrClient);
      }
    }
  }

  private void deleteCollection(String zkHost, DeleteParams params) throws Exception {
    var builder =
        new HttpJettySolrClient.Builder()
            .withIdleTimeout(30, TimeUnit.SECONDS)
            .withConnectionTimeout(15, TimeUnit.SECONDS)
            .withKeyStoreReloadInterval(-1, TimeUnit.SECONDS)
            .withOptionalBasicAuthCredentials(params.credentials);
    echoIfVerbose("Connecting to ZooKeeper at " + zkHost);
    try (CloudSolrClient cloudSolrClient = CLIUtils.getCloudSolrClient(zkHost, builder)) {
      deleteCollection(cloudSolrClient, params);
    }
  }

  private void deleteCollection(CloudSolrClient cloudSolrClient, DeleteParams params)
      throws Exception {
    Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new IllegalStateException(
          "No live nodes found! Cannot delete a collection until "
              + "there is at least 1 live node in the cluster.");

    ZkStateReader zkStateReader = ZkStateReader.from(cloudSolrClient);
    if (!zkStateReader.getClusterState().hasCollection(params.name)) {
      throw new IllegalArgumentException("Collection " + params.name + " not found!");
    }

    String configName = zkStateReader.getClusterState().getCollection(params.name).getConfigName();
    boolean effectiveDeleteConfig = params.deleteConfig;

    if (effectiveDeleteConfig && configName != null) {
      if (params.force) {
        log.warn(
            "Skipping safety checks, configuration directory {} will be deleted with impunity.",
            configName);
      } else {
        // need to scan all Collections to see if any are using the config
        Collection<String> collections = zkStateReader.getClusterState().getCollectionNames();

        // give a little note to the user if there are many collections in case it takes a while
        if (collections.size() > 50)
          if (log.isInfoEnabled()) {
            log.info(
                "Scanning {} to ensure no other collections are using config {}",
                collections.size(),
                configName);
          }

        Optional<String> inUse =
            collections.stream()
                .filter(n -> !n.equals(params.name)) // ignore this collection
                .filter(
                    n ->
                        configName.equals(
                            zkStateReader.getClusterState().getCollection(n).getConfigName()))
                .findFirst();
        if (inUse.isPresent()) {
          effectiveDeleteConfig = false;
          log.warn(
              "Configuration directory {} is also being used by {}{}",
              configName,
              inUse.get(),
              "; configuration will not be deleted from ZooKeeper. You can pass the --force-delete-config flag to force delete.");
        }
      }
    }

    echoIfVerbose("\nDeleting collection '" + params.name + "' using V2 Collections API");

    try {
      var req = new CollectionsApi.DeleteCollection(params.name);
      var response = req.process(cloudSolrClient);
      echoIfVerbose(response);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to delete collection '" + params.name + "' due to: " + sse.getMessage());
    }

    if (effectiveDeleteConfig) {
      String configZnode = "/configs/" + configName;
      try {
        zkStateReader.getZkClient().clean(configZnode);
      } catch (Exception exc) {
        echo(
            "\nWARNING: Failed to delete configuration directory "
                + configZnode
                + " in ZooKeeper due to: "
                + exc.getMessage()
                + "\nYou'll need to manually delete this znode using the bin/solr zk rm command.");
      }
    }

    echo(String.format(Locale.ROOT, "\nDeleted collection '%s'", params.name));
  }

  private void deleteCore(DeleteParams params, SolrClient solrClient) throws Exception {
    echo("\nDeleting core '" + params.name + "' using V2 Cores API\n");

    try {
      var req = new CoresApi.UnloadCore(params.name);
      req.setDeleteIndex(true);
      req.setDeleteDataDir(true);
      req.setDeleteInstanceDir(true);
      var response = req.process(solrClient);
      echoIfVerbose(response);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to delete core '" + params.name + "' due to: " + sse.getMessage());
    }
  }

  @Override
  public int callTool() throws Exception {
    String zkHostArg = (connectionOptions != null) ? connectionOptions.zkHost : null;
    String solrUrlArg = (connectionOptions != null) ? connectionOptions.solrUrl : null;
    DeleteParams params = new DeleteParams(name, credentials, deleteConfig, force);

    if (zkHostArg != null) {
      deleteCollection(zkHostArg, params);
    } else {
      String resolvedSolrUrl;
      if (solrUrlArg != null) {
        resolvedSolrUrl = CLIUtils.normalizeSolrUrl(solrUrlArg);
      } else {
        resolvedSolrUrl = CLIUtils.getDefaultSolrUrl();
        CLIO.err(
            "Neither --zk-host or --solr-url parameters, nor ZK_HOST env var provided, so assuming solr url is "
                + resolvedSolrUrl
                + ".");
      }
      try (var solrClient = CLIUtils.getSolrClient(resolvedSolrUrl, credentials)) {
        Map<String, Object> status = StatusTool.reportStatus(solrClient);
        @SuppressWarnings("unchecked")
        Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
        if (cloud != null) {
          String zookeeper = (String) cloud.get("ZooKeeper");
          if (zookeeper != null && zookeeper.endsWith("(embedded)")) {
            zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
          }
          deleteCollection(zookeeper, params);
        } else {
          deleteCore(params, solrClient);
        }
      }
    }
    return 0;
  }
}
