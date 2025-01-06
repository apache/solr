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

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports delete command in the bin/solr script. */
public class DeleteTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of the core / collection to delete.")
          .build();

  private static final Option DELETE_CONFIG_OPTION =
      Option.builder()
          .longOpt("delete-config")
          .desc(
              "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true.")
          .build();

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .longOpt("force")
          .desc(
              "Skip safety checks when deleting the configuration directory used by a collection.")
          .build();

  public DeleteTool() {
    this(CLIO.getOutStream());
  }

  public DeleteTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "delete";
  }

  @Override
  public String getHeader() {
    return "Deletes a collection or core depending on whether Solr is running in SolrCloud or standalone mode. "
        + "Deleting a collection does not delete it's configuration unless you pass in the --delete-config flag.\n"
        + "\n"
        + "List of options:";
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
      if (CLIUtils.isCloudMode(solrClient)) {
        deleteCollection(cli);
      } else {
        deleteCore(cli, solrClient);
      }
    }
  }

  protected void deleteCollection(CommandLine cli) throws Exception {
    Http2SolrClient.Builder builder =
        new Http2SolrClient.Builder()
            .withIdleTimeout(30, TimeUnit.SECONDS)
            .withConnectionTimeout(15, TimeUnit.SECONDS)
            .withKeyStoreReloadInterval(-1, TimeUnit.SECONDS)
            .withOptionalBasicAuthCredentials(
                cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION));

    String zkHost = CLIUtils.getZkHost(cli);
    try (CloudSolrClient cloudSolrClient = CLIUtils.getCloudHttp2SolrClient(zkHost, builder)) {
      echoIfVerbose("Connecting to ZooKeeper at " + zkHost);
      cloudSolrClient.connect();
      deleteCollection(cloudSolrClient, cli);
    }
  }

  protected void deleteCollection(CloudSolrClient cloudSolrClient, CommandLine cli)
      throws Exception {
    Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new IllegalStateException(
          "No live nodes found! Cannot delete a collection until "
              + "there is at least 1 live node in the cluster.");

    ZkStateReader zkStateReader = ZkStateReader.from(cloudSolrClient);
    String collectionName = cli.getOptionValue(COLLECTION_NAME_OPTION);
    if (!zkStateReader.getClusterState().hasCollection(collectionName)) {
      throw new IllegalArgumentException("Collection " + collectionName + " not found!");
    }

    String configName =
        zkStateReader.getClusterState().getCollection(collectionName).getConfigName();
    boolean deleteConfig = cli.hasOption(DELETE_CONFIG_OPTION);

    if (deleteConfig && configName != null) {
      if (cli.hasOption(FORCE_OPTION)) {
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
                .filter(name -> !name.equals(collectionName)) // ignore this collection
                .filter(
                    name ->
                        configName.equals(
                            zkStateReader.getClusterState().getCollection(name).getConfigName()))
                .findFirst();
        if (inUse.isPresent()) {
          deleteConfig = false;
          log.warn(
              "Configuration directory {} is also being used by {}{}",
              configName,
              inUse.get(),
              "; configuration will not be deleted from ZooKeeper. You can pass the --force-delete-config flag to force delete.");
        }
      }
    }

    echoIfVerbose("\nDeleting collection '" + collectionName + "' using CollectionAdminRequest");

    NamedList<Object> response;
    try {
      var req = CollectionAdminRequest.deleteCollection(collectionName);
      req.setResponseParser(new JsonMapResponseParser());
      response = cloudSolrClient.request(req);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to delete collection '" + collectionName + "' due to: " + sse.getMessage());
    }

    if (deleteConfig) {
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

    if (isVerbose() && response != null) {
      // pretty-print the response to stdout
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(response.asMap(10));
      echo(arr.toString());
      echo("\n");
    }

    echo(String.format(Locale.ROOT, "\nDeleted collection '%s'", collectionName));
  }

  protected void deleteCore(CommandLine cli, SolrClient solrClient) throws Exception {
    String coreName = cli.getOptionValue(COLLECTION_NAME_OPTION);

    echo("\nDeleting core '" + coreName + "' using CoreAdminRequest\n");

    NamedList<Object> response;
    try {
      CoreAdminRequest.Unload unloadRequest = new CoreAdminRequest.Unload(true);
      unloadRequest.setDeleteIndex(true);
      unloadRequest.setDeleteDataDir(true);
      unloadRequest.setDeleteInstanceDir(true);
      unloadRequest.setCoreName(coreName);
      unloadRequest.setResponseParser(new NoOpResponseParser("json"));
      response = solrClient.request(unloadRequest);
    } catch (SolrServerException sse) {
      throw new Exception("Failed to delete core '" + coreName + "' due to: " + sse.getMessage());
    }

    if (response != null) {
      echoIfVerbose((String) response.get("response"));
      echoIfVerbose("\n");
    }
  }
}
