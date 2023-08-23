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

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.common.params.CommonParams.SYSTEM_INFO_PATH;

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.NamedList;
import org.noggit.CharArr;
import org.noggit.JSONWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_SOLRURL,
        Option.builder(NAME)
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the core / collection to delete.")
            .build(),
        Option.builder("deleteConfig")
            .argName("true|false")
            .hasArg()
            .required(false)
            .desc(
                "Flag to indicate if the underlying configuration directory for a collection should also be deleted; default is true.")
            .build(),
        Option.builder("forceDeleteConfig")
            .required(false)
            .desc(
                "Skip safety checks when deleting the configuration directory used by a collection.")
            .build(),
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.getDefaultSolrUrl());
    if (!solrUrl.endsWith("/")) solrUrl += "/";

    try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
      Map<String, Object> systemInfo =
          solrClient
              .request(new GenericSolrRequest(SolrRequest.METHOD.GET, SYSTEM_INFO_PATH))
              .asMap();
      if ("solrcloud".equals(systemInfo.get("mode"))) {
        deleteCollection(cli);
      } else {
        deleteCore(cli, solrClient);
      }
    }
  }

  protected void deleteCollection(CommandLine cli) throws Exception {
    String zkHost = SolrCLI.getZkHost(cli);
    try (CloudSolrClient cloudSolrClient =
        new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
            .withInternalClientBuilder(
                new Http2SolrClient.Builder()
                    .withIdleTimeout(30, TimeUnit.SECONDS)
                    .withConnectionTimeout(15, TimeUnit.SECONDS))
            .build()) {
      echoIfVerbose("Connecting to ZooKeeper at " + zkHost, cli);
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
    String collectionName = cli.getOptionValue(NAME);
    if (!zkStateReader.getClusterState().hasCollection(collectionName)) {
      throw new IllegalArgumentException("Collection " + collectionName + " not found!");
    }

    String configName =
        zkStateReader.getClusterState().getCollection(collectionName).getConfigName();
    boolean deleteConfig = "true".equals(cli.getOptionValue("deleteConfig", "true"));
    if (deleteConfig && configName != null) {
      if (cli.hasOption("forceDeleteConfig")) {
        log.warn(
            "Skipping safety checks, configuration directory {} will be deleted with impunity.",
            configName);
      } else {
        // need to scan all Collections to see if any are using the config
        Set<String> collections = zkStateReader.getClusterState().getCollectionsMap().keySet();

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
              "; configuration will not be deleted from ZooKeeper. You can pass the -forceDeleteConfig flag to force delete.");
        }
      }
    }

    echoIfVerbose(
        "\nDeleting collection '" + collectionName + "' using CollectionAdminRequest", cli);

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
                + "\nYou'll need to manually delete this znode using the zkcli script.");
      }
    }

    if (response != null) {
      // pretty-print the response to stdout
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(response.asMap());
      echo(arr.toString());
      echo("\n");
    }

    echo("Deleted collection '" + collectionName + "' using CollectionAdminRequest");
  }

  protected void deleteCore(CommandLine cli, SolrClient solrClient) throws Exception {
    String coreName = cli.getOptionValue(NAME);

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
      echoIfVerbose((String) response.get("response"), cli);
      echoIfVerbose("\n", cli);
    }
  }
}
