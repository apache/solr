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

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetService;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/** Supports create_collection command in the bin/solr script. */
public class CreateCollectionTool extends ToolBase {

  public static final List<Option> CREATE_COLLECTION_OPTIONS =
      List.of(
          SolrCLI.OPTION_ZKHOST,
          SolrCLI.OPTION_SOLRURL,
          Option.builder("c")
              .longOpt("name")
              .argName("NAME")
              .hasArg()
              .required(true)
              .desc("Name of collection to create.")
              .build(),
          Option.builder("s")
              .longOpt("shards")
              .argName("#")
              .hasArg()
              .required(false)
              .desc("Number of shards; default is 1.")
              .build(),
          Option.builder("rf")
              .longOpt("replicationFactor")
              .argName("#")
              .hasArg()
              .required(false)
              .desc(
                  "Number of copies of each document across the collection (replicas per shard); default is 1.")
              .build(),
          Option.builder("d")
              .longOpt("confdir")
              .argName("NAME")
              .hasArg()
              .required(false)
              .desc(
                  "Configuration directory to copy when creating the new collection; default is "
                      + SolrCLI.DEFAULT_CONFIG_SET
                      + '.')
              .build(),
          Option.builder("n")
              .longOpt("confname")
              .argName("NAME")
              .hasArg()
              .required(false)
              .desc("Configuration name; default is the collection name.")
              .build(),
          SolrCLI.OPTION_VERBOSE);

  public CreateCollectionTool() {
    this(CLIO.getOutStream());
  }

  public CreateCollectionTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "create_collection";
  }

  @Override
  public List<Option> getOptions() {
    return CREATE_COLLECTION_OPTIONS;
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at is running in standalone server mode, please use the create_core command instead;\n"
              + "create_collection can only be used when running in SolrCloud mode.\n");
    }

    try (CloudHttp2SolrClient cloudSolrClient =
        new CloudHttp2SolrClient.Builder(Collections.singletonList(zkHost), Optional.empty())
            .build()) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      cloudSolrClient.connect();
      runCloudTool(cloudSolrClient, cli);
    }
  }

  protected void runCloudTool(CloudSolrClient cloudSolrClient, CommandLine cli) throws Exception {

    String collectionName = cli.getOptionValue("name");
    final String solrInstallDir = System.getProperty("solr.install.dir");
    String confName = cli.getOptionValue("confname");
    String confDir = cli.getOptionValue("confdir", "_default");
    ensureConfDirExists(confDir, solrInstallDir);
    printDefaultConfigsetWarningIfNecessary(cli);

    Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new IllegalStateException(
          "No live nodes found! Cannot create a collection until "
              + "there is at least 1 live node in the cluster.");

    String solrUrl = cli.getOptionValue("solrUrl");
    if (solrUrl == null) {
      String firstLiveNode = liveNodes.iterator().next();
      solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
    }

    // build a URL to create the collection
    int numShards = Integer.parseInt(cli.getOptionValue("shards", String.valueOf(1)));
    int replicationFactor =
        Integer.parseInt(cli.getOptionValue("replicationFactor", String.valueOf(1)));

    final String configsetsDir = solrInstallDir + "/server/solr/configsets";

    boolean configExistsInZk =
        confName != null
            && !confName.trim().isEmpty()
            && ZkStateReader.from(cloudSolrClient)
                .getZkClient()
                .exists("/configs/" + confName, true);

    if (CollectionAdminParams.SYSTEM_COLL.equals(collectionName)) {
      // do nothing
    } else if (configExistsInZk) {
      echo("Re-using existing configuration directory " + confName);
    } else { // if (confdir != null && !confdir.trim().isEmpty()) {
      if (confName == null || confName.trim().isEmpty()) {
        confName = collectionName;
      }
      Path confPath = ConfigSetService.getConfigsetPath(confDir, configsetsDir);

      echoIfVerbose(
          "Uploading "
              + confPath.toAbsolutePath()
              + " for config "
              + confName
              + " to ZooKeeper at "
              + cloudSolrClient.getClusterStateProvider().getQuorumHosts(),
          cli);
      ZkMaintenanceUtils.uploadToZK(
          ZkStateReader.from(cloudSolrClient).getZkClient(),
          confPath,
          ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName,
          ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);
    }

    // since creating a collection is a heavy-weight operation, check for existence first
    if (SolrCLI.safeCheckCollectionExists(solrUrl, collectionName)) {
      throw new IllegalStateException(
          "\nCollection '"
              + collectionName
              + "' already exists!\nChecked collection existence using CollectionAdminRequest");
    }

    // doesn't seem to exist ... try to create
    echoIfVerbose(
        "\nCreating new collection '" + collectionName + "' using CollectionAdminRequest", cli);

    NamedList<Object> response;
    try {
      response =
          cloudSolrClient.request(
              CollectionAdminRequest.createCollection(
                  collectionName, confName, numShards, replicationFactor));
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to create collection '" + collectionName + "' due to: " + sse.getMessage());
    }

    if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
      CharArr arr = new CharArr();
      new JSONWriter(arr, 2).write(response.asMap());
      echo(arr.toString());
    } else {
      String endMessage =
          String.format(
              Locale.ROOT,
              "Created collection '%s' with %d shard(s), %d replica(s)",
              collectionName,
              numShards,
              replicationFactor);
      if (confName != null && !confName.trim().isEmpty()) {
        endMessage += String.format(Locale.ROOT, " with config-set '%s'", confName);
      }

      echo(endMessage);
    }
  }

  /**
   * Ensure the confDirName is a path to a directory by itself or when it is combined with the
   * solrInstallDir.
   */
  private void ensureConfDirExists(String confDirName, String solrInstallDir) {
    if (!new File(confDirName).isDirectory()) {
      final String fullConfDir = solrInstallDir + "/server/solr/configsets/" + confDirName;
      if (!new File(fullConfDir).isDirectory()) {
        echo("Specified configuration directory " + confDirName + " not found!");
        System.exit(1);
      }
    }
  }

  private void printDefaultConfigsetWarningIfNecessary(CommandLine cli) {
    final String confDirectoryName = cli.getOptionValue("confdir", "_default"); // CREATE_CONFDIR
    final String confName = cli.getOptionValue("confname", "");

    if (confDirectoryName.equals("_default")
        && (confName.equals("") || confName.equals("_default"))) {
      final String collectionName = cli.getOptionValue("name");
      final String solrUrl = cli.getOptionValue("solrUrl");
      final String curlCommand =
          String.format(
              Locale.ROOT,
              "curl %s/%s/config -d "
                  + "'{\"set-user-property\": {\"update.autoCreateFields\":\"false\"}}'",
              solrUrl,
              collectionName);
      final String configCommand =
          String.format(
              Locale.ROOT,
              "bin/solr config -c %s -p 8983 -action set-user-property -property update.autoCreateFields -value false",
              collectionName);
      echo(
          "WARNING: Using _default configset. Data driven schema functionality is enabled by default, which is");
      echo("         NOT RECOMMENDED for production use.");
      echo("");
      echo("         To turn it off:");
      echo("            " + curlCommand);
      echo("         Or:");
      echo("            " + configCommand);
    }
  }
}
