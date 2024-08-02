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
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.JsonMapResponseParser;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.ConfigSetService;
import org.noggit.CharArr;
import org.noggit.JSONWriter;

/** Supports create command in the bin/solr script. */
public class CreateTool extends ToolBase {

  public CreateTool() {
    this(CLIO.getOutStream());
  }

  public CreateTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "create";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("c")
            .longOpt("name")
            .hasArg()
            .argName("NAME")
            .required(true)
            .desc("Name of collection or core to create.")
            .build(),
        Option.builder("sh")
            .longOpt("shards")
            .hasArg()
            .argName("#")
            .desc("Number of shards; default is 1.")
            .build(),
        Option.builder("rf")
            .longOpt("replication-factor")
            .hasArg()
            .argName("#")
            .desc(
                "Number of copies of each document across the collection (replicas per shard); default is 1.")
            .build(),
        Option.builder()
            .longOpt("replicationFactor")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --replication-factor instead")
                    .get())
            .hasArg()
            .argName("#")
            .required(false)
            .desc(
                "Number of copies of each document across the collection (replicas per shard); default is 1.")
            .build(),
        Option.builder("d")
            .longOpt("conf-dir")
            .argName("DIR")
            .hasArg()
            .desc(
                "Configuration directory to copy when creating the new collection; default is "
                    + SolrCLI.DEFAULT_CONFIG_SET
                    + '.')
            .build(),
        Option.builder("confdir")
            .longOpt("confdir")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --conf-dir instead")
                    .get())
            .argName("DIR")
            .hasArg()
            .required(false)
            .desc(
                "Configuration directory to copy when creating the new collection; default is "
                    + SolrCLI.DEFAULT_CONFIG_SET
                    + '.')
            .build(),
        Option.builder("n")
            .longOpt("conf-name")
            .argName("NAME")
            .hasArg()
            .required(false)
            .desc("Configuration name; default is the collection name.")
            .build(),
        Option.builder("confname")
            .longOpt("confname")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --conf-name instead")
                    .get())
            .argName("NAME")
            .hasArg()
            .required(false)
            .desc("Configuration name; default is the collection name.")
            .build(),
        SolrCLI.OPTION_SOLRURL,
        SolrCLI.OPTION_SOLRURL_DEPRECATED,
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_ZKHOST_DEPRECATED,
        SolrCLI.OPTION_CREDENTIALS);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);

    try (var solrClient = SolrCLI.getSolrClient(cli)) {
      if (SolrCLI.isCloudMode(solrClient)) {
        createCollection(cli);
      } else {
        createCore(cli, solrClient);
      }
    }
  }

  protected void createCore(CommandLine cli, SolrClient solrClient) throws Exception {
    String coreName = cli.getOptionValue("name");
    String solrUrl = cli.getOptionValue("solr-url", SolrCLI.getDefaultSolrUrl());

    final String solrInstallDir = System.getProperty("solr.install.dir");
    final String confDirName =
        cli.hasOption("confdir")
            ? cli.getOptionValue("confdir")
            : cli.getOptionValue("conf-dir", SolrCLI.DEFAULT_CONFIG_SET);

    // we allow them to pass a directory instead of a configset name
    Path configsetDir = Paths.get(confDirName);
    Path solrInstallDirPath = Paths.get(solrInstallDir);

    if (!Files.isDirectory(configsetDir)) {
      ensureConfDirExists(solrInstallDirPath, configsetDir);
    }
    printDefaultConfigsetWarningIfNecessary(cli);

    String coreRootDirectory; // usually same as solr home, but not always

    Map<String, Object> systemInfo =
        solrClient
            .request(new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH))
            .asMap();

    // convert raw JSON into user-friendly output
    coreRootDirectory = (String) systemInfo.get("core_root");

    if (SolrCLI.safeCheckCoreExists(
        solrUrl, coreName, cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt()))) {
      throw new IllegalArgumentException(
          "\nCore '"
              + coreName
              + "' already exists!\nChecked core existence using Core API command");
    }

    Path coreInstanceDir = Paths.get(coreRootDirectory, coreName);
    Path confDir = getFullConfDir(solrInstallDirPath, configsetDir).resolve("conf");
    if (!Files.isDirectory(coreInstanceDir)) {
      Files.createDirectories(coreInstanceDir);
      if (!Files.isDirectory(coreInstanceDir)) {
        throw new IOException(
            "Failed to create new core instance directory: " + coreInstanceDir.toAbsolutePath());
      }

      FileUtils.copyDirectoryToDirectory(confDir.toFile(), coreInstanceDir.toFile());

      echoIfVerbose(
          "\nCopying configuration to new core instance directory:\n"
              + coreInstanceDir.toAbsolutePath(),
          cli);
    }

    echoIfVerbose("\nCreating new core '" + coreName + "' using CoreAdminRequest", cli);

    try {
      CoreAdminResponse res = CoreAdminRequest.createCore(coreName, coreName, solrClient);
      if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
        echo(res.jsonStr());
        echo("\n");
      } else {
        echo(String.format(Locale.ROOT, "\nCreated new core '%s'", coreName));
      }
    } catch (Exception e) {
      /* create-core failed, cleanup the copied configset before propagating the error. */
      PathUtils.deleteDirectory(coreInstanceDir);
      throw e;
    }
  }

  protected void createCollection(CommandLine cli) throws Exception {
    Http2SolrClient.Builder builder =
        new Http2SolrClient.Builder()
            .withIdleTimeout(30, TimeUnit.SECONDS)
            .withConnectionTimeout(15, TimeUnit.SECONDS)
            .withKeyStoreReloadInterval(-1, TimeUnit.SECONDS)
            .withOptionalBasicAuthCredentials(
                cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt()));
    String zkHost = SolrCLI.getZkHost(cli);
    try (CloudSolrClient cloudSolrClient = SolrCLI.getCloudHttp2SolrClient(zkHost, builder)) {
      echoIfVerbose("Connecting to ZooKeeper at " + zkHost, cli);
      cloudSolrClient.connect();
      createCollection(cloudSolrClient, cli);
    }
  }

  protected void createCollection(CloudSolrClient cloudSolrClient, CommandLine cli)
      throws Exception {

    String collectionName = cli.getOptionValue("name");
    final String solrInstallDir = System.getProperty("solr.install.dir");
    String confName =
        cli.hasOption("conf-name")
            ? cli.getOptionValue("conf-name")
            : cli.getOptionValue("confname");
    String confDir =
        cli.hasOption("confdir")
            ? cli.getOptionValue("confdir")
            : cli.getOptionValue("conf-dir", SolrCLI.DEFAULT_CONFIG_SET);
    Path solrInstallDirPath = Paths.get(solrInstallDir);
    Path confDirPath = Paths.get(confDir);
    ensureConfDirExists(solrInstallDirPath, confDirPath);
    printDefaultConfigsetWarningIfNecessary(cli);

    Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new IllegalStateException(
          "No live nodes found! Cannot create a collection until "
              + "there is at least 1 live node in the cluster.");

    String solrUrl = cli.getOptionValue("solr-url");
    if (solrUrl == null) {
      String firstLiveNode = liveNodes.iterator().next();
      solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
    }

    // build a URL to create the collection
    int numShards = Integer.parseInt(cli.getOptionValue("shards", String.valueOf(1)));
    int replicationFactor = 1;

    if (cli.hasOption("replication-factor")) {
      replicationFactor = Integer.parseInt(cli.getOptionValue("replication-factor"));
    } else if (cli.hasOption("replicationFactor")) {
      replicationFactor = Integer.parseInt(cli.getOptionValue("replicationFactor"));
    }

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

      final Path configsetsDirPath = SolrCLI.getConfigSetsDir(solrInstallDirPath);
      Path confPath = ConfigSetService.getConfigsetPath(confDir, configsetsDirPath.toString());

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
    if (SolrCLI.safeCheckCollectionExists(
        solrUrl, collectionName, cli.getOptionValue(SolrCLI.OPTION_CREDENTIALS.getLongOpt()))) {
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
      var req =
          CollectionAdminRequest.createCollection(
              collectionName, confName, numShards, replicationFactor);
      req.setResponseParser(new JsonMapResponseParser());
      response = cloudSolrClient.request(req);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to create collection '" + collectionName + "' due to: " + sse.getMessage());
    }

    if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
      // pretty-print the response to stdout
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

  private Path getFullConfDir(Path solrInstallDir, Path confDirName) {
    return SolrCLI.getConfigSetsDir(solrInstallDir).resolve(confDirName);
  }

  private void ensureConfDirExists(Path solrInstallDir, Path confDirName) {
    if (!Files.isDirectory(confDirName)) {

      Path fullConfDir = getFullConfDir(solrInstallDir, confDirName);
      if (!Files.isDirectory(fullConfDir)) {
        echo("Specified configuration directory " + confDirName + " not found!");
        System.exit(1);
      }
    }
  }

  private void printDefaultConfigsetWarningIfNecessary(CommandLine cli) {
    final String confDirectoryName =
        cli.hasOption("confdir")
            ? cli.getOptionValue("confdir")
            : cli.getOptionValue("conf-dir", SolrCLI.DEFAULT_CONFIG_SET);
    final String confName = cli.getOptionValue("confname", "");

    if (confDirectoryName.equals("_default")
        && (confName.equals("") || confName.equals("_default"))) {
      final String collectionName = cli.getOptionValue("name");
      final String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.getDefaultSolrUrl());
      final String curlCommand =
          String.format(
              Locale.ROOT,
              "curl %s/solr/%s/config -d "
                  + "'{\"set-user-property\": {\"update.autoCreateFields\":\"false\"}}'",
              solrUrl,
              collectionName);
      final String configCommand =
          String.format(
              Locale.ROOT,
              "bin/solr config -c %s -s %s --action set-user-property --property update.autoCreateFields --value false",
              collectionName,
              solrUrl);
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
