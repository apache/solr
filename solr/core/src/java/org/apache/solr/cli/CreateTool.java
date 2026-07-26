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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.cli.CommonCLIOptions.DefaultValues;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionsApi;
import org.apache.solr.client.solrj.request.ConfigsetsApi;
import org.apache.solr.client.solrj.request.CoresApi;
import org.apache.solr.client.solrj.request.SystemInfoRequest;
import org.apache.solr.client.solrj.response.SystemInfoResponse;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.core.ConfigSetService;

/** Supports create command in the bin/solr script. */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "create",
    mixinStandardHelpOptions = true,
    description =
        "Creates a core or collection depending on whether Solr is running in standalone (core) or SolrCloud mode (collection).",
    exitCodeListHeading = "%nExit Codes:%n",
    exitCodeList = {
      "0:Collection or core created successfully.",
      "1:Failed to create; Solr may not be running, or the collection/core already exists."
    },
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Create a collection or core",
      "  bin/solr create -c myCollection",
      "",
      "  # Create a collection with 2 shards and 2 replicas",
      "  bin/solr create -c myCollection -s 2 -rf 2",
      "",
      "  # Create a collection using an existing configset",
      "  bin/solr create -c myCollection -d myconfig"
    })
public class CreateTool extends ToolBase {

  private static final Option COLLECTION_NAME_OPTION =
      Option.builder("c")
          .longOpt("name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Name of collection or core to create.")
          .get();

  private static final Option SHARDS_OPTION =
      Option.builder("sh")
          .longOpt("shards")
          .hasArg()
          .argName("#")
          .type(Integer.class)
          .desc("Number of shards; default is 1.")
          .get();

  private static final Option REPLICATION_FACTOR_OPTION =
      Option.builder("rf")
          .longOpt("replication-factor")
          .hasArg()
          .argName("#")
          .type(Integer.class)
          .desc(
              "Number of copies of each document across the collection (replicas per shard); default is 1.")
          .get();

  private static final Option CONF_DIR_OPTION =
      Option.builder("d")
          .longOpt("conf-dir")
          .hasArg()
          .argName("DIR")
          .desc(
              "Configuration directory to copy when creating the new collection; default is "
                  + DefaultValues.DEFAULT_CONFIG_SET
                  + '.')
          .get();

  private static final Option CONF_NAME_OPTION =
      Option.builder("n")
          .longOpt("conf-name")
          .hasArg()
          .argName("NAME")
          .desc("Configuration name; default is the collection name.")
          .get();

  /** Options bean shared between commons-cli and picocli paths. */
  record CreateParams(
      String name,
      String confDir,
      String confName,
      String solrUrl,
      String credentials,
      int shards,
      int replicationFactor) {}

  // --- picocli fields ---

  @picocli.CommandLine.ArgGroup(exclusive = true, multiplicity = "0..1")
  private ConnectionOptions connectionOptions;

  @picocli.CommandLine.Mixin private CredentialsOptions credentialsOptions;

  @picocli.CommandLine.Option(
      names = {"-c", "--name"},
      required = true,
      description = "Name of collection or core to create.")
  private String name;

  @picocli.CommandLine.Option(
      names = {"-sh", "--shards"},
      description = "Number of shards; default is 1.",
      defaultValue = "1")
  private int shards;

  @picocli.CommandLine.Option(
      names = {"-rf", "--replication-factor"},
      description =
          "Number of copies of each document across the collection (replicas per shard); default is 1.",
      defaultValue = "1")
  private int replicationFactor;

  @picocli.CommandLine.Option(
      names = {"-d", "--conf-dir"},
      description =
          "Configuration directory to copy when creating the new collection; default is "
              + DefaultValues.DEFAULT_CONFIG_SET
              + ".",
      defaultValue = DefaultValues.DEFAULT_CONFIG_SET)
  private String confDir;

  @picocli.CommandLine.Option(
      names = {"-n", "--conf-name"},
      description = "Configuration name; default is the collection name.")
  private String confName;

  public CreateTool() {
    this(new DefaultToolRuntime());
  }

  public CreateTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "create";
  }

  @Override
  public String getHeader() {
    return """
        Creates a core or collection depending on whether Solr is running in standalone (core) or SolrCloud mode (collection).
        If you are using standalone mode you must run this command on the Solr server itself.

        List of options:""";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(COLLECTION_NAME_OPTION)
        .addOption(SHARDS_OPTION)
        .addOption(REPLICATION_FACTOR_OPTION)
        .addOption(CONF_DIR_OPTION)
        .addOption(CONF_NAME_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    try (var solrClient = CLIUtils.getSolrClient(cli)) {
      CreateParams params =
          new CreateParams(
              cli.getOptionValue(COLLECTION_NAME_OPTION),
              cli.getOptionValue(CONF_DIR_OPTION, DefaultValues.DEFAULT_CONFIG_SET),
              cli.getOptionValue(CONF_NAME_OPTION),
              cli.getOptionValue(CommonCLIOptions.SOLR_URL_OPTION, CLIUtils.getDefaultSolrUrl()),
              cli.getOptionValue(CommonCLIOptions.CREDENTIALS_OPTION),
              cli.getParsedOptionValue(SHARDS_OPTION, 1),
              cli.getParsedOptionValue(REPLICATION_FACTOR_OPTION, 1));
      if (CLIUtils.isCloudMode(solrClient)) {
        createCollection(params, solrClient);
      } else {
        createCore(params, solrClient);
      }
    }
  }

  private void createCore(CreateParams params, SolrClient solrClient) throws Exception {
    // we allow them to pass a directory instead of a configset name
    Path configsetDir = Path.of(params.confDir);
    Path solrInstallDirPath = Path.of(System.getProperty("solr.install.dir"));

    if (!Files.isDirectory(configsetDir)) {
      ensureConfDirExists(solrInstallDirPath, configsetDir);
    }
    printDefaultConfigsetWarning(params);

    SystemInfoResponse sysResponse = (new SystemInfoRequest()).process(solrClient);
    // usually same as solr home, but not always
    String coreRootDirectory = sysResponse.getCoreRoot();

    if (CLIUtils.safeCheckCoreExists(params.solrUrl, params.name, params.credentials)) {
      throw new IllegalArgumentException(
          "\nCore '"
              + params.name
              + "' already exists!\nChecked core existence using Core API command");
    }

    Path coreInstanceDir = Path.of(coreRootDirectory, params.name);
    Path confDir = getFullConfDir(solrInstallDirPath, configsetDir).resolve("conf");
    if (!Files.isDirectory(coreInstanceDir)) {
      Files.createDirectories(coreInstanceDir);
      if (!Files.isDirectory(coreInstanceDir)) {
        throw new IOException(
            "Failed to create new core instance directory: " + coreInstanceDir.toAbsolutePath());
      }

      PathUtils.copyDirectory(confDir, coreInstanceDir, StandardCopyOption.COPY_ATTRIBUTES);

      echoIfVerbose(
          "\nCopying configuration to new core instance directory:\n"
              + coreInstanceDir.toAbsolutePath());
    }

    echoIfVerbose("\nCreating new core '" + params.name + "' using V2 Cores API");

    try {
      var req = new CoresApi.CreateCore();
      req.setName(params.name);
      req.setInstanceDir(params.name);
      req.process(solrClient);
      echo(String.format(Locale.ROOT, "\nCreated new core '%s'", params.name));

    } catch (Exception e) {
      /* create-core failed, cleanup the copied configset before propagating the error. */
      PathUtils.deleteDirectory(coreInstanceDir);
      throw e;
    }
  }

  private void createCollection(CreateParams params, SolrClient solrClient) throws Exception {

    Path solrInstallDirPath = Path.of(System.getProperty("solr.install.dir"));
    Path confDirPath = Path.of(params.confDir);
    ensureConfDirExists(solrInstallDirPath, confDirPath);
    printDefaultConfigsetWarning(params);

    String solrUrl = params.solrUrl;

    // build a URL to create the collection
    int numShards = params.shards;
    int replicationFactor = params.replicationFactor;
    String confName = params.confName;

    boolean configExists =
        confName != null
            && !confName.trim().isEmpty()
            && new ConfigsetsApi.ListConfigSet().process(solrClient).configSets.contains(confName);

    if (configExists) {
      echo("Re-using existing configuration directory " + confName);
    } else { // if (confdir != null && !confdir.trim().isEmpty()) {
      if (confName == null || confName.trim().isEmpty()) {
        confName = params.name;
      }

      final Path configsetsDirPath = CLIUtils.getConfigSetsDir(solrInstallDirPath);
      Path confPath =
          ConfigSetService.getConfigsetPath(params.confDir, configsetsDirPath.toString());

      echoIfVerbose(
          "Uploading "
              + confPath.toAbsolutePath()
              + " for config "
              + confName
              + " using the Configsets V2 API");
      var uploadReq =
          new ConfigsetsApi.UploadConfigSet(
              confName, new ByteArrayInputStream(zipConfigSet(confPath)));
      uploadReq.process(solrClient);
    }

    // since creating a collection is a heavy-weight operation, check for existence first
    if (CLIUtils.safeCheckCollectionExists(solrUrl, params.name, params.credentials)) {
      throw new IllegalStateException(
          "\nCollection '"
              + params.name
              + "' already exists!\nChecked collection existence using V2 Collections API");
    }

    // doesn't seem to exist ... try to create
    echoIfVerbose("\nCreating new collection '" + params.name + "' using V2 Collections API");

    try {
      var req = new CollectionsApi.CreateCollection();
      req.setName(params.name);
      req.setConfig(confName);
      req.setNumShards(numShards);
      req.setReplicationFactor(replicationFactor);
      var response = req.process(solrClient);
      echoIfVerbose(response);
    } catch (SolrServerException sse) {
      throw new Exception(
          "Failed to create collection '" + params.name + "' due to: " + sse.getMessage());
    }

    String endMessage =
        String.format(
            Locale.ROOT,
            "Created collection '%s' with %d shard(s), %d replica(s)",
            params.name,
            numShards,
            replicationFactor);
    if (confName != null && !confName.trim().isEmpty()) {
      endMessage += String.format(Locale.ROOT, " with config-set '%s'", confName);
    }

    echo(endMessage);
  }

  /** Zips the contents of a configset directory for upload via the Configsets V2 API. */
  private static byte[] zipConfigSet(Path confPath) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ZipOutputStream zipOut = new ZipOutputStream(baos)) {
      Files.walkFileTree(
          confPath,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              zipOut.putNextEntry(new ZipEntry(confPath.relativize(file).toString()));
              Files.copy(file, zipOut);
              zipOut.closeEntry();
              return FileVisitResult.CONTINUE;
            }
          });
    }
    return baos.toByteArray();
  }

  private Path getFullConfDir(Path solrInstallDir, Path confDirName) {
    return CLIUtils.getConfigSetsDir(solrInstallDir).resolve(confDirName);
  }

  private void ensureConfDirExists(Path solrInstallDir, Path confDirName) {
    if (!Files.isDirectory(confDirName)) {

      Path fullConfDir = getFullConfDir(solrInstallDir, confDirName);
      if (!Files.isDirectory(fullConfDir)) {
        echo("Specified configuration directory " + confDirName + " not found!");
        runtime.exit(1);
      }
    }
  }

  private void printDefaultConfigsetWarning(CreateParams params) {
    printDefaultConfigsetWarning(
        params.confDir,
        params.confName != null ? params.confName : "",
        params.name,
        params.solrUrl != null ? params.solrUrl : CLIUtils.getDefaultSolrUrl());
  }

  private void printDefaultConfigsetWarning(
      String confDirName, String confNameArg, String collectionName, String solrUrl) {
    if (confDirName.equals("_default")
        && (confNameArg.isEmpty() || confNameArg.equals("_default"))) {
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

  @Override
  public int callTool() throws Exception {
    String zkHostArg =
        (connectionOptions != null) ? connectionOptions.zkHost : EnvUtils.getProperty("zkHost");
    String solrUrlArg = (connectionOptions != null) ? connectionOptions.solrUrl : null;

    if (zkHostArg != null) {
      String resolvedSolrUrl = CLIUtils.resolveSolrUrlFromZkHost(zkHostArg);
      CreateParams params =
          new CreateParams(
              name,
              confDir,
              confName,
              resolvedSolrUrl,
              credentialsOptions.credentials,
              shards,
              replicationFactor);
      try (var solrClient =
          CLIUtils.getSolrClient(resolvedSolrUrl, credentialsOptions.credentials)) {
        createCollection(params, solrClient);
      }
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
      CreateParams params =
          new CreateParams(
              name,
              confDir,
              confName,
              resolvedSolrUrl,
              credentialsOptions.credentials,
              shards,
              replicationFactor);
      try (var solrClient =
          CLIUtils.getSolrClient(resolvedSolrUrl, credentialsOptions.credentials)) {
        if (CLIUtils.isCloudMode(solrClient)) {
          createCollection(params, solrClient);
        } else {
          createCore(params, solrClient);
        }
      }
    }
    return 0;
  }
}
