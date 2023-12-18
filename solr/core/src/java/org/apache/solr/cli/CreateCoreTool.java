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
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.params.CommonParams;

public class CreateCoreTool extends ToolBase {

  public CreateCoreTool() {
    this(CLIO.getOutStream());
  }

  public CreateCoreTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "create_core";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        SolrCLI.OPTION_SOLRURL,
        Option.builder("c")
            .longOpt("name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the core to create.")
            .build(),
        Option.builder("d")
            .longOpt("confdir")
            .argName("NAME")
            .hasArg()
            .required(false)
            .desc(
                "Configuration directory to copy when creating the new core; default is "
                    + SolrCLI.DEFAULT_CONFIG_SET
                    + '.')
            .build(),
        // Option.builder("configsetsDir")
        //    .argName("DIR")
        //     .hasArg()
        //    .required(true)
        //    .desc("Path to configsets directory on the local system.")
        //    .build(),
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String coreName = cli.getOptionValue("name");
    String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.DEFAULT_SOLR_URL);

    final String solrInstallDir = System.getProperty("solr.install.dir");
    final String confDirName =
        cli.getOptionValue("confdir", SolrCLI.DEFAULT_CONFIG_SET); // CREATE_CONFDIR

    // we allow them to pass a directory instead of a configset name
    File configsetDir = new File(confDirName);
    if (!configsetDir.isDirectory()) {
      ensureConfDirExists(confDirName, solrInstallDir);
    }
    printDefaultConfigsetWarningIfNecessary(cli);

    String coreRootDirectory; // usually same as solr home, but not always
    try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
      Map<String, Object> systemInfo =
          solrClient
              .request(
                  new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH))
              .asMap();
      if ("solrcloud".equals(systemInfo.get("mode"))) {
        throw new IllegalStateException(
            "Solr at "
                + solrUrl
                + " is running in SolrCloud mode, please use create_collection command instead.");
      }

      // convert raw JSON into user-friendly output
      coreRootDirectory = (String) systemInfo.get("core_root");
    }

    if (SolrCLI.safeCheckCoreExists(solrUrl, coreName)) {
      throw new IllegalArgumentException(
          "\nCore '"
              + coreName
              + "' already exists!\nChecked core existence using Core API command");
    }

    File coreInstanceDir = new File(coreRootDirectory, coreName);
    File confDir = new File(getFullConfDir(confDirName, solrInstallDir), "conf");
    if (!coreInstanceDir.isDirectory()) {
      coreInstanceDir.mkdirs();
      if (!coreInstanceDir.isDirectory()) {
        throw new IOException(
            "Failed to create new core instance directory: " + coreInstanceDir.getAbsolutePath());
      }

      FileUtils.copyDirectoryToDirectory(confDir, coreInstanceDir);

      echoIfVerbose(
          "\nCopying configuration to new core instance directory:\n"
              + coreInstanceDir.getAbsolutePath(),
          cli);
    }

    echoIfVerbose("\nCreating new core '" + coreName + "' using CoreAdminRequest", cli);

    try (var solrClient = SolrCLI.getSolrClient(solrUrl)) {
      CoreAdminResponse res = CoreAdminRequest.createCore(coreName, coreName, solrClient);
      if (cli.hasOption(SolrCLI.OPTION_VERBOSE.getOpt())) {
        echo(res.jsonStr());
        echo("\n");
      } else {
        echo(String.format(Locale.ROOT, "\nCreated new core '%s'", coreName));
      }
    } catch (Exception e) {
      /* create-core failed, cleanup the copied configset before propagating the error. */
      PathUtils.deleteDirectory(coreInstanceDir.toPath());
      throw e;
    }
  }

  private String getFullConfDir(String confDirName, String solrInstallDir) {
    return solrInstallDir + "/server/solr/configsets/" + confDirName;
  }

  private String ensureConfDirExists(String confDirName, String solrInstallDir) {
    String fullConfDir = getFullConfDir(confDirName, solrInstallDir);
    if (!new File(fullConfDir).isDirectory()) {
      echo("Specified configuration directory " + confDirName + " not found!");
      System.exit(1);
    }

    return fullConfDir;
  }

  private void printDefaultConfigsetWarningIfNecessary(CommandLine cli) {
    final String confDirectoryName = cli.getOptionValue("confdir", "_default"); // CREATE_CONFDIR
    final String confName = cli.getOptionValue("confname", "");

    if (confDirectoryName.equals("_default")
        && (confName.equals("") || confName.equals("_default"))) {
      final String collectionName = cli.getOptionValue("collection");
      final String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.DEFAULT_SOLR_URL);
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
