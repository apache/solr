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

import java.io.File;
import java.io.FileNotFoundException;
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
        Option.builder(NAME)
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the core to create.")
            .build(),
        Option.builder("confdir")
            .argName("CONFIG")
            .hasArg()
            .required(false)
            .desc(
                "Configuration directory to copy when creating the new core; default is "
                    + SolrCLI.DEFAULT_CONFIG_SET
                    + '.')
            .build(),
        Option.builder("configsetsDir")
            .argName("DIR")
            .hasArg()
            .required(true)
            .desc("Path to configsets directory on the local system.")
            .build(),
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String solrUrl = cli.getOptionValue("solrUrl", SolrCLI.getDefaultSolrUrl());
    if (!solrUrl.endsWith("/")) solrUrl += "/";

    File configsetsDir = new File(cli.getOptionValue("configsetsDir"));
    if (!configsetsDir.isDirectory())
      throw new FileNotFoundException(configsetsDir.getAbsolutePath() + " not found!");

    String configSet = cli.getOptionValue("confdir", SolrCLI.DEFAULT_CONFIG_SET);
    File configSetDir = new File(configsetsDir, configSet);
    if (!configSetDir.isDirectory()) {
      // we allow them to pass a directory instead of a configset name
      File possibleConfigDir = new File(configSet);
      if (possibleConfigDir.isDirectory()) {
        configSetDir = possibleConfigDir;
      } else {
        throw new FileNotFoundException(
            "Specified config directory "
                + configSet
                + " not found in "
                + configsetsDir.getAbsolutePath());
      }
    }

    String coreName = cli.getOptionValue(NAME);

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

      // Fall back to solr_home, in case we are running against older server that does not return
      // the property
      if (coreRootDirectory == null) coreRootDirectory = (String) systemInfo.get("solr_home");
      if (coreRootDirectory == null)
        coreRootDirectory = configsetsDir.getParentFile().getAbsolutePath();
    }

    if (SolrCLI.safeCheckCoreExists(solrUrl, coreName)) {
      throw new IllegalArgumentException(
          "\nCore '"
              + coreName
              + "' already exists!\nChecked core existence using Core API command");
    }

    File coreInstanceDir = new File(coreRootDirectory, coreName);
    File confDir = new File(configSetDir, "conf");
    if (!coreInstanceDir.isDirectory()) {
      coreInstanceDir.mkdirs();
      if (!coreInstanceDir.isDirectory())
        throw new IOException(
            "Failed to create new core instance directory: " + coreInstanceDir.getAbsolutePath());

      if (confDir.isDirectory()) {
        FileUtils.copyDirectoryToDirectory(confDir, coreInstanceDir);
      } else {
        // hmmm ... the configset we're cloning doesn't have a conf sub-directory,
        // we'll just assume it is OK if it has solrconfig.xml
        if ((new File(configSetDir, "solrconfig.xml")).isFile()) {
          FileUtils.copyDirectory(configSetDir, new File(coreInstanceDir, "conf"));
        } else {
          throw new IllegalArgumentException(
              "\n"
                  + configSetDir.getAbsolutePath()
                  + " doesn't contain a conf subdirectory or solrconfig.xml\n");
        }
      }
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
} // end CreateCoreTool class
