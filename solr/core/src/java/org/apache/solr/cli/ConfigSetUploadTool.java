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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DeprecatedAttributes;
import org.apache.commons.cli.Option;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.util.FileTypeMagicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk upconfig command in the bin/solr script. */
public class ConfigSetUploadTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ConfigSetUploadTool() {
    this(CLIO.getOutStream());
  }

  public ConfigSetUploadTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("n")
            .longOpt("conf-name")
            .hasArg()
            .argName("NAME")
            .required(false) // should be true, but we have deprecated option as well.
            .desc("Configset name in ZooKeeper.")
            .build(),
        Option.builder()
            .longOpt("confname")
            .hasArg()
            .argName("NAME")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --conf-name instead")
                    .get())
            .required(false)
            .desc("Configset name in ZooKeeper.")
            .build(),
        Option.builder("d")
            .longOpt("conf-dir")
            .hasArg()
            .argName("DIR")
            .required(false) // should be true, but we have deprecated option as well.
            .desc("Local directory with configs.")
            .build(),
        Option.builder()
            .longOpt("confdir")
            .hasArg()
            .argName("DIR")
            .deprecated(
                DeprecatedAttributes.builder()
                    .setForRemoval(true)
                    .setSince("9.7")
                    .setDescription("Use --conf-dir instead")
                    .get())
            .required(false)
            .desc("Local directory with configs.")
            .build(),
        SolrCLI.OPTION_SOLRURL,
        SolrCLI.OPTION_SOLRURL_DEPRECATED,
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_ZKHOST_DEPRECATED,
        SolrCLI.OPTION_CREDENTIALS);
  }

  @Override
  public String getName() {
    return "upconfig";
  }

  @Override
  public String getUsage() {
    return "bin/solr zk upconfig [-d <DIR>] [-n <NAME>] [-s <HOST>] [-u <credentials>] [-z <HOST>]";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);

    final String solrInstallDir = System.getProperty("solr.install.dir");
    Path solrInstallDirPath = Paths.get(solrInstallDir);

    String confName =
        cli.hasOption("conf-name")
            ? cli.getOptionValue("conf-name")
            : cli.getOptionValue("confname");
    String confDir =
        cli.hasOption("conf-dir") ? cli.getOptionValue("conf-dir") : cli.getOptionValue("confdir");
    try (SolrZkClient zkClient = SolrCLI.getSolrZkClient(cli, zkHost)) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);

      final Path configsetsDirPath = SolrCLI.getConfigSetsDir(solrInstallDirPath);
      Path confPath = ConfigSetService.getConfigsetPath(confDir, configsetsDirPath.toString());

      echo(
          "Uploading "
              + confPath.toAbsolutePath()
              + " for config "
              + cli.getOptionValue("conf-name")
              + " to ZooKeeper at "
              + zkHost);
      FileTypeMagicUtil.assertConfigSetFolderLegal(confPath);
      ZkMaintenanceUtils.uploadToZK(
          zkClient,
          confPath,
          ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName,
          ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);

    } catch (Exception e) {
      log.error("Could not complete upconfig operation for reason: ", e);
      throw (e);
    }
  }
}
