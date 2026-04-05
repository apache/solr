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
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.core.ConfigSetService;
import org.apache.solr.util.FileTypeMagicUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk upconfig command in the bin/solr script. */
@picocli.CommandLine.Command(
    name = "upconfig",
    mixinStandardHelpOptions = true,
    description = "Upload a configset from the local filesystem to ZooKeeper.")
public class ConfigSetUploadTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Option CONF_NAME_OPTION =
      Option.builder("n")
          .longOpt("conf-name")
          .hasArg()
          .argName("NAME")
          .required()
          .desc("Configset name in ZooKeeper.")
          .get();

  private static final Option CONF_DIR_OPTION =
      Option.builder("d")
          .longOpt("conf-dir")
          .hasArg()
          .argName("DIR")
          .required()
          .desc("Local directory with configs.")
          .get();

  @picocli.CommandLine.Mixin ZkConnectionOptions zkOpts;

  @picocli.CommandLine.Option(
      names = {"-n", "--conf-name"},
      description = "Configset name in ZooKeeper.",
      required = true)
  private String confName;

  @picocli.CommandLine.Option(
      names = {"-d", "--conf-dir"},
      description = "Local directory with configs.",
      required = true,
      paramLabel = "DIR")
  private String confDir;

  public ConfigSetUploadTool() {
    this(new DefaultToolRuntime());
  }

  public ConfigSetUploadTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(CONF_NAME_OPTION)
        .addOption(CONF_DIR_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
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
    String zkHost = CLIUtils.getZkHost(cli);
    String confName = cli.getOptionValue(CONF_NAME_OPTION);
    String confDir = cli.getOptionValue(CONF_DIR_OPTION);

    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");
    try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
      doUpconfig(zkClient, zkHost, confName, confDir);
    } catch (Exception e) {
      log.error("Could not complete upconfig operation for reason: ", e);
      throw (e);
    }
  }

  private void doUpconfig(SolrZkClient zkClient, String zkHost, String confName, String confDir)
      throws Exception {
    final String solrInstallDir = System.getProperty("solr.install.dir");
    Path solrInstallDirPath = Path.of(solrInstallDir);
    final Path configsetsDirPath = CLIUtils.getConfigSetsDir(solrInstallDirPath);
    Path confPath = ConfigSetService.getConfigsetPath(confDir, configsetsDirPath.toString());

    echo(
        "Uploading "
            + confPath.toAbsolutePath()
            + " for config "
            + confName
            + " to ZooKeeper at "
            + zkHost);
    FileTypeMagicUtil.assertConfigSetFolderLegal(confPath);
    ZkMaintenanceUtils.uploadToZK(
        zkClient,
        confPath,
        ZkMaintenanceUtils.CONFIGS_ZKNODE + "/" + confName,
        ZkMaintenanceUtils.UPLOAD_FILENAME_EXCLUDE_PATTERN);
  }

  @Override
  public int callTool() throws Exception {
    String zkHost = zkOpts.resolveZkHost();

    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doUpconfig(zkClient, zkHost, confName, confDir);
      return 0;
    } catch (Exception e) {
      log.error("Could not complete upconfig operation for reason: ", e);
      throw (e);
    }
  }
}
