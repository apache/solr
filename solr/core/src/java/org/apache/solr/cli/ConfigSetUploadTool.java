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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkMaintenanceUtils;
import org.apache.solr.core.ConfigSetService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        Option.builder("confname")
            .argName("confname") // Comes out in help message
            .hasArg() // Has one sub-argument
            .required(true) // confname argument must be present
            .desc("Configset name in ZooKeeper.")
            .build(), // passed as -confname value
        Option.builder("confdir")
            .argName("confdir")
            .hasArg()
            .required(true)
            .desc("Local directory with configs.")
            .build(),
        Option.builder("configsetsDir")
            .argName("configsetsDir")
            .hasArg()
            .required(false)
            .desc("Parent directory of example configsets.")
            .build(),
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public String getName() {
    return "upconfig";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);
    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at "
              + cli.getOptionValue("solrUrl")
              + " is running in standalone server mode, upconfig can only be used when running in SolrCloud mode.\n");
    }

    String confName = cli.getOptionValue("confname");
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      Path confPath =
          ConfigSetService.getConfigsetPath(
              cli.getOptionValue("confdir"), cli.getOptionValue("configsetsDir"));

      echo(
          "Uploading "
              + confPath.toAbsolutePath()
              + " for config "
              + cli.getOptionValue("confname")
              + " to ZooKeeper at "
              + zkHost);
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
