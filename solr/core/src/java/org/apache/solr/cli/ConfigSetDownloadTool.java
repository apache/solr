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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk downconfig command in the bin/solr script. */
public class ConfigSetDownloadTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ConfigSetDownloadTool() {
    this(CLIO.getOutStream());
  }

  public ConfigSetDownloadTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder()
            .longOpt("conf-name")
            .hasArg()
            .argName("NAME")
            .required(true)
            .desc("Configset name in ZooKeeper.")
            .build(),
        Option.builder()
            .longOpt("conf-dir")
            .hasArg()
            .argName("DIR")
            .required(true)
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
    return "downconfig";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);

    try (SolrZkClient zkClient = SolrCLI.getSolrZkClient(cli, zkHost)) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
      String confName = cli.getOptionValue("conf-name");
      String confDir = cli.getOptionValue("conf-dir");
      Path configSetPath = Paths.get(confDir);
      // we try to be nice about having the "conf" in the directory, and we create it if it's not
      // there.
      if (!configSetPath.endsWith("/conf")) {
        configSetPath = Paths.get(configSetPath.toString(), "conf");
      }
      Files.createDirectories(configSetPath);
      echo(
          "Downloading configset "
              + confName
              + " from ZooKeeper at "
              + zkHost
              + " to directory "
              + configSetPath.toAbsolutePath());

      zkClient.downConfig(confName, configSetPath);
    } catch (Exception e) {
      log.error("Could not complete downconfig operation for reason: ", e);
      throw (e);
    }
  }
}
