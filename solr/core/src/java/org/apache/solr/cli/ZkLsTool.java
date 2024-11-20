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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk ls command in the bin/solr script. */
public class ZkLsTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ZkLsTool() {
    this(CLIO.getOutStream());
  }

  public ZkLsTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(CommonCLIOptions.RECURSIVE_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public String getName() {
    return "ls";
  }

  @Override
  public String getUsage() {
    // very brittle.  Maybe add a getArgsUsage to append the "path"?
    return "bin/solr zk ls [-r ] [-s <HOST>] [-u <credentials>] [-v] [-z <HOST>] path";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String zkHost = CLIUtils.getZkHost(cli);
    String znode = cli.getArgs()[0];

    try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");

      boolean recursive = cli.hasOption(CommonCLIOptions.RECURSIVE_OPTION);
      echoIfVerbose(
          "Getting listing for ZooKeeper node "
              + znode
              + " from ZooKeeper at "
              + zkHost
              + " recursive: "
              + recursive);
      stdout.print(zkClient.listZnode(znode, recursive));
    } catch (Exception e) {
      log.error("Could not complete ls operation for reason: ", e);
      throw (e);
    }
  }
}
