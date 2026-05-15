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
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

/** Supports zk ls command in the bin/solr script. */
@CommandLine.Command(
    name = "ls",
    mixinStandardHelpOptions = true,
    description = "List the contents of a ZooKeeper node.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # List top-level ZooKeeper nodes",
      "  bin/solr zk ls /configs -z localhost:9983",
      "",
      "  # Recursively list a configset",
      "  bin/solr zk ls -r /configs/myconfig -z localhost:9983"
    })
public class ZkLsTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @CommandLine.Mixin ZkConnectionOptions zkOpts;

  @CommandLine.Parameters(
      index = "0",
      arity = "1",
      description = "The path of the ZooKeeper znode path to list.")
  private String path;

  @CommandLine.Mixin RecursiveOption recursiveOpt;

  public ZkLsTool() {
    this(new DefaultToolRuntime());
  }

  public ZkLsTool(ToolRuntime runtime) {
    super(runtime);
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
  public void runImpl(org.apache.commons.cli.CommandLine cli) throws Exception {
    String zkHost = CLIUtils.getZkHost(cli);
    String znode = cli.getArgs()[0];
    boolean recursive = cli.hasOption(CommonCLIOptions.RECURSIVE_OPTION);

    try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
      doLs(zkClient, zkHost, znode, recursive);
    } catch (Exception e) {
      log.error("Could not complete ls operation for reason: ", e);
      throw (e);
    }
  }

  private void doLs(SolrZkClient zkClient, String zkHost, String znode, boolean recursive)
      throws Exception {
    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");
    echoIfVerbose(
        "Getting listing for ZooKeeper node "
            + znode
            + " from ZooKeeper at "
            + zkHost
            + " recursive: "
            + recursive);
    runtime.print(zkClient.listZnode(znode, recursive));
  }

  @Override
  public int callTool() throws Exception {
    String zkHost = zkOpts.resolveZkHost();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doLs(zkClient, zkHost, path, recursiveOpt.recursive);
      return 0;
    } catch (Exception e) {
      log.error("Could not complete ls operation for reason: ", e);
      throw (e);
    }
  }
}
