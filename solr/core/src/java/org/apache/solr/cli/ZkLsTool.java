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
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk ls command in the bin/solr script. */
@picocli.CommandLine.Command(name = "ls", description = "List the contents of a ZooKeeper node.")
public class ZkLsTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // NOCOMMIT: Find a way to make this common for all tools that need it
  @picocli.CommandLine.Option(
      names = {"-z", "--zk-host"},
      description =
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh; otherwise, defaults to "
              + CommonCLIOptions.DefaultValues.ZK_HOST
              + '.',
      required = true)
  String zkHost;

  // TODO: Can refer to parent command
  @picocli.CommandLine.ParentCommand private ZkTool parent;

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      description = "The path of the ZooKeeper znode path to list.")
  private String path;

  @picocli.CommandLine.Option(
      names = {"-r", "--recursive"},
      description = "Apply the command recursively.")
  private boolean recursive;

  @picocli.CommandLine.Option(
      names = {"-u", "--credentials"},
      description =
          "Credentials in the format username:password. Example: --credentials solr:SolrRocks")
  private String credentials;

  @picocli.CommandLine.Option(
      names = {"-s", "--solr-url"},
      description = "Base Solr URL, which can be used to determine the zk-host if that's not known")
  private String solrUrl;

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
    zkHost = CLIUtils.getZkHost(cli);
    path = cli.getArgs()[0];
    recursive = cli.hasOption(CommonCLIOptions.RECURSIVE_OPTION);

    try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
      doLs(zkClient);
    } catch (Exception e) {
      log.error("Could not complete ls operation for reason: ", e);
      throw (e);
    }
  }

  private void doLs(SolrZkClient zkClient) throws Exception {
    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");

    echoIfVerbose(
        "Getting listing for ZooKeeper node "
            + path
            + " from ZooKeeper at "
            + zkHost
            + " recursive: "
            + recursive);
    stdout.print(zkClient.listZnode(path, recursive));
  }

  @Override
  public int callTool() throws Exception {
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doLs(zkClient);
      return 0;
    } catch (Exception e) {
      log.error("Could not complete ls operation for reason: ", e);
      throw (e);
    }
  }
}
