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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk rm command in the bin/solr script. */
@picocli.CommandLine.Command(name = "rm", description = "Remove a znode from ZooKeeper.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Remove a single file from ZooKeeper",
      "  bin/solr zk rm /configs/myconfig/solrconfig.xml -z localhost:9983",
      "",
      "  # Recursively remove a configset",
      "  bin/solr zk rm -r /configs/myconfig -z localhost:9983"
    })
public class ZkRmTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @picocli.CommandLine.Mixin ZkConnectionOptions zkOpts;

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      description = "The ZooKeeper znode path to remove (zk: prefix optional).")
  private String path;

  @picocli.CommandLine.Mixin RecursiveOption recursiveOpt;

  public ZkRmTool() {
    this(new DefaultToolRuntime());
  }

  public ZkRmTool(ToolRuntime runtime) {
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
    return "rm";
  }

  @Override
  public String getUsage() {
    return "bin/solr zk rm [-r ] [-s <HOST>] [-u <credentials>] [-v] [-z <HOST>] path";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String zkHost = CLIUtils.getZkHost(cli);
    String target = cli.getArgs()[0];
    boolean recursive = cli.hasOption(CommonCLIOptions.RECURSIVE_OPTION);

    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");
    try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
      doRm(zkClient, zkHost, target, recursive);
    } catch (Exception e) {
      log.error("Could not complete rm operation for reason: ", e);
      throw (e);
    }
  }

  private void doRm(SolrZkClient zkClient, String zkHost, String target, boolean recursive)
      throws Exception {
    String znode = target;
    if (target.toLowerCase(Locale.ROOT).startsWith("zk:")) {
      znode = target.substring(3);
    }
    if (znode.equals("/")) {
      throw new SolrServerException("You may not remove the root ZK node ('/')!");
    }
    if (!recursive && !zkClient.getChildren(znode, null).isEmpty()) {
      throw new SolrServerException(
          "ZooKeeper node " + znode + " has children and recursive has NOT been specified.");
    }
    echo(
        "Removing ZooKeeper node "
            + znode
            + " from ZooKeeper at "
            + zkHost
            + " recursive: "
            + recursive);
    zkClient.clean(znode);
  }

  @Override
  public int callTool() throws Exception {
    String zkHost = zkOpts.resolveZkHost();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doRm(zkClient, zkHost, path, recursiveOpt.recursive);
      return 0;
    } catch (Exception e) {
      log.error("Could not complete rm operation for reason: ", e);
      throw (e);
    }
  }
}
