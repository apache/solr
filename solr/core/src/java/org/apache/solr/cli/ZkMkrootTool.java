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

import static org.apache.solr.packagemanager.PackageUtils.format;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk mkroot command in the bin/solr script. */
@picocli.CommandLine.Command(
    name = "mkroot",
    description =
        "Make a znode in ZooKeeper with no data. Can be used to make a path of arbitrary depth"
            + " but primarily intended to create a 'chroot'.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Create a chroot path for Solr",
      "  bin/solr zk mkroot /solr -z localhost:2181",
      "",
      "  # Create a nested chroot path",
      "  bin/solr zk mkroot /solr/myapp -z localhost:2181"
    })
public class ZkMkrootTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Option FAIL_ON_EXISTS_OPTION =
      Option.builder()
          .longOpt("fail-on-exists")
          .hasArg()
          .desc("Raise an error if the root exists.  Defaults to false.")
          .get();

  @picocli.CommandLine.Mixin ZkConnectionOptions zkOpts;

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      description = "The ZooKeeper znode path to create.")
  private String path;

  @picocli.CommandLine.Option(
      names = {"--fail-on-exists"},
      arity = "0..1",
      fallbackValue = "true",
      description = "Raise an error if the znode already exists. Defaults to false.")
  private boolean failOnExists;

  public ZkMkrootTool() {
    this(new DefaultToolRuntime());
  }

  public ZkMkrootTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(FAIL_ON_EXISTS_OPTION)
        .addOption(CommonCLIOptions.CREDENTIALS_OPTION)
        .addOptionGroup(getConnectionOptions());
  }

  @Override
  public String getName() {
    return "mkroot";
  }

  @Override
  public String getUsage() {
    return "bin/solr zk mkroot [--fail-on-exists <arg>] [-s <HOST>] [-u <credentials>] [-v] [-z <HOST>] path";
  }

  @Override
  public String getHeader() {
    StringBuilder sb = new StringBuilder();
    format(
        sb,
        "mkroot makes a znode in Zookeeper with no data. Can be used to make a path of arbitrary");
    format(sb, "depth but primarily intended to create a 'chroot'.\n\nList of options:");
    return sb.toString();
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String zkHost = CLIUtils.getZkHost(cli);
    String znode = cli.getArgs()[0];
    boolean failOnExists = cli.hasOption(FAIL_ON_EXISTS_OPTION);

    try (SolrZkClient zkClient = CLIUtils.getSolrZkClient(cli, zkHost)) {
      doMkroot(zkClient, zkHost, znode, failOnExists);
    } catch (Exception e) {
      log.error("Could not complete mkroot operation for reason: ", e);
      throw (e);
    }
  }

  private void doMkroot(SolrZkClient zkClient, String zkHost, String znode, boolean failOnExists)
      throws Exception {
    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");
    echo("Creating ZooKeeper path " + znode + " on ZooKeeper at " + zkHost);
    zkClient.makePath(znode, failOnExists);
  }

  @Override
  public int callTool() throws Exception {
    String zkHost = zkOpts.resolveZkHost();

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doMkroot(zkClient, zkHost, path, failOnExists);
      return 0;
    } catch (Exception e) {
      log.error("Could not complete mkroot operation for reason: ", e);
      throw (e);
    }
  }
}
