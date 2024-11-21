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

import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Supports zk mkroot command in the bin/solr script. */
public class ZkMkrootTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final Option FAIL_ON_EXISTS_OPTION =
      Option.builder()
          .longOpt("fail-on-exists")
          .hasArg()
          .desc("Raise an error if the root exists.  Defaults to false.")
          .build();

  public ZkMkrootTool() {
    this(CLIO.getOutStream());
  }

  public ZkMkrootTool(PrintStream stdout) {
    super(stdout);
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
      echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...");

      echo("Creating ZooKeeper path " + znode + " on ZooKeeper at " + zkHost);
      zkClient.makePath(znode, failOnExists, true);
    } catch (Exception e) {
      log.error("Could not complete mkroot operation for reason: ", e);
      throw (e);
    }
  }
}
