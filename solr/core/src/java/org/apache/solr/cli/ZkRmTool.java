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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.common.cloud.SolrZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkRmTool extends ToolBase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public ZkRmTool() {
    this(CLIO.getOutStream());
  }

  public ZkRmTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("path")
            .argName("path")
            .hasArg()
            .required(true)
            .desc("Path to remove.")
            .build(),
        SolrCLI.OPTION_RECURSE,
        SolrCLI.OPTION_ZKHOST,
        SolrCLI.OPTION_VERBOSE);
  }

  @Override
  public String getName() {
    return "rm";
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    SolrCLI.raiseLogLevelUnlessVerbose(cli);
    String zkHost = SolrCLI.getZkHost(cli);

    if (zkHost == null) {
      throw new IllegalStateException(
          "Solr at "
              + cli.getOptionValue("zkHost")
              + " is running in standalone server mode, 'zk rm' can only be used when running in SolrCloud mode.\n");
    }
    String target = cli.getOptionValue("path");
    boolean recurse = Boolean.parseBoolean(cli.getOptionValue("recurse"));

    String znode = target;
    if (target.toLowerCase(Locale.ROOT).startsWith("zk:")) {
      znode = target.substring(3);
    }
    if (znode.equals("/")) {
      throw new SolrServerException("You may not remove the root ZK node ('/')!");
    }
    echoIfVerbose("\nConnecting to ZooKeeper at " + zkHost + " ...", cli);
    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      if (!recurse && zkClient.getChildren(znode, null, true).size() != 0) {
        throw new SolrServerException(
            "ZooKeeper node " + znode + " has children and recurse has NOT been specified.");
      }
      echo(
          "Removing ZooKeeper node "
              + znode
              + " from ZooKeeper at "
              + zkHost
              + " recurse: "
              + recurse);
      zkClient.clean(znode);
    } catch (Exception e) {
      log.error("Could not complete rm operation for reason: ", e);
      throw (e);
    }
  }
}
