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
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;

/**
 * Supports linking a configset to a collection
 *
 */
public class LinkConfigTool extends ToolBase {

  public LinkConfigTool() {
    this(CLIO.getOutStream());
  }

  public LinkConfigTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "linkconfig";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder("c")
            .longOpt("name")
            .argName("NAME")
            .hasArg()
            .required(true)
            .desc("Name of the collection to link.")
            .build(),
        Option.builder("confname")
            .argName("confname")
            .hasArg()
            .required(true)
            .desc("Configset name in ZooKeeper.")
            .build(),
        SolrCLI.OPTION_ZKHOST);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {

    String collection = cli.getOptionValue("name");
    String confName = cli.getOptionValue("confname");
    String zkHost = SolrCLI.getZkHost(cli);

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {

      ZkController.linkConfSet(zkClient, collection, confName);
    }
  }
}
