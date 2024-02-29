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

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;

/**
 * Supports cluster command in the bin/solr script.
 *
 * <p>Set cluster properties by directly manipulating ZooKeeper.
 */
public class ClusterTool extends ToolBase {
  // It is a shame this tool doesn't more closely mimic how the ConfigTool works.

  public ClusterTool() {
    this(CLIO.getOutStream());
  }

  public ClusterTool(PrintStream stdout) {
    super(stdout);
  }

  @Override
  public String getName() {
    return "cluster";
  }

  @Override
  public List<Option> getOptions() {
    return List.of(
        Option.builder()
            .longOpt("property")
            .argName("PROP")
            .hasArg()
            .required(true)
            .desc("Name of the Cluster property to apply the action to, such as: 'urlScheme'.")
            .build(),
        Option.builder()
            .longOpt("value")
            .argName("VALUE")
            .hasArg()
            .required(false)
            .desc("Set the property to this value.")
            .build(),
        SolrCLI.OPTION_ZKHOST);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {

    String propertyName = cli.getOptionValue("property");
    String propertyValue = cli.getOptionValue("value", null);
    String zkHost = SolrCLI.getZkHost(cli);

    if (!ZkController.checkChrootPath(zkHost, true)) {
      throw new IllegalStateException(
          "A chroot was specified in zkHost but the znode doesn't exist.");
    }

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {

      ClusterProperties props = new ClusterProperties(zkClient);
      try {
        props.setClusterProperty(propertyName, propertyValue);
      } catch (IOException ex) {
        throw new Exception(
            "Unable to set the cluster property due to following error : "
                + ex.getLocalizedMessage());
      }
    }
  }
}
