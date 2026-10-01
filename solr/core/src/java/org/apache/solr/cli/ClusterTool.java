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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.ClusterProperties;
import org.apache.solr.common.cloud.SolrZkClient;

/**
 * Supports cluster command in the bin/solr script.
 *
 * <p>Set cluster properties by directly manipulating ZooKeeper.
 */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "cluster",
    description = "Set cluster properties by directly manipulating ZooKeeper.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Set the urlScheme cluster property",
      "  bin/solr cluster --property urlScheme --value https"
    })
public class ClusterTool extends ToolBase {
  // It is a shame this tool doesn't more closely mimic how the ConfigTool works.

  private static final Option PROPERTY_OPTION =
      Option.builder()
          .longOpt("property")
          .hasArg()
          .argName("PROPERTY")
          .required()
          .desc("Name of the Cluster property to apply the action to, such as: 'urlScheme'.")
          .get();

  private static final Option VALUE_OPTION =
      Option.builder()
          .longOpt("value")
          .hasArg()
          .argName("VALUE")
          .desc("Set the property to this value.")
          .get();

  /** Parameters for the cluster command, independent of the command line parser. */
  record ClusterParams(String propertyName, String propertyValue, String zkHost) {}

  // --- picocli fields ---

  @picocli.CommandLine.Option(
      names = "--property",
      required = true,
      paramLabel = "PROPERTY",
      description = "Name of the Cluster property to apply the action to, such as: 'urlScheme'.")
  private String propertyOpt;

  @picocli.CommandLine.Option(
      names = "--value",
      paramLabel = "VALUE",
      description = "Set the property to this value.")
  private String valueOpt;

  @picocli.CommandLine.Option(
      names = {"-z", "--zk-host"},
      paramLabel = "zkHost",
      description =
          "Zookeeper connection string; unnecessary if ZK_HOST is defined in solr.in.sh;"
              + " otherwise, discovered from a running Solr instance.")
  private String zkHostOpt;

  public ClusterTool() {
    this(new DefaultToolRuntime());
  }

  public ClusterTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "cluster";
  }

  @Override
  public Options getOptions() {
    return super.getOptions()
        .addOption(PROPERTY_OPTION)
        .addOption(VALUE_OPTION)
        .addOption(CommonCLIOptions.ZK_HOST_OPTION);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    ClusterParams params =
        new ClusterParams(
            cli.getOptionValue(PROPERTY_OPTION),
            cli.getOptionValue(VALUE_OPTION),
            CLIUtils.getZkHost(cli));
    setClusterProperty(params);
  }

  void setClusterProperty(ClusterParams params) throws Exception {
    String propertyName = params.propertyName();
    String propertyValue = params.propertyValue();
    String zkHost = params.zkHost();

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

  @Override
  public int callTool() throws Exception {
    ClusterParams params = new ClusterParams(propertyOpt, valueOpt, resolveZkHost());
    setClusterProperty(params);
    return 0;
  }

  /**
   * Mirrors {@link CLIUtils#getZkHost(CommandLine)}: explicit {@code --zk-host} wins outright,
   * otherwise discovered from a running Solr instance at the default URL.
   */
  private String resolveZkHost() throws Exception {
    if (zkHostOpt != null && !zkHostOpt.isBlank()) {
      return zkHostOpt;
    }
    String defaultSolrUrl = CLIUtils.getDefaultSolrUrl();
    try (var solrClient = CLIUtils.getSolrClient(defaultSolrUrl, null)) {
      Map<String, Object> status = StatusTool.reportStatus(solrClient);
      @SuppressWarnings("unchecked")
      Map<String, Object> cloud = (Map<String, Object>) status.get("cloud");
      if (cloud == null) {
        return null;
      }
      String zookeeper = (String) cloud.get("ZooKeeper");
      if (zookeeper.endsWith("(embedded)")) {
        zookeeper = zookeeper.substring(0, zookeeper.length() - "(embedded)".length());
      }
      return zookeeper;
    }
  }
}
