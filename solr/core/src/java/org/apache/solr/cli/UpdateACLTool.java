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

import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.solr.client.solrj.impl.SolrZkClientTimeout;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.SolrZkClient;

/**
 * Supports updating ACL for a path in ZK
 *
 * <p>Set ACL properties by directly manipulating ZooKeeper.
 */
@SuppressWarnings("UnnecessarilyFullyQualified")
@picocli.CommandLine.Command(
    name = "updateacls",
    description =
        "Recursively re-applies ZooKeeper ACLs to a znode and all its descendants."
            + " The ACLs are determined by the ZkACLProvider configured via the zkACLProvider"
            + " system property (pass via SOLR_TOOL_OPTS). Without a custom provider, the"
            + " default OPEN_ACL_UNSAFE ACLs are applied.",
    footerHeading = "%nExamples:%n",
    footer = {
      "  # Apply ACLs to the root Solr znode",
      "  bin/solr zk updateacls /solr -z localhost:9983",
      "",
      "  # Apply ACLs to a specific configset",
      "  bin/solr zk updateacls /configs/myconfig -z localhost:9983"
    })
public class UpdateACLTool extends ToolBase {
  // It is a shame this tool doesn't more closely mimic how the ConfigTool works.

  @picocli.CommandLine.Mixin ZkConnectionOptions zkOpts;

  @picocli.CommandLine.Parameters(
      index = "0",
      arity = "1",
      description = "The ZooKeeper znode path to update ACLs for.")
  private String path;

  public UpdateACLTool() {
    this(new DefaultToolRuntime());
  }

  public UpdateACLTool(ToolRuntime runtime) {
    super(runtime);
  }

  @Override
  public String getName() {
    return "updateacls";
  }

  @Override
  public String getUsage() {
    return "bin/solr zk updateacls [-z <HOST>";
  }

  @Override
  public Options getOptions() {
    return super.getOptions().addOption(CommonCLIOptions.ZK_HOST_OPTION);
  }

  @Override
  public void runImpl(CommandLine cli) throws Exception {
    String zkHost = CLIUtils.getZkHost(cli);
    String path = cli.getArgs()[0];

    if (!ZkController.checkChrootPath(zkHost, true)) {
      throw new IllegalStateException(
          "A chroot was specified in zkHost but the znode doesn't exist.");
    }

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doUpdateAcls(zkClient, path);
    }
  }

  private void doUpdateAcls(SolrZkClient zkClient, String path) throws Exception {
    zkClient.updateACLs(path);
  }

  @Override
  public int callTool() throws Exception {
    String zkHost = zkOpts.resolveZkHost();

    if (!ZkController.checkChrootPath(zkHost, true)) {
      throw new IllegalStateException(
          "A chroot was specified in zkHost but the znode doesn't exist.");
    }

    try (SolrZkClient zkClient =
        new SolrZkClient.Builder()
            .withUrl(zkHost)
            .withTimeout(SolrZkClientTimeout.DEFAULT_ZK_CLIENT_TIMEOUT, TimeUnit.MILLISECONDS)
            .build()) {
      doUpdateAcls(zkClient, path);
      return 0;
    }
  }
}
