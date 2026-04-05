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

import static org.apache.solr.cli.CLIUtils.getCloudSolrClient;
import static org.apache.solr.cli.CLIUtils.normalizeSolrUrl;

import java.util.Set;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.EnvUtils;
import picocli.CommandLine;

/** Provides default values for CLI arguments. */
public class CliDefaultValueProvider implements CommandLine.IDefaultValueProvider {
  @Override
  public String defaultValue(CommandLine.Model.ArgSpec argSpec) throws Exception {
    return switch (argSpec.paramLabel()) {
      case "<zkHost>" -> EnvUtils.getProperty("zkHost");
      case "<solrUrl>" -> {
        String val = EnvUtils.getProperty("solr.url");
        yield val != null ? val : resolveSolrUrlViaZkHost(argSpec);
      }
      case "<port>" -> EnvUtils.getProperty("solr.port", "8983");
      case "<maxWaitSecs>" -> EnvUtils.getProperty("solr.max.wait.seconds", "0");
      default -> null;
    };
  }

  /**
   * If no solrUrl is provided on the command line, and SOLR_URL is not set, this method will be
   * used to determine the solrUrl from the zkHost.
   *
   * @param argSpec the argSpec for the solrUrl option
   * @return the solrUrl
   * @throws Exception if an error occurs
   */
  public static String resolveSolrUrlViaZkHost(picocli.CommandLine.Model.ArgSpec argSpec)
      throws Exception {
    // Find value of zkHost from command line options. The argSpec passed in will be for the
    // solrUrl option.
    CommandLine.Model.OptionSpec zkHostOption = argSpec.command().findOption("--zk-host");

    String zkHost = zkHostOption != null ? zkHostOption.getValue() : null;
    if (zkHost == null) {
      return null;
    }

    String solrUrl;
    try (CloudSolrClient cloudSolrClient = getCloudSolrClient(zkHost)) {
      cloudSolrClient.connect();
      Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
      if (liveNodes.isEmpty())
        throw new IllegalStateException(
            "No live nodes found! Cannot determine 'solrUrl' from ZooKeeper: " + zkHost);

      String firstLiveNode = liveNodes.iterator().next();
      solrUrl = ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
      solrUrl = normalizeSolrUrl(solrUrl, false);
    }
    solrUrl = normalizeSolrUrl(solrUrl);
    return solrUrl;
  }
}
