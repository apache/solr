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

import static org.apache.solr.cli.SolrCLI.findTool;
import static org.apache.solr.cli.SolrCLI.parseCmdLine;

import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

public class HealthcheckToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CloudSolrClient cloudSolrClient = cluster.getSolrClient();

    CollectionAdminRequest.createCollection("bob", 1, 1).process(cloudSolrClient);
  }

  @Test
  public void testHealthcheckWithZkHostParameter() throws Exception {

    String[] args =
        new String[] {
          "healthcheck", "-collection", "bob", "-zkHost", cluster.getZkClient().getZkServerAddress()
        };
    assertEquals(0, runTool(args));
  }

  @Test
  public void testHealthcheckWithSolrUrlParameter() throws Exception {

    Set<String> liveNodes = cluster.getSolrClient().getClusterState().getLiveNodes();
    String firstLiveNode = liveNodes.iterator().next();
    String solrUrl =
        ZkStateReader.from(cluster.getSolrClient()).getBaseUrlForNodeName(firstLiveNode);

    String[] args = new String[] {"healthcheck", "-collection", "bob", "-solrUrl", solrUrl};
    assertEquals(0, runTool(args));
  }

  private int runTool(String[] args) throws Exception {
    Tool tool = findTool(args);
    assertTrue(tool instanceof HealthcheckTool);
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    return tool.runTool(cli);
  }
}
