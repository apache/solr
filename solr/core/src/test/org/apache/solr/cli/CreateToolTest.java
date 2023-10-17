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

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.solr.cli.SolrCLI.findTool;
import static org.apache.solr.cli.SolrCLI.parseCmdLine;
import static org.apache.solr.security.Sha256AuthenticationProvider.getSaltedHashedValue;

import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.Utils;
import org.apache.solr.security.BasicAuthPlugin;
import org.apache.solr.security.RuleBasedAuthorizationPlugin;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateToolTest extends SolrCloudTestCase {

  private static final String USER = "solr";
  private static final String PASS = "SolrRocksAgain";
  private static final String collectionName = "testCreateCollectionWithBasicAuth";

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    final String SECURITY_JSON =
        Utils.toJSONString(
            Map.of(
                "authorization",
                Map.of(
                    "class",
                    RuleBasedAuthorizationPlugin.class.getName(),
                    "user-role",
                    singletonMap(USER, "admin"),
                    "permissions",
                    singletonList(Map.of("name", "all", "role", "admin"))),
                "authentication",
                Map.of(
                    "class",
                    BasicAuthPlugin.class.getName(),
                    "blockUnknown",
                    true,
                    "credentials",
                    singletonMap(USER, getSaltedHashedValue(PASS)))));

    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SECURITY_JSON)
        .configure();
  }

  @Test
  public void testCreateCollectionWithBasicAuth() throws Exception {

    String[] args = {
      "create",
      "-c",
      collectionName,
      "-n",
      "cloud-minimal",
      "-zkHost",
      cluster.getZkClient().getZkServerAddress(),
      "-credentials",
      USER + ":" + PASS,
      "-verbose"
    };

    assertEquals(0, runTool(args));
  }

  private int runTool(String[] args) throws Exception {
    Tool tool = findTool(args);
    assertTrue(tool instanceof CreateTool);
    CommandLine cli = parseCmdLine(tool.getName(), args, tool.getOptions());
    return tool.runTool(cli);
  }
}
