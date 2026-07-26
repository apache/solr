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

import java.util.Arrays;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.util.SecurityJson;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupClusterWithSecurityEnabled() throws Exception {
    configureCluster(2)
        .addConfig("conf", configset("cloud-minimal"))
        .withSecurityJson(SecurityJson.SIMPLE)
        .configure();
  }

  /** Runs the tool via the commons-cli invocation path (see {@link CLITestHelper#runTool}). */
  private int runCommonsCli(String[] args) throws Exception {
    return CLITestHelper.runTool(args, CreateTool.class);
  }

  /**
   * Runs the tool via the picocli invocation path (see {@link ZkSubcommandsPicocliTest} for the
   * same pattern used elsewhere).
   */
  private int runPicocli(String[] args) throws Exception {
    // args[0] is the tool/subcommand name used by commons-cli dispatch; strip it for picocli.
    String[] toolArgs = Arrays.copyOfRange(args, 1, args.length);
    ToolBase tool = CreateTool.class.getDeclaredConstructor().newInstance();
    return new picocli.CommandLine(tool)
        .setDefaultValueProvider(new CliDefaultValueProvider())
        .execute(toolArgs);
  }

  private String[] createCollectionWithBasicAuthArgs(String collectionName) {
    return new String[] {
      "create",
      "-c",
      collectionName,
      "-n",
      "cloud-minimal",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--credentials",
      SecurityJson.USER_PASS,
      "--verbose"
    };
  }

  @Test
  public void testCreateCollectionWithBasicAuth() throws Exception {
    assertEquals(
        0,
        runCommonsCli(
            createCollectionWithBasicAuthArgs("testCreateCollectionWithBasicAuth-commonsCli")));
    assertEquals(
        0,
        runPicocli(createCollectionWithBasicAuthArgs("testCreateCollectionWithBasicAuth-picocli")));
  }

  private String[] createCollectionUploadsNewConfigSetArgs(String collectionName) {
    return new String[] {
      "create",
      "-c",
      collectionName,
      "-d",
      configset("cloud-minimal").toString(),
      "-n",
      "cloud-minimal-uploaded",
      "-z",
      cluster.getZkClient().getZkServerAddress(),
      "--credentials",
      SecurityJson.USER_PASS,
      "--verbose"
    };
  }

  @Test
  public void testCreateCollectionUploadsNewConfigSet() throws Exception {
    assertEquals(
        0,
        runCommonsCli(
            createCollectionUploadsNewConfigSetArgs(
                "testCreateCollectionUploadsNewConfigSet-commonsCli")));
    assertEquals(
        0,
        runPicocli(
            createCollectionUploadsNewConfigSetArgs(
                "testCreateCollectionUploadsNewConfigSet-picocli")));
  }
}
