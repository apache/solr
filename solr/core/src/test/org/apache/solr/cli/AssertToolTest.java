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

import static org.apache.solr.cli.SolrCLI.parseCmdLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.BeforeClass;
import org.junit.Test;

public class AssertToolTest extends SolrCloudTestCase {

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  public void checksForTheExistenceOfDirectoryThatExists() throws Exception {
    Path tempDir = Files.createTempDirectory("myTempDir");
    final String[] args = new String[] {"--exitcode", "--exists", tempDir.toString()};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to pass assertion that directory exists", numAssertionsFailed, 0);
  }

  @Test
  public void checksForTheExistenceOfDirectoryThatDoesntExist() throws Exception {
    final String[] args = new String[] {"--exitcode", "--exists", "/foo/bar/baz"};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to fail assertion that directory exists", numAssertionsFailed, 1);
  }

  @Test
  public void checksForTheNonExistenceOfDirectoryThatExists() throws Exception {
    Path tempDir = Files.createTempDirectory("myTempDir");
    final String[] args = new String[] {"--exitcode", "--not-exists", tempDir.toString()};
  }

  @Test
  public void checksForTheNonExistenceDirectoryThatDoesntExist() throws Exception {
    final String[] args = new String[] {"--exitcode", "--not-exists", "/foo/bar/baz"};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to fail assertion that directory doesnt exist",
        numAssertionsFailed,
        1);
  }

  @Test
  public void checksForThePresenceOfSolrOnCorrectUrl() throws Exception {
    final String baseUrl = getRealSolrBaseUrl();
    final String[] args = new String[] {"--exitcode", "--started", baseUrl};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to pass assertion when Solr is running on provided URL",
        numAssertionsFailed,
        0);
  }

  @Test
  public void checksForThePresenceOfSolrOnIncorrectUrl() throws Exception {
    final String[] args = new String[] {"--exitcode", "--started", "http://www.google.com"};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to fail assertion when Solr isn't running on provided URL",
        numAssertionsFailed,
        1);
  }

  @Test
  public void checksForTheAbsenceOfSolrOnCorrectUrl() throws Exception {
    final String baseUrl = getRealSolrBaseUrl();
    final String[] args = new String[] {"--exitcode", "--not-started", baseUrl};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to fail assertion when Solr is running on provided URL",
        numAssertionsFailed,
        1);
  }

  @Test
  public void checksForTheAbsenceOfSolrOnIncorrectUrl() throws Exception {
    final String[] args = new String[] {"--exitcode", "--not-started", "http://www.google.com"};

    final int numAssertionsFailed = runAssertToolWithArgs(args);

    assertEquals(
        "Expected AssertTool to pass assertion when Solr isn't running on provided URL",
        numAssertionsFailed,
        0);
  }

  private int runAssertToolWithArgs(String[] args) throws Exception {
    final AssertTool tool = new AssertTool();
    final CommandLine cli = parseCmdLine(tool, args);
    return tool.runTool(cli);
  }

  private String getRealSolrBaseUrl() {
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    final String firstLiveNode = liveNodes.iterator().next();
    // return cloudClient.getBaseUrlForNodeName(firstLiveNode);
    return ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
  }
}
