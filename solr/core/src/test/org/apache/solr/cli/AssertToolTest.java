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

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.common.util.SuppressForbidden;
import org.junit.BeforeClass;
import org.junit.Test;

public class AssertToolTest extends SolrCloudTestCase {
  private static final long CLOUD_NODES_TIMEOUT_MS = 500;
  private static final long CLOUD_NODES_TEST_DEADLINE_MS = CLOUD_NODES_TIMEOUT_MS * 10;

  /** Runs the tool. Overridden by the picocli variant of this test. */
  protected int runTool(String[] args, Class<? extends ToolBase> clazz) throws Exception {
    return CLITestHelper.runTool(args, clazz);
  }

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig("conf", configset("cloud-minimal")).configure();
  }

  @Test
  @SuppressWarnings("removal")
  @SuppressForbidden(reason = "Deprecated, for removal in future Java version")
  public void raisesExitCode100OnError() throws Exception {
    assumeTrue(
        "This test only works with security manager, as it raises an error accessing /tmp",
        System.getSecurityManager() != null);
    final String[] args = new String[] {"assert", "--exitcode", "--exists", "/tmp"};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals("Expected AssertTool to raise an error", 100, numAssertionsFailed);
  }

  @Test
  public void checksForTheExistenceOfDirectoryThatExists() throws Exception {
    Path tempDir = Files.createTempDirectory("myTempDir");
    final String[] args = new String[] {"assert", "--exitcode", "--exists", tempDir.toString()};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to pass assertion that directory exists", 0, numAssertionsFailed);
  }

  @Test
  public void checksForTheExistenceOfDirectoryThatDoesntExist() throws Exception {
    // we create a tempdir to avoid the Java Security Manager flagging that we can't access
    // /foo/bar/baz directly.
    Path tempDir = Files.createTempDirectory("myTempDir");
    final String[] args =
        new String[] {"assert", "--exitcode", "--exists", tempDir.toString() + "/foo/bar/baz"};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to fail assertion that directory exists", 1, numAssertionsFailed);
  }

  @Test
  public void checksForTheNonExistenceOfDirectoryThatExists() throws Exception {
    Path tempDir = Files.createTempDirectory("myTempDir");
    final String[] args = new String[] {"assert", "--exitcode", "--not-exists", tempDir.toString()};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to fail assertion that directory doesnt exist",
        1,
        numAssertionsFailed);
  }

  @Test
  public void checksForTheNonExistenceDirectoryThatDoesntExist() throws Exception {
    Path tempDir = Files.createTempDirectory("myTempDir");
    final String[] args =
        new String[] {"assert", "--exitcode", "--not-exists", tempDir.toString() + "/foo/bar/baz"};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to fail assertion that directory doesnt exist",
        0,
        numAssertionsFailed);
  }

  @Test
  public void checksForThePresenceOfSolrOnCorrectUrl() throws Exception {
    final String baseUrl = getRealSolrBaseUrl();
    final String[] args = new String[] {"assert", "--exitcode", "--started", baseUrl};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to pass assertion when Solr is running on provided URL",
        0,
        numAssertionsFailed);
  }

  @Test
  public void checksForThePresenceOfSolrOnIncorrectUrl() throws Exception {
    final String[] args =
        new String[] {"assert", "--exitcode", "--started", "http://www.google.com"};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to fail assertion when Solr isn't running on provided URL",
        1,
        numAssertionsFailed);
  }

  @Test
  public void checksForTheAbsenceOfSolrOnCorrectUrl() throws Exception {
    final String baseUrl = getRealSolrBaseUrl();
    final String[] args = new String[] {"assert", "--exitcode", "--not-started", baseUrl};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to fail assertion when Solr is running on provided URL",
        1,
        numAssertionsFailed);
  }

  @Test
  public void checksForTheAbsenceOfSolrOnIncorrectUrl() throws Exception {
    final String[] args =
        new String[] {"assert", "--exitcode", "--not-started", "http://www.google.com"};

    final int numAssertionsFailed = runTool(args, AssertTool.class);

    assertEquals(
        "Expected AssertTool to pass assertion when Solr isn't running on provided URL",
        0,
        numAssertionsFailed);
  }

  @Test
  public void checksForLiveCloudNodesUsingSolrUrl() throws Exception {
    final String[] args =
        new String[] {
          "assert", "--exitcode", "--cloud-nodes", "1", "--solr-url", getRealSolrBaseUrl()
        };

    assertEquals(0, runTool(args, AssertTool.class));
  }

  @Test
  public void failsWhenThereAreTooFewLiveCloudNodes() throws Exception {
    final String[] args =
        new String[] {
          "assert",
          "--exitcode",
          "--cloud-nodes",
          "2",
          "--solr-url",
          getRealSolrBaseUrl(),
          "--timeout",
          "0"
        };

    assertEquals(1, runTool(args, AssertTool.class));
  }

  @Test
  public void checksForLiveCloudNodesUsingTheDefaultSolrUrl() throws Exception {
    System.setProperty("solr.host", cluster.getJettySolrRunner(0).getBaseUrl().getHost());
    System.setProperty(
        "solr.port.listen", Integer.toString(cluster.getJettySolrRunner(0).getLocalPort()));
    System.setProperty("solr.url.scheme", cluster.getJettySolrRunner(0).getBaseUrl().getProtocol());
    final String[] args = new String[] {"assert", "--exitcode", "--cloud-nodes", "1"};

    assertEquals(0, runTool(args, AssertTool.class));
  }

  @Test
  public void checksForLiveCloudNodesUsingSolrUrlProperty() throws Exception {
    System.setProperty("solr.url", getRealSolrBaseUrl());
    System.setProperty("solr.port.listen", "1");
    final String[] args = new String[] {"assert", "--exitcode", "--cloud-nodes", "1"};

    assertEquals(0, runTool(args, AssertTool.class));
  }

  @Test
  public void timesOutWhenCloudNodeEndpointDoesNotRespond() throws Exception {
    try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        ExecutorService executor =
            ExecutorUtil.newMDCAwareSingleThreadExecutor(
                new SolrNamedThreadFactory("cloud-nodes-assertion"))) {
      server.setSoTimeout(Math.toIntExact(CLOUD_NODES_TEST_DEADLINE_MS));
      final String[] args =
          new String[] {
            "assert",
            "--exitcode",
            "--cloud-nodes",
            "1",
            "--solr-url",
            "http://localhost:" + server.getLocalPort(),
            "--timeout",
            Long.toString(CLOUD_NODES_TIMEOUT_MS)
          };
      var assertion = executor.submit(() -> runTool(args, AssertTool.class));
      try (Socket connection = server.accept()) {
        connection.setSoTimeout(Math.toIntExact(CLOUD_NODES_TEST_DEADLINE_MS));
        assertNotEquals(-1, connection.getInputStream().read());
        assertEquals(1, (int) assertion.get(CLOUD_NODES_TEST_DEADLINE_MS, TimeUnit.MILLISECONDS));
      }
    }
  }

  @Test
  public void rejectsNonPositiveCloudNodeCounts() throws Exception {
    final String[] args =
        new String[] {"assert", "--cloud-nodes", "0", "--solr-url", getRealSolrBaseUrl()};

    assertEquals(100, runTool(args, AssertTool.class));
  }

  private String getRealSolrBaseUrl() {
    final CloudSolrClient cloudSolrClient = cluster.getSolrClient();
    final Set<String> liveNodes = cloudSolrClient.getClusterState().getLiveNodes();
    final String firstLiveNode = liveNodes.iterator().next();

    return ZkStateReader.from(cloudSolrClient).getBaseUrlForNodeName(firstLiveNode);
  }
}
