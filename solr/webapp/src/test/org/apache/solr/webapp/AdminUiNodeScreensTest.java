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
package org.apache.solr.webapp;

import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;

/**
 * Verifies the data displayed on the node-level Admin UI screens (java properties, thread dump,
 * cloud views, security, login) against the backing APIs.
 */
public class AdminUiNodeScreensTest extends AdminUiTestBase {

  private static final String COLLECTION = "nodescoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 2);
  }

  @Test
  public void testJavaPropertiesMatchApi() throws Exception {
    NamedList<Object> response = adminApi("/admin/info/properties", params());
    Map<?, ?> props = (Map<?, ?>) response.get("system.properties");
    String expectedJavaVersion = (String) props.get("java.version");

    openPage("~java-properties", By.id("java-properties"));
    waitFor(By.cssSelector("#java-properties li"));
    // find the row for java.version and compare its value; the UI inserts zero-width
    // spaces (&#8203;) into names and values for line wrapping, so strip them
    String value = null;
    for (WebElement row : driver.findElements(By.cssSelector("#java-properties li"))) {
      String name = row.findElement(By.cssSelector("dt")).getText().replace("​", "");
      if (name.equals("java.version")) {
        value = row.findElement(By.cssSelector("dd")).getText().replace("​", "");
      }
    }
    assertEquals(expectedJavaVersion, value);
  }

  @Test
  public void testThreadDumpShowsThreads() {
    openPage("~threads", By.id("thread-dump"));
    waitFor(By.cssSelector("#thread-dump tbody tr"));
    List<WebElement> rows = driver.findElements(By.cssSelector("#thread-dump tbody tr"));
    assertFalse("Thread dump should list threads", rows.isEmpty());
    // a Jetty worker thread is always present in a running Solr node
    waitForPageContains("qtp");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCloudNodesListsAllNodes() {
    openPage("~cloud?view=nodes", By.id("nodes-table"));
    // both cluster nodes run on the same host, shown as two node rows
    waitForPageContains("Hosts 1 - 1 of 1");
    List<WebElement> nodeNames = driver.findElements(By.cssSelector("#nodes-table .node-name"));
    assertEquals("Expected one row per live node", 2, nodeNames.size());
    for (var jetty : cluster.getJettySolrRunners()) {
      waitForPageContains(":" + jetty.getLocalPort());
    }
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCloudNodesUseRequestSpecificTargets() throws Exception {
    openPage("", By.id("index"));
    startNodeRequestCapture();
    JavascriptExecutor js = (JavascriptExecutor) driver;
    try {
      driver.get(baseUrl + "/index.html#/~cloud?view=nodes");
      waitFor(By.id("nodes-table"));
      List<String> nodes =
          cluster.getJettySolrRunners().stream().map(jetty -> jetty.getNodeName()).toList();
      waitUntil(
          "system information has returned for each node",
          () ->
              Boolean.TRUE.equals(
                  js.executeScript(
                      """
                      var scope = angular.element(document.getElementById('nodes-content')).scope();
                      if (!scope || !scope.nodes) return false;
                      return arguments[0].every(function(node) {
                        return scope.nodes[node] && scope.nodes[node].system;
                      });
                      """,
                      nodes)));
      waitUntil(
          "metrics requests have been sent for each node",
          () -> capturedNodeRequests("admin/metrics").size() >= nodes.size());
      for (String node : nodes) {
        assertEquals(
            node,
            js.executeScript(
                """
                var scope = angular.element(document.getElementById('nodes-content')).scope();
                return scope.nodes[arguments[0]].system.node;
                """,
                node));
        for (String path : List.of("/api/node/system", "admin/metrics")) {
          assertTrue(
              "Missing target header for " + node + " at " + path,
              capturedNodeRequests(path).stream().anyMatch(r -> node.equals(r.get("target"))));
          assertTrue(
              "Legacy node selectors must not be sent at " + path,
              capturedNodeRequests(path).stream()
                  .noneMatch(r -> r.get("url").toString().matches(".*[?&]nodes?=.*")));
        }
      }
      assertNoSevereConsoleErrors();
    } finally {
      stopNodeRequestCapture();
    }
  }

  @Test
  public void testSelectedLocalNodeReadsRemainPinned() throws Exception {
    String localNode = cluster.getJettySolrRunner(0).getNodeName();
    openPage("", By.id("index"));
    waitForTextContains(By.cssSelector("#content #index .node dd"), localNode);
    startNodeRequestCapture();
    try {
      String[][] pages = {
        {"~java-properties", "java-properties", "/api/node/properties"},
        {"~threads", "thread-dump", "/api/node/threads"}
      };
      for (String[] page : pages) {
        driver.get(baseUrl + "/index.html#/" + page[0]);
        waitFor(By.id(page[1]));
        waitUntil(
            "the selected-local request was sent at " + page[2],
            () -> !capturedNodeRequests(page[2]).isEmpty());
        assertTrue(
            "The selected node must be pinned even when it served the UI at " + page[2],
            capturedNodeRequests(page[2]).stream().allMatch(r -> localNode.equals(r.get("target"))));
      }
      waitUntil(
          "the selected-local core-menu request was sent",
          () -> !capturedNodeRequests("/api/cores").isEmpty());
      assertTrue(
          "Core-menu reads must stay pinned to the selected local node",
          capturedNodeRequests("/api/cores").stream()
              .allMatch(r -> localNode.equals(r.get("target"))));
      assertNoSevereConsoleErrors();
    } finally {
      stopNodeRequestCapture();
    }
  }

  @Test
  public void testCloudNodesRetainHealthyDataWhenSystemInfoFails() throws Exception {
    openPage("", By.id("index"));
    String localNode = cluster.getJettySolrRunner(0).getNodeName();
    String remoteNode = cluster.getJettySolrRunner(1).getNodeName();
    JavascriptExecutor js = (JavascriptExecutor) driver;
    try {
      js.executeScript(
          """
          var injector = angular.element(document.body).injector();
          var service = injector.get('NodeRequests');
          window.failedNodeRequest = {service: service, original: service.get};
          var remoteNode = arguments[0];
          service.get = function(url, node) {
            if (url === '/api/node/system' && node === remoteNode) {
              window.failedNodeRequest.triggered = true;
              return injector.get('$q').reject({status: 503});
            }
            return window.failedNodeRequest.original.apply(service, arguments);
          };
          """,
          remoteNode);
      driver.get(baseUrl + "/index.html#/~cloud?view=nodes");
      waitFor(By.id("nodes-table"));
      waitUntil(
          "the healthy node has data and the failed node request was attempted",
          () ->
              Boolean.TRUE.equals(
                  js.executeScript(
                      """
                      var scope = angular.element(document.getElementById('nodes-content')).scope();
                      return window.failedNodeRequest.triggered === true
                        && scope && scope.nodes && scope.nodes[arguments[0]]
                        && !!scope.nodes[arguments[0]].system;
                      """,
                      localNode)));
      assertEquals(
          localNode,
          js.executeScript(
              "return angular.element(document.getElementById('nodes-content')).scope()"
                  + ".nodes[arguments[0]].system.node;",
              localNode));
      assertTrue(
          "The failed node should show its own error without removing healthy rows",
          driver.findElements(By.cssSelector("#nodes-table .node-error")).stream()
              .anyMatch(e -> e.getText().contains("503")));
      assertEquals(2, driver.findElements(By.cssSelector("#nodes-table .node-name")).size());
      assertNoSevereConsoleErrors();
    } finally {
      js.executeScript(
          """
          if (window.failedNodeRequest) {
            window.failedNodeRequest.service.get = window.failedNodeRequest.original;
            delete window.failedNodeRequest;
          }
          """);
    }
  }

  @Test
  public void testCloudTreeShowsZkNodes() {
    openPage("~cloud?view=tree", By.id("tree-content"));
    waitForPageContains("live_nodes");
    waitForPageContains("collections");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCloudGraphShowsReplicas() throws Exception {
    openPage("~cloud?view=graph", By.id("graph-content"));
    // the d3 tree renders one circle per zk/collection/shard/replica node; the fixture
    // collection has two replicas, so expect at least: root + collection + shard + 2 replicas
    waitUntil(
        "graph should render circles",
        () -> driver.findElements(By.cssSelector("#graph-content svg circle")).size() >= 5);
    waitForPageContains(COLLECTION);
    waitForPageContains("shard1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCloudZkStatusShowsEnsemble() {
    openPage("~cloud?view=zkstatus", By.id("zk-status-content"));
    // the embedded test ensemble is a single standalone zookeeper reported green
    waitForTextContains(By.cssSelector(".zookeeper-status"), "green");
    waitForPageContains("Ensemble size: 1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testSecurityScreenWarnsNotEnabled() {
    openPage("~security", By.id("securityPanel"));
    waitForPageContains("Security is not enabled");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testLoginScreenWithoutAuthentication() {
    openPage("login", By.id("login"));
    waitForPageContains("uthentication");
    assertNoSevereConsoleErrors();
  }
}
