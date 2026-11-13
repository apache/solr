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
