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

import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;

/**
 * Browser coverage for the Admin UI live-node picker: selecting another node, verifying node-scoped
 * screens follow that target, restoring the selection from sessionStorage across reload, and
 * confirming the picker stays hidden in standalone mode (covered separately).
 */
public class AdminUiNodePickerTest extends AdminUiTestBase {

  private static final By DASHBOARD_NODE = By.cssSelector("#content #index .node dd");

  @Before
  public void clearPersistedNodeSelection() {
    ensureCloudCluster();
    // openPage boots via index.html; clear any selection left by a prior test in this JVM
    driver.get(baseUrl + "/index.html#/");
    waitFor(By.cssSelector("#content > *"));
    ((JavascriptExecutor) driver)
        .executeScript("sessionStorage.removeItem('adminui.selectedNode');");
  }

  /** Selects another live node through the Chosen node picker. */
  private void selectNode(String nodeName) {
    chosenSelect("node_selector_select", nodeName);
  }

  @Test
  public void testSelectRemoteNodeUpdatesDashboardAndPersistsAcrossReload() throws Exception {
    String localNode = cluster.getJettySolrRunner(0).getNodeName();
    String remoteNode = cluster.getJettySolrRunner(1).getNodeName();

    openPage("", By.id("index"));
    waitFor(By.id("node-selector-item"));
    waitForTextContains(DASHBOARD_NODE, localNode);
    waitFor(By.id("selected-node-badge"));

    selectNode(remoteNode);

    waitForTextContains(DASHBOARD_NODE, remoteNode);
    waitForTextContains(By.cssSelector("#selected-node-badge .badge-value"), remoteNode);
    assertTrue(
        "badge should mark remote targeting",
        waitFor(By.id("selected-node-badge")).getAttribute("class").contains("remote"));
    waitForTextContains(By.id("node-selector-hint"), "Node screens only");

    // full reload must restore the remote target before the dashboard fetch
    driver.navigate().refresh();
    waitFor(By.cssSelector("#content #index"));
    waitForTextContains(DASHBOARD_NODE, remoteNode);
    waitForTextContains(By.cssSelector("#selected-node-badge .badge-value"), remoteNode);
    assertTrue(waitFor(By.id("selected-node-badge")).getAttribute("class").contains("remote"));

    assertNoSevereConsoleErrors();
  }

  @Test
  public void testNodeSelectionSurvivesInAppNavigation() {
    String remoteNode = cluster.getJettySolrRunner(1).getNodeName();

    openPage("", By.id("index"));
    waitFor(By.id("node-selector-item"));
    selectNode(remoteNode);
    waitForTextContains(DASHBOARD_NODE, remoteNode);

    // in-app navigation must keep the picker selection (sessionStorage) and show the badge
    click(By.cssSelector("#menu #java-properties a"));
    waitFor(By.id("java-properties"));
    waitFor(By.cssSelector("#java-properties li"));
    waitForTextContains(By.cssSelector("#selected-node-badge .badge-value"), remoteNode);
    assertTrue(waitFor(By.id("selected-node-badge")).getAttribute("class").contains("remote"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testLoggingLevelsScreenFollowsSelectedNode() {
    String remoteNode = cluster.getJettySolrRunner(1).getNodeName();

    openPage("", By.id("index"));
    waitFor(By.id("node-selector-item"));
    selectNode(remoteNode);

    click(By.cssSelector("#menu #logging a"));
    waitFor(By.id("logging"));
    click(By.cssSelector("#menu #logging .level a"));
    waitFor(By.id("loggingtree"));

    waitForTextContains(By.cssSelector("#selected-node-badge .badge-value"), remoteNode);
    assertTrue(waitFor(By.id("selected-node-badge")).getAttribute("class").contains("remote"));
    waitForTextContains(By.id("node-selector-hint"), "Node screens only");
    assertNoSevereConsoleErrors();
  }
}
