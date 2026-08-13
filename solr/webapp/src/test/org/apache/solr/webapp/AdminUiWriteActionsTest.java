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
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/** Exercises write actions performed through the Admin UI, verifying the effect via the APIs. */
public class AdminUiWriteActionsTest extends AdminUiTestBase {

  private static final String CONFIG = "writeconf";
  private static final String COLLECTION = "writecoll";

  @BeforeClass
  public static void setupFixture() throws Exception {
    cluster.uploadConfigSet(ExternalPaths.DEFAULT_CONFIGSET, CONFIG);
    CollectionAdminRequest.createCollection(COLLECTION, CONFIG, 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testCreateAndDeleteCollectionViaUi() throws Exception {
    String name = "uicreated";
    openPage("~collections", By.id("collections"));

    // create through the Add Collection dialog
    waitFor(By.cssSelector("#navigation button#add")).click();
    WebElement nameInput = waitFor(By.id("add_name"));
    nameInput.clear();
    nameInput.sendKeys(name);
    chosenSelect("add_config", CONFIG);
    WebElement numShards = waitFor(By.id("add_numShards"));
    numShards.clear();
    numShards.sendKeys("1");
    WebElement replicationFactor = waitFor(By.id("add_replicationFactor"));
    replicationFactor.clear();
    replicationFactor.sendKeys("1");
    waitFor(By.xpath("//button[@ng-click='addCollection()']")).click();

    // the new collection shows up in the list, and the API confirms it
    waitForPageContains(name);
    assertCollectionExists(name, true);

    // delete it through the delete dialog, which requires typing the name to confirm
    openPage("~collections/" + name, By.id("collections"));
    waitFor(By.id("delete-collection")).click();
    WebElement confirmInput = waitFor(By.id("collectionDeleteConfirm"));
    confirmInput.clear();
    confirmInput.sendKeys(name);
    waitFor(By.xpath("//button[@ng-click='deleteCollection()']")).click();

    assertCollectionExists(name, false);
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testIndexDocumentViaUi() throws Exception {
    openPage(COLLECTION + "/documents", By.id("documents"));
    WebElement docInput = waitFor(By.id("document"));
    docInput.clear();
    docInput.sendKeys("{\"id\":\"ui-doc-1\",\"title_txt\":\"indexed from the admin ui\"}");
    waitFor(By.id("submit")).click();
    waitForTextContains(By.cssSelector("#documents #result"), "success");

    // the document becomes searchable (the form defaults to commitWithin=1000)
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    long numFound = 0;
    while (System.nanoTime() < deadlineNanos) {
      numFound =
          cluster
              .getSolrClient(COLLECTION)
              .query(new SolrQuery("id:ui-doc-1"))
              .getResults()
              .getNumFound();
      if (numFound > 0) break;
      Thread.sleep(250);
    }
    assertEquals("Document indexed via UI should be searchable", 1, numFound);
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testChangeLogLevelViaUi() throws Exception {
    String logger = "org.apache.solr.core";
    openPage("~logging/level", By.id("loggingtree"));

    WebElement anchor =
        waitFor(By.cssSelector("#loggingtree a.jstree-anchor[title='" + logger + "']"));
    anchor.click();
    waitFor(By.xpath("//li[a/@title='" + logger + "']//a[normalize-space()='WARN']")).click();
    assertLoggerLevel(logger, "WARN");

    // revert to unset; the logger then reports the inherited level with set=false
    waitFor(By.cssSelector("#loggingtree a.jstree-anchor[title='" + logger + "']")).click();
    waitFor(By.xpath("//li[a/@title='" + logger + "']//a[normalize-space()='UNSET']")).click();
    assertLoggerLevel(logger, null);
    assertNoSevereConsoleErrors();
  }

  private void assertCollectionExists(String name, boolean expectExists) throws Exception {
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    boolean exists = !expectExists;
    while (System.nanoTime() < deadlineNanos) {
      List<String> collections = CollectionAdminRequest.listCollections(cluster.getSolrClient());
      exists = collections.contains(name);
      if (exists == expectExists) return;
      Thread.sleep(250);
    }
    fail(
        "Collection "
            + name
            + " should "
            + (expectExists ? "" : "not ")
            + "exist, but does"
            + (exists ? "" : " not"));
  }

  /**
   * Asserts the level a logger was explicitly set to, or with {@code expectedLevel} null, that the
   * logger has no explicit level (it then reports the inherited effective level with set=false).
   */
  @SuppressWarnings("unchecked")
  private void assertLoggerLevel(String logger, String expectedLevel) throws Exception {
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    Object actualLevel = "(logger not found)";
    Object actualSet = null;
    while (System.nanoTime() < deadlineNanos) {
      NamedList<Object> response = adminApi("/admin/info/logging", params());
      for (Map<?, ?> entry : (List<Map<?, ?>>) response.get("loggers")) {
        if (logger.equals(entry.get("name"))) {
          actualLevel = entry.get("level");
          actualSet = entry.get("set");
        }
      }
      boolean matches =
          expectedLevel == null
              ? Boolean.FALSE.equals(actualSet)
              : expectedLevel.equals(actualLevel) && Boolean.TRUE.equals(actualSet);
      if (matches) return;
      Thread.sleep(250);
    }
    fail(
        "Logger "
            + logger
            + " expected level "
            + (expectedLevel == null ? "(unset)" : expectedLevel)
            + " but was "
            + actualLevel
            + " (set="
            + actualSet
            + ")");
  }
}
