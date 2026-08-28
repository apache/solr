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
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Tests the Collections screen ({@code #/~collections}): display of collection details, and the
 * write actions offered by the screen - create/delete collection, aliases, replicas and reload.
 */
public class AdminUiCollectionsScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "collscreen";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testCollectionDetailDisplay() {
    openPage("~collections/" + COLLECTION, By.id("collections"));
    waitForPageContains(COLLECTION);
    waitForPageContains("shard1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCreateAndDeleteCollectionViaUi() throws Exception {
    String name = "uicreated";
    openPage("~collections", By.id("collections"));

    // create through the Add Collection dialog
    click(By.cssSelector("#navigation button#add"));
    WebElement nameInput = waitFor(By.id("add_name"));
    nameInput.clear();
    nameInput.sendKeys(name);
    chosenSelect("add_config", COLLECTION);
    WebElement numShards = waitFor(By.id("add_numShards"));
    numShards.clear();
    numShards.sendKeys("1");
    WebElement replicationFactor = waitFor(By.id("add_replicationFactor"));
    replicationFactor.clear();
    replicationFactor.sendKeys("1");
    click(By.xpath("//button[@ng-click='addCollection()']"));

    // the new collection shows up in the list, and the API confirms it
    waitForPageContains(name);
    assertCollectionExists(name, true);

    // delete it through the delete dialog, which requires typing the name to confirm
    openPage("~collections/" + name, By.id("collections"));
    click(By.id("delete-collection"));
    WebElement confirmInput = waitFor(By.id("collectionDeleteConfirm"));
    confirmInput.clear();
    confirmInput.sendKeys(name);
    click(By.xpath("//button[@ng-click='deleteCollection()']"));

    assertCollectionExists(name, false);
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCreateAndDeleteAliasViaUi() throws Exception {
    String alias = "uialias";
    openPage("~collections", By.id("collections"));

    // the Create Alias button stays disabled until the collection list has loaded
    waitForPageContains(COLLECTION);
    click(By.cssSelector("button#create-alias:not([disabled])"));
    WebElement aliasInput = waitFor(By.id("alias"));
    aliasInput.clear();
    aliasInput.sendKeys(alias);
    // the collections picker is a plain multi-select; click the option directly
    click(By.xpath("//select[@id='aliasCollections']/option[text()='" + COLLECTION + "']"));
    click(By.xpath("//button[@ng-click='createAlias()']"));

    waitUntil(
        "alias " + alias + " should exist",
        () -> listAliases().getOrDefault(alias, "").equals(COLLECTION));

    // aliases are listed with an alias_ route prefix
    openPage("~collections/alias_" + alias, By.id("collections"));
    click(By.id("delete-alias"));
    click(By.xpath("//button[@ng-click='deleteAlias()']"));

    waitUntil("alias " + alias + " should be gone", () -> !listAliases().containsKey(alias));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testAddAndDeleteReplicaViaUi() throws Exception {
    openPage("~collections/" + COLLECTION, By.id("collections"));

    // expand shard1 and open the add-replica form
    click(By.xpath("//div[@id='shard-data']//a[contains(., 'shard1')]"));
    click(By.id("add-replica"));
    click(By.xpath("//button[@ng-click='addReplica(shard)']"));

    waitUntil("second replica should appear", () -> replicaCount() == 2);

    // delete the added replica: expand it and confirm removal within the same replica block
    driver.navigate().refresh();
    click(By.xpath("//div[@id='shard-data']//a[contains(., 'shard1')]"));
    waitFor(By.xpath("//a[@ng-click='toggleRemoveReplica(replica)']"));
    List<WebElement> removeToggles =
        driver.findElements(By.xpath("//a[@ng-click='toggleRemoveReplica(replica)']"));
    assertEquals("Expected a remove toggle per replica", 2, removeToggles.size());
    WebElement toggle = removeToggles.get(1);
    toggle.click();
    toggle
        .findElement(
            By.xpath(
                "ancestor::ul[contains(@class,'replica')][1]"
                    + "//button[@ng-click='deleteReplica(replica)']"))
        .click();

    waitUntil("replica should be removed again", () -> replicaCount() == 1);
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testReloadCollectionViaUi() throws Exception {
    // reloading resets the core's start time; that proves the action end-to-end,
    // unlike the UI success indicator which only flashes for a second
    String coreName = coreNameOnNode0(COLLECTION);
    Object startTimeBefore = coreStartTime(coreName);

    openPage("~collections/" + COLLECTION, By.id("collections"));
    click(By.id("reload"));
    waitUntil(
        "core start time should change after reload",
        () -> !startTimeBefore.equals(coreStartTime(coreName)));
    assertNoSevereConsoleErrors();
  }

  private void assertCollectionExists(String name, boolean expectExists) throws Exception {
    waitUntil(
        "collection " + name + " should " + (expectExists ? "exist" : "not exist"),
        () -> {
          try {
            return CollectionAdminRequest.listCollections(cluster.getSolrClient()).contains(name)
                == expectExists;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private Map<String, String> listAliases() {
    try {
      CollectionAdminResponse response =
          new CollectionAdminRequest.ListAliases().process(cluster.getSolrClient());
      return response.getAliases();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Object coreStartTime(String coreName) {
    try {
      return adminApi("/admin/cores", params("core", coreName))
          ._get(List.of("status", coreName, "startTime"), null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private int replicaCount() {
    try {
      return (int)
          cluster
              .getSolrClient()
              .getClusterState()
              .getCollection(COLLECTION)
              .replicaStream()
              .count();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
