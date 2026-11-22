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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;

/**
 * Tests the Schema Browser screen: browsing fields and their flags, term info, and adding/deleting
 * fields and copy-field rules through the UI dialogs (the fixture uses a mutable managed schema).
 */
public class AdminUiSchemaScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "schemacoll";
  private static final String COPY_SOURCE = "ui_copy_source";
  private static final String COPY_DEST_ONE = "ui_copy_dest_one";
  private static final String COPY_DEST_TWO = "ui_copy_dest_two";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
    SolrClient client = cluster.getSolrClient(COLLECTION);
    for (int i = 1; i <= 3; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc" + i);
      client.add(doc);
    }
    client.commit();
    for (String field : List.of(COPY_SOURCE, COPY_DEST_ONE, COPY_DEST_TWO)) {
      new SchemaRequest.AddField(Map.of("name", field, "type", "string", "multiValued", true))
          .process(cluster.getSolrClient(), COLLECTION);
    }
  }

  @Test
  public void testSchemaBrowserShowsFields() {
    openPage(COLLECTION + "/schema", By.id("schema"));
    // managed schema is editable, so the action buttons are shown
    waitFor(By.id("addField"));
    // known fields from the _default configset are browsable
    waitForPageContains("_version_");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testFieldDetailShowsFlagsAndTermInfo() throws Exception {
    // the id field of _default is indexed, stored and required per the schema API
    NamedList<Object> response = adminApi("/" + COLLECTION + "/schema/fields/id", params());
    Map<?, ?> field = (Map<?, ?>) response.get("field");
    assertEquals(Boolean.TRUE, field.get("indexed"));
    assertEquals(Boolean.TRUE, field.get("stored"));

    openPage(COLLECTION + "/schema?field=id", By.id("schema"));
    // the detail header shows the selected field name
    waitForTextContains(By.cssSelector("#schema span.name"), "id");
    // the flags matrix lists these properties for the field
    waitForPageContains("Indexed");
    waitForPageContains("Stored");

    // term info for the populated id field shows the indexed terms
    click(By.xpath("//button[@ng-click='toggleTerms()']"));
    waitForPageContains("doc1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testAddAndDeleteFieldViaUi() throws Exception {
    String fieldName = "ui_added_field";
    openPage(COLLECTION + "/schema", By.id("schema"));

    click(By.id("addField"));
    WebElement nameInput = waitFor(By.id("add_name"));
    nameInput.clear();
    nameInput.sendKeys(fieldName);
    chosenSelect("add_type", "string");
    click(By.xpath("//button[@ng-click='addField()']"));

    waitUntil("field " + fieldName + " should exist in schema", () -> fieldExists(fieldName));

    // delete it again from the field detail view
    openPage(COLLECTION + "/schema?field=" + fieldName, By.id("schema"));
    waitForTextContains(By.cssSelector("#schema span.name"), fieldName);
    click(By.xpath("//dd[contains(@class,'delete-field')]/button"));
    click(By.xpath("//div[contains(@class,'delete')]//button[@ng-click='delete()']"));

    waitUntil("field " + fieldName + " should be gone", () -> !fieldExists(fieldName));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCopyFieldsUseDedicatedEndpoints() throws Exception {
    new SchemaRequest.AddCopyField(COPY_SOURCE, List.of(COPY_DEST_ONE), 100)
        .process(cluster.getSolrClient(), COLLECTION);
    try {
      openPage(COLLECTION + "/schema", By.id("schema"));
      openAddCopyFieldDialog();
      waitFor(By.id("add_source")).sendKeys(COPY_SOURCE);
      waitFor(By.id("add_dest")).sendKeys(COPY_DEST_TWO);
      waitFor(By.id("add_max_chars")).sendKeys("20");
      ((JavascriptExecutor) driver).executeScript("performance.clearResourceTimings();");
      click(By.xpath("//button[@ng-click='addCopyField()']"));

      waitUntil(
          "adding a copy rule should retain the source's existing destinations",
          () -> copyFieldDestinations().equals(List.of(COPY_DEST_ONE, COPY_DEST_TWO)));
      String copyFieldPath = "/api/collections/" + COLLECTION + "/schema/copyfields/" + COPY_SOURCE;
      assertBrowserRequested(copyFieldPath);
      var copyFields =
          new SchemaRequest.CopyFields(params("source.fl", COPY_SOURCE))
              .process(cluster.getSolrClient(), COLLECTION)
              .getCopyFields();
      assertEquals(
          List.of(
              Map.of("source", COPY_SOURCE, "dest", COPY_DEST_ONE, "maxChars", 100),
              Map.of("source", COPY_SOURCE, "dest", COPY_DEST_TWO, "maxChars", 20)),
          copyFields);

      openPage(COLLECTION + "/schema?field=" + COPY_SOURCE, By.id("schema"));
      waitForTextContains(By.cssSelector("#schema span.name"), COPY_SOURCE);
      String destinationRow =
          "//dd[contains(@class,'copyfield')][.//a[normalize-space()='" + COPY_DEST_TWO + "']]";
      click(By.xpath(destinationRow + "//span[@ng-click='toggleDeleteCopyField(field)']"));
      ((JavascriptExecutor) driver).executeScript("performance.clearResourceTimings();");
      click(
          By.xpath(
              destinationRow + "//button[@ng-click='deleteCopyField(field, name, field.name)']"));

      waitUntil(
          "deleting a copy rule should retain the source's other destinations",
          () -> copyFieldDestinations().equals(List.of(COPY_DEST_ONE)));
      assertBrowserRequested(copyFieldPath + "/" + COPY_DEST_TWO);
      assertNoSevereConsoleErrors();
    } finally {
      List<String> destinations = copyFieldDestinations();
      if (!destinations.isEmpty()) {
        new SchemaRequest.DeleteCopyField(COPY_SOURCE, destinations)
            .process(cluster.getSolrClient(), COLLECTION);
      }
    }
  }

  @Test
  public void testDeleteWildcardCopyFieldFromDestination() throws Exception {
    assertDeleteWildcardCopyField("ui_copy_*", COPY_DEST_TWO, COPY_DEST_TWO, COPY_DEST_TWO);
  }

  @Test
  public void testDeleteWildcardCopyFieldFromSource() throws Exception {
    assertDeleteWildcardCopyField("ui_copy_*", COPY_DEST_TWO, COPY_SOURCE, COPY_DEST_TWO);
  }

  @Test
  public void testDeleteCopyFieldToDynamicFieldReference() throws Exception {
    String destination = "ui_copy_target_s";
    new SchemaRequest.AddCopyField(COPY_SOURCE, List.of(destination))
        .process(cluster.getSolrClient(), COLLECTION);
    try {
      openPage(COLLECTION + "/schema?field=" + COPY_SOURCE, By.id("schema"));
      waitForTextContains(By.cssSelector("#schema span.name"), COPY_SOURCE);
      String row =
          "//dd[contains(@class,'copyfield')][.//a[normalize-space()='" + destination + "']]";
      waitFor(By.xpath(row));
      assertEquals(
          "an exact rule to a dynamic-field reference remains deletable",
          1,
          driver
              .findElements(By.xpath(row + "//span[@ng-click='toggleDeleteCopyField(field)']"))
              .size());
      click(By.xpath(row + "//span[@ng-click='toggleDeleteCopyField(field)']"));
      click(By.xpath(row + "//button[starts-with(@ng-click,'deleteCopyField(')]"));
      waitUntil("the exact rule should be deleted", () -> copyFieldDestinations().isEmpty());
      assertNoSevereConsoleErrors();
    } finally {
      if (!copyFieldDestinations().isEmpty()) {
        new SchemaRequest.DeleteCopyField(COPY_SOURCE, List.of(destination))
            .process(cluster.getSolrClient(), COLLECTION);
      }
    }
  }

  private void assertDeleteWildcardCopyField(
      String source, String destination, String selectedField, String displayedDestination)
      throws Exception {
    openPage(COLLECTION + "/schema", By.id("schema"));
    openAddCopyFieldDialog();
    waitFor(By.id("add_source")).sendKeys(source);
    waitFor(By.id("add_dest")).sendKeys(destination);
    click(By.xpath("//button[@ng-click='addCopyField()']"));
    waitUntil(
        "the wildcard rule should exist",
        () -> copyFieldDestinations(source).contains(destination));
    try {
      openPage(COLLECTION + "/schema?field=" + selectedField, By.id("schema"));
      waitForTextContains(By.cssSelector("#schema span.name"), selectedField);
      String destinationRow =
          "//dd[contains(@class,'copyfield')][.//a[normalize-space()='"
              + displayedDestination
              + "']]";
      waitFor(By.xpath(destinationRow));
      assertTrue(
          "expanded wildcard matches must be read-only",
          driver
              .findElements(
                  By.xpath(destinationRow + "//span[@ng-click='toggleDeleteCopyField(field)']"))
              .isEmpty());
      openPage(COLLECTION + "/schema?field=" + destination, By.id("schema"));
      waitForTextContains(By.cssSelector("#schema span.name"), destination);
      String sourceRow =
          "//dd[contains(@class,'copyfield')][.//a[normalize-space()='" + source + "']]";
      click(By.xpath(sourceRow + "//span[@ng-click='toggleDeleteCopyField(field)']"));
      ((JavascriptExecutor) driver).executeScript("performance.clearResourceTimings();");
      click(By.xpath(sourceRow + "//button[starts-with(@ng-click,'deleteCopyField(')]"));
      waitUntil(
          "the wildcard rule should be deleted", () -> copyFieldDestinations(source).isEmpty());
      assertBrowserRequested(
          "/api/collections/" + COLLECTION + "/schema/copyfields/" + source + "/" + destination);
      assertNoSevereConsoleErrors();
    } finally {
      if (!copyFieldDestinations(source).isEmpty()) {
        new SchemaRequest.DeleteCopyField(source, List.of(destination))
            .process(cluster.getSolrClient(), COLLECTION);
      }
    }
  }

  private void assertBrowserRequested(String path) throws Exception {
    waitUntil(
        "the browser should request " + path,
        () ->
            ((List<?>)
                    ((JavascriptExecutor) driver)
                        .executeScript(
                            "return performance.getEntriesByType('resource')"
                                + ".map(entry => new URL(entry.name).pathname);"))
                .contains(path));
  }

  @Test
  public void testCopyFieldRequiresSource() throws Exception {
    openPage(COLLECTION + "/schema", By.id("schema"));
    openAddCopyFieldDialog();
    waitFor(By.id("add_dest")).sendKeys(COPY_DEST_TWO);
    click(By.xpath("//button[@ng-click='addCopyField()']"));

    waitForTextContains(By.cssSelector("div[ng-repeat='error in addCopyFieldErrors']"), "source");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCopyFieldRequiresDestination() throws Exception {
    openPage(COLLECTION + "/schema", By.id("schema"));
    openAddCopyFieldDialog();
    waitFor(By.id("add_source")).sendKeys(COPY_SOURCE);
    click(By.xpath("//button[@ng-click='addCopyField()']"));

    waitForTextContains(
        By.cssSelector("div[ng-repeat='error in addCopyFieldErrors']"), "destination");
    assertNoSevereConsoleErrors();
  }

  private void openAddCopyFieldDialog() throws InterruptedException {
    click(By.id("addCopyField"));
    WebElement source = waitFor(By.id("add_source"));
    waitUntil(
        "the copy-field dialog should focus the source input",
        () -> source.equals(driver.switchTo().activeElement()));
  }

  private List<String> copyFieldDestinations() {
    return copyFieldDestinations(COPY_SOURCE);
  }

  @SuppressWarnings("unchecked")
  private List<String> copyFieldDestinations(String source) {
    try {
      NamedList<Object> response =
          adminApi("/" + COLLECTION + "/schema/copyfields", params("source.fl", source));
      List<Map<?, ?>> rules = (List<Map<?, ?>>) response.get("copyFields");
      return rules.stream().map(rule -> (String) rule.get("dest")).sorted().toList();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private boolean fieldExists(String fieldName) {
    try {
      NamedList<Object> response = adminApi("/" + COLLECTION + "/schema/fields", params());
      List<Map<?, ?>> fields = (List<Map<?, ?>>) response.get("fields");
      return fields.stream().anyMatch(f -> fieldName.equals(f.get("name")));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
