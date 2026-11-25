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
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Tests the Schema Browser screen: browsing fields and their flags, term info, and adding/deleting
 * a field through the UI dialogs (the fixture uses a mutable managed schema).
 */
public class AdminUiSchemaScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "schemacoll";

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
