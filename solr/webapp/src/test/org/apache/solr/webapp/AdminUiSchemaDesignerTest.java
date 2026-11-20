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

import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.solr.client.solrj.request.SchemaDesignerApi;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Happy-path test of the Schema Designer screen: create a new schema, paste a sample document and
 * let the designer analyze it.
 *
 * <p>The Analyze action remains disabled until creation of the mutable schema has completed, so a
 * fast user cannot race the prep and analyze requests.
 */
public class AdminUiSchemaDesignerTest extends AdminUiTestBase {

  @Test
  public void testDesignSchemaFromSampleDocument() throws Exception {
    openPage("~schema-designer", By.id("designer"));
    waitUntil(
        "the language choices have loaded",
        () ->
            !driver.findElements(By.cssSelector("#languages option[value='string:en']")).isEmpty());

    // create a new schema via the dialog
    click(By.cssSelector("#designer #add"));
    WebElement schemaName = waitFor(By.id("add_schema"));
    schemaName.clear();
    schemaName.sendKeys("uidesigned");
    click(By.xpath("//button[@ng-click='addSchema()']"));

    // paste a sample document and analyze it
    WebElement sampleDocs = waitFor(By.cssSelector("#sample-docs textarea#document"));
    sampleDocs.clear();
    sampleDocs.sendKeys("[{\"id\":\"1\",\"designer_title\":\"Hello Designer\"}]");
    click(By.cssSelector("#analyze:not([disabled])"));

    waitForPageContains("designer_title");
    assertNoSevereConsoleErrors();

    click(By.cssSelector("#schemaJsTree > ul > li > .jstree-anchor"));
    var languages = waitFor(By.id("languages"));
    for (var option : languages.findElements(By.tagName("option"))) {
      if (option.isSelected() != "French".equals(option.getText())) {
        option.click();
      }
    }
    click(By.cssSelector("#analyze:not([disabled])"));
    waitUntil(
        "the selected language is saved",
        () -> {
          try {
            var info = new SchemaDesignerApi.GetInfo("uidesigned").process(cluster.getSolrClient());
            return List.of("fr").equals(info.languages);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    // add a field through the UI
    click(By.cssSelector("#addField"));
    setText(By.id("add_name"), "extra_test_field");
    click(By.xpath("//button[@ng-click='addField()']"));

    waitForPageContains("extra_test_field");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testLanguageChoicesFollowMultilingualSchema() throws Exception {
    ensureCloudCluster();
    var service = cluster.getJettySolrRunner(0).getCoreContainer().getConfigSetService();
    byte[] original = service.downloadFileFromConfig("_multilingual", "managed-schema.xml");
    String schema =
        new String(original, StandardCharsets.UTF_8)
            .replace(
                "</schema>",
                "<fieldType name=\"text_uk\" class=\"solr.TextField\">"
                    + "<analyzer><tokenizer name=\"standard\"/></analyzer></fieldType></schema>");
    try {
      service.uploadFileToConfig(
          "_multilingual", "managed-schema.xml", schema.getBytes(StandardCharsets.UTF_8), true);
      var choices = new SchemaDesignerApi.ListDesignerConfigs().process(cluster.getSolrClient());
      String label =
          choices.availableLanguages.stream()
              .filter(language -> "uk".equals(language.code))
              .findFirst()
              .orElseThrow()
              .name;

      openPage("~schema-designer", By.id("designer"));
      waitUntil(
          "the new schema language appears in the selector",
          () ->
              driver.findElements(By.cssSelector("#languages option")).stream()
                  .anyMatch(option -> label.equals(option.getDomProperty("textContent"))));
      assertNoSevereConsoleErrors();
    } finally {
      service.uploadFileToConfig("_multilingual", "managed-schema.xml", original, true);
    }
  }

  @Test
  public void testLanguageSelectionDisabledWithoutMultilingual() throws Exception {
    ensureCloudCluster();
    var service = cluster.getJettySolrRunner(0).getCoreContainer().getConfigSetService();
    String saved = "savedMultilingualForUi";
    service.copyConfig("_multilingual", saved);
    service.deleteConfig("_multilingual");
    try {
      openPage("~schema-designer", By.id("designer"));
      waitUntil(
          "the configset list has loaded",
          () ->
              driver.findElements(By.cssSelector("#copy_schema option")).stream()
                  .anyMatch(option -> saved.equals(option.getDomProperty("textContent"))));
      waitUntil(
          "language selection is disabled",
          () -> !driver.findElements(By.cssSelector("#languages[disabled]")).isEmpty());
      assertNoSevereConsoleErrors();
    } finally {
      service.copyConfig(saved, "_multilingual");
      service.deleteConfig(saved);
    }
  }
}
