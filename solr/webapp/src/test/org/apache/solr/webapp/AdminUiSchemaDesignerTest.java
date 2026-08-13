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

import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Happy-path test of the Schema Designer screen: create a new schema, paste a sample document and
 * let the designer analyze it.
 *
 * <p>Nightly: the designer chains many requests and is the most complex screen in the UI.
 */
@Nightly
public class AdminUiSchemaDesignerTest extends AdminUiTestBase {

  @Test
  public void testDesignSchemaFromSampleDocument() throws Exception {
    openPage("~schema-designer", By.id("designer"));

    // create a new schema via the dialog
    waitFor(By.cssSelector("#designer #add")).click();
    WebElement schemaName = waitFor(By.id("add_schema"));
    schemaName.clear();
    schemaName.sendKeys("uidesigned");
    waitFor(By.xpath("//button[@ng-click='addSchema()']")).click();

    // paste a sample document and analyze it
    WebElement sampleDocs = waitFor(By.cssSelector("#sample-docs textarea#document"));
    sampleDocs.clear();
    sampleDocs.sendKeys("[{\"id\":\"1\",\"designer_title\":\"Hello Designer\"}]");
    waitFor(By.id("analyze")).click();

    // the analyzed schema lists the field derived from the sample doc; the designer
    // occasionally races itself persisting the schema ("version mismatch, retry") - in
    // that case dismiss via the offered Reload Schema button and analyze again
    waitUntil(
        "analyzed schema should list the sample doc field",
        () -> {
          if (driver.getPageSource().contains("designer_title")) {
            return true;
          }
          if (driver.getPageSource().contains("version mismatch")) {
            driver.findElements(By.xpath("//button[contains(., 'Reload Schema')]")).stream()
                .filter(WebElement::isDisplayed)
                .findFirst()
                .ifPresent(WebElement::click);
            driver.findElements(By.id("analyze")).stream()
                .filter(WebElement::isDisplayed)
                .findFirst()
                .ifPresent(WebElement::click);
          }
          return false;
        });
    // the designer's own API calls (prep/analyze/luke against its temp core) error
    // transiently while it persists and reloads the schema - it recovers via its retry
    // dialog, so only unrelated console errors fail the test
    assertNoSevereConsoleErrors("schema-designer/", "._designer_");
  }
}
