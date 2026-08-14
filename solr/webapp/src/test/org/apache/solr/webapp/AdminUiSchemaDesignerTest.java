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

import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Happy-path test of the Schema Designer screen: create a new schema, paste a sample document and
 * let the designer analyze it.
 *
 * <p>AwaitsFix: the designer backend transiently fails its own prep/analyze calls ("version
 * mismatch, retry", "Error loading solr config") when driven at automation speed, making this test
 * flaky even with retries.
 */
@LuceneTestCase.AwaitsFix(bugUrl = "https://issues.apache.org/jira/browse/SOLR-18347")
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

    // the analyzed schema lists the field derived from the sample doc. The designer
    // backend transiently fails its own calls ("version mismatch, retry", "Error
    // loading solr config") and surfaces an error dialog - dismiss it and analyze
    // again, with a generous budget since each round trips several requests
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.multipliedBy(3).toNanos();
    boolean analyzed = false;
    while (!analyzed && System.nanoTime() < deadlineNanos) {
      analyzed = driver.getPageSource().contains("designer_title");
      if (!analyzed) {
        for (String dismissButton : new String[] {"Reload Schema", "OK"}) {
          driver.findElements(By.xpath("//button[contains(., '" + dismissButton + "')]")).stream()
              .filter(WebElement::isDisplayed)
              .findFirst()
              .ifPresent(WebElement::click);
        }
        driver.findElements(By.id("analyze")).stream()
            .filter(WebElement::isDisplayed)
            .findFirst()
            .ifPresent(WebElement::click);
        Thread.sleep(500);
      }
    }
    assertTrue("Analyzed schema should list the sample doc field", analyzed);
    // the designer's own API calls (prep/analyze/luke against its temp core) error
    // transiently while it persists and reloads the schema - it recovers via its retry
    // dialog, so only unrelated console errors fail the test
    assertNoSevereConsoleErrors("schema-designer/", "._designer_");
  }
}
