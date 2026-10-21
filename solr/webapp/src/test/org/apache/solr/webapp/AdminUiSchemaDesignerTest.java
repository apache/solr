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

    // add a field through the UI
    click(By.cssSelector("#addField"));
    setText(By.id("add_name"), "extra_test_field");
    click(By.xpath("//button[@ng-click='addField()']"));

    waitForPageContains("extra_test_field");
    assertNoSevereConsoleErrors();
  }
}
