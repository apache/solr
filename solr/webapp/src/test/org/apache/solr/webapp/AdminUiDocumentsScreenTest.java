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
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/** Tests the Documents screen: the indexing form and submitting documents through it. */
public class AdminUiDocumentsScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "docscoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testDocumentsScreenForm() {
    openPage(COLLECTION + "/documents", By.id("documents"));
    List<String> types =
        driver.findElements(By.cssSelector("#document-type option")).stream()
            .map(WebElement::getText)
            .collect(Collectors.toList());
    assertTrue("Doc type dropdown should offer JSON, got " + types, types.contains("JSON"));
    assertTrue("Doc type dropdown should offer XML, got " + types, types.contains("XML"));
    assertTrue("Doc type dropdown should offer CSV, got " + types, types.contains("CSV"));
    waitFor(By.id("submit"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testIndexDocumentViaUi() throws Exception {
    openPage(COLLECTION + "/documents", By.id("documents"));
    WebElement docInput = waitFor(By.id("document"));
    docInput.clear();
    docInput.sendKeys("{\"id\":\"ui-doc-1\",\"title_txt\":\"indexed from the admin ui\"}");
    click(By.id("submit"));
    waitForTextContains(By.cssSelector("#documents #result"), "success");

    // the document becomes searchable (the form defaults to commitWithin=1000)
    waitUntil(
        "document indexed via UI should be searchable",
        () -> {
          try {
            return cluster
                    .getSolrClient(COLLECTION)
                    .query(new SolrQuery("id:ui-doc-1"))
                    .getResults()
                    .getNumFound()
                == 1;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
    assertNoSevereConsoleErrors();
  }
}
