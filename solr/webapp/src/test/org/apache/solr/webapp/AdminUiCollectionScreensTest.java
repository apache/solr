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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Verifies the per-collection analysis/files/segments/plugins/overview screens against a fixture
 * collection with indexed documents. Screens with their own write actions have dedicated test
 * classes (query, documents, schema, paramsets).
 */
public class AdminUiCollectionScreensTest extends AdminUiTestBase {

  private static final String COLLECTION = "books";
  private static final int NUM_DOCS = 4;

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
    SolrClient client = cluster.getSolrClient(COLLECTION);
    for (int i = 1; i <= NUM_DOCS; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", Integer.toString(i));
      doc.addField("title_txt", "Book number " + i);
      client.add(doc);
    }
    client.commit();
  }

  @Test
  public void testAnalysisScreenAnalyzesText() {
    openPage(COLLECTION + "/analysis", By.id("analysis-holder"));
    chosenSelect("type_or_name", "text_general");
    WebElement indexText = waitFor(By.id("analysis_fieldvalue_index"));
    indexText.clear();
    indexText.sendKeys("Running QUICKLY");
    click(By.cssSelector("#field-analysis button[type=submit]"));
    // text_general tokenizes and lowercases
    waitForPageContains("running");
    waitForPageContains("quickly");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testFilesScreenShowsConfig() {
    openPage(COLLECTION + "/files", By.id("files"));
    waitForPageContains("solrconfig.xml");
    // open the file and check its content is rendered
    openPage(COLLECTION + "/files?file=solrconfig.xml", By.id("files"));
    waitForPageContains("luceneMatchVersion");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testSegmentsScreenShowsSegments() throws Exception {
    String coreName = coreNameOnNode0(COLLECTION);
    openPage(coreName + "/segments", By.id("segments"));
    waitUntil(
        "at least one segment should render after committing docs",
        () -> !driver.findElements(By.cssSelector("#segments #response li")).isEmpty());
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testPluginsScreenShowsStats() {
    String coreName = coreNameOnNode0(COLLECTION);
    openPage(coreName + "/plugins", By.id("plugins"));
    waitForPageContains("searcher");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCollectionOverviewShowsShard() {
    openPage(COLLECTION + "/collection-overview", By.id("dashboard"));
    waitForPageContains("shard1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCoreOverviewShowsStats() {
    String coreName = coreNameOnNode0(COLLECTION);
    openPage(coreName + "/core-overview", By.id("dashboard"));
    waitForPageContains("Num Docs");
    waitForPageContains(Integer.toString(NUM_DOCS));
    // the ping widget answers 503 when the configset has no healthcheck file
    assertNoSevereConsoleErrors("/admin/ping");
  }
}
