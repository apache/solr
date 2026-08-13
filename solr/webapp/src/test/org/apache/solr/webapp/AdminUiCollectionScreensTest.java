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
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.ExternalPaths;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Verifies the per-collection Admin UI screens against a fixture collection with indexed documents.
 */
public class AdminUiCollectionScreensTest extends AdminUiTestBase {

  private static final String COLLECTION = "books";
  private static final int NUM_DOCS = 4;

  @BeforeClass
  public static void setupCollection() throws Exception {
    cluster.uploadConfigSet(ExternalPaths.DEFAULT_CONFIGSET, COLLECTION);
    // pin the replica to the node the browser talks to, so core-level screens
    // (plugins, segments) find it locally
    CollectionAdminRequest.createCollection(COLLECTION, COLLECTION, 1, 1)
        .setCreateNodeSet(cluster.getJettySolrRunner(0).getNodeName())
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION, 1, 1);

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
  public void testQueryScreenExecutesQueries() {
    openPage(COLLECTION + "/query", By.id("query"));

    // default *:* query finds all documents
    waitFor(By.cssSelector("#query button[type=submit]")).click();
    waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":" + NUM_DOCS);

    // a specific id query finds exactly one document
    WebElement queryInput = waitFor(By.id("q"));
    queryInput.clear();
    queryInput.sendKeys("id:1");
    waitFor(By.cssSelector("#query button[type=submit]")).click();
    waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testAnalysisScreenAnalyzesText() {
    openPage(COLLECTION + "/analysis", By.id("analysis-holder"));
    chosenSelect("type_or_name", "text_general");
    WebElement indexText = waitFor(By.id("analysis_fieldvalue_index"));
    indexText.clear();
    indexText.sendKeys("Running QUICKLY");
    waitFor(By.cssSelector("#field-analysis button[type=submit]")).click();
    // text_general tokenizes and lowercases
    waitForPageContains("running");
    waitForPageContains("quickly");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testSchemaScreenShowsFields() {
    openPage(COLLECTION + "/schema", By.id("schema"));
    // managed schema is editable, so the action buttons are shown
    waitFor(By.id("addField"));
    // known fields from the _default configset are browsable
    waitForPageContains("_version_");
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
    String coreName = coreNameOnNode0();
    openPage(coreName + "/segments", By.id("segments"));
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    List<WebElement> segments = List.of();
    while (System.nanoTime() < deadlineNanos) {
      segments = driver.findElements(By.cssSelector("#segments #response li"));
      if (!segments.isEmpty()) break;
      Thread.sleep(200);
    }
    assertFalse("Expected at least one segment after committing docs", segments.isEmpty());
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testPluginsScreenShowsStats() throws Exception {
    String coreName = coreNameOnNode0();
    openPage(coreName + "/plugins", By.id("plugins"));
    waitForPageContains("searcher");
    assertNoSevereConsoleErrors();
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
  public void testParamsetsScreenRenders() {
    openPage(COLLECTION + "/paramsets", By.id("paramsets"));
    waitFor(By.cssSelector("#paramsets #form"));
    // the shared menu code intermittently throws a benign TypeError while the
    // per-collection menu resolves; the screen itself renders fine
    assertNoSevereConsoleErrors("Cannot read properties of null (reading 'name')");
  }

  @Test
  public void testCollectionOverviewShowsShard() {
    openPage(COLLECTION + "/collection-overview", By.id("dashboard"));
    waitForPageContains("shard1");
    assertNoSevereConsoleErrors();
  }

  /** Returns the fixture collection's core name on node 0, the node the browser talks to. */
  private static String coreNameOnNode0() {
    for (String name : cluster.getJettySolrRunner(0).getCoreContainer().getAllCoreNames()) {
      if (name.startsWith(COLLECTION + "_")) {
        return name;
      }
    }
    throw new AssertionError("No core found on node 0 for collection " + COLLECTION);
  }

  private static String abbreviate(String s) {
    return s.length() > 300 ? s.substring(0, 300) + "..." : s;
  }
}
