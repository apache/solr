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
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/** Tests the Query screen: executing queries through the form and its parameter fields. */
public class AdminUiQueryScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "querycoll";
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
  public void testQueryScreenExecutesQueries() {
    openPage(COLLECTION + "/query", By.id("query"));

    // default *:* query finds all documents
    click(By.cssSelector("#query button[type=submit]"));
    waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":" + NUM_DOCS);

    // a specific id query finds exactly one document
    WebElement queryInput = waitFor(By.id("q"));
    queryInput.clear();
    queryInput.sendKeys("id:1");
    click(By.cssSelector("#query button[type=submit]"));
    waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":1");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testRowsAndFieldListParameters() {
    openPage(COLLECTION + "/query", By.id("query"));

    WebElement rows = waitFor(By.id("rows"));
    rows.clear();
    rows.sendKeys("2");
    WebElement fl = waitFor(By.id("fl"));
    fl.clear();
    fl.sendKeys("id");
    click(By.cssSelector("#query button[type=submit]"));

    String response =
        waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":" + NUM_DOCS);
    // only two docs are returned, and only their id field
    assertEquals("Expected 2 returned docs: " + response, 2, countOccurrences(response, "\"id\":"));
    assertFalse("fl=id should exclude other fields: " + response, response.contains("title_txt"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testParamsetDropdown() throws Exception {
    // create a paramset via the API, then apply it through the useParams dropdown
    String paramset = "uiqueryparams";
    GenericSolrRequest setParams =
        new GenericSolrRequest(
            SolrRequest.METHOD.POST, "/" + COLLECTION + "/config/params", params());
    setParams.setContentWriter(
        new RequestWriter.StringPayloadContentWriter(
            "{\"set\":{\"" + paramset + "\":{\"rows\":\"2\"}}}", CommonParams.JSON_MIME));
    try (SolrClient client = cluster.getJettySolrRunner(0).newClient()) {
      client.request(setParams);
    }

    openPage(COLLECTION + "/query", By.id("query"));
    chosenSelect("useParams", paramset);
    click(By.cssSelector("#query button[type=submit]"));

    String response =
        waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":" + NUM_DOCS);
    assertEquals(
        "Paramset rows=2 should limit returned docs: " + response,
        2,
        countOccurrences(response, "\"id\":"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testEdismaxToggle() {
    openPage(COLLECTION + "/query", By.id("query"));

    // the qf field only shows once a dismax parser is selected
    assertFalse(
        "qf should be hidden for the default parser",
        driver.findElement(By.id("qf")).isDisplayed());
    waitFor(By.id("defType")).sendKeys("edismax");
    WebElement qf = waitFor(By.id("qf"));
    qf.sendKeys("title_txt");
    WebElement queryInput = waitFor(By.id("q"));
    queryInput.clear();
    queryInput.sendKeys("number");
    click(By.cssSelector("#query button[type=submit]"));

    // all fixture docs match "number" in title_txt via the edismax qf
    String response =
        waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":" + NUM_DOCS);
    assertTrue("Response should echo defType", response.contains("edismax"));

    // the edismax-only uf field is offered, dismax hides it again
    assertTrue("uf should show for edismax", driver.findElement(By.id("uf")).isDisplayed());
    waitFor(By.id("defType")).sendKeys("dismax");
    assertFalse("uf should hide for dismax", driver.findElement(By.id("uf")).isDisplayed());
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testRawQueryParameters() {
    openPage(COLLECTION + "/query", By.id("query"));

    waitFor(By.cssSelector("#custom_parameters input[name=rawParamQuery]")).sendKeys("fq=id:2");
    click(By.cssSelector("#query button[type=submit]"));

    String response = waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":1");
    assertTrue("The raw fq param should be echoed: " + response, response.contains("id:2"));
    assertNoSevereConsoleErrors();
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) >= 0) {
      count++;
      idx += needle.length();
    }
    return count;
  }
}
