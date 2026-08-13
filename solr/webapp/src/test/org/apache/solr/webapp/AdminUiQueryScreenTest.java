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
  public void testRowsAndFieldListParameters() {
    openPage(COLLECTION + "/query", By.id("query"));

    WebElement rows = waitFor(By.id("rows"));
    rows.clear();
    rows.sendKeys("2");
    WebElement fl = waitFor(By.id("fl"));
    fl.clear();
    fl.sendKeys("id");
    waitFor(By.cssSelector("#query button[type=submit]")).click();

    String response =
        waitForTextContains(By.cssSelector("#query #response"), "\"numFound\":" + NUM_DOCS);
    // only two docs are returned, and only their id field
    assertEquals("Expected 2 returned docs: " + response, 2, countOccurrences(response, "\"id\":"));
    assertFalse("fl=id should exclude other fields: " + response, response.contains("title_txt"));
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
