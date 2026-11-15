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
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;

/** Tests the Stream screen: executing a streaming expression through the form. */
public class AdminUiStreamScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "streamcoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
    SolrClient client = cluster.getSolrClient(COLLECTION);
    for (int i = 1; i <= 3; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "stream-doc-" + i);
      client.add(doc);
    }
    client.commit();
  }

  @Test
  public void testStreamingExpressionViaUi() {
    openPage(COLLECTION + "/stream", By.id("stream"));
    WebElement expr = waitFor(By.id("expr"));
    expr.clear();
    expr.sendKeys("search(" + COLLECTION + ",q=\"*:*\",fl=\"id\",sort=\"id asc\")");
    click(By.cssSelector("#stream button[type=submit]"));
    String response = waitForTextContains(By.cssSelector("#stream #result"), "stream-doc-1");
    assertTrue("All docs should stream: " + response, response.contains("stream-doc-3"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testLargeExpressionSucceedsViaUi() {
    // A streaming expression large enough that a GET request's URL/header would be rejected by
    // Jetty before ever reaching Solr (SOLR-9759) - a single wildcard clause keeps it one simple
    // query (matching nothing, since no real id starts with this), so a clean zero-hit response
    // (rather than a hang, a truncated request, or a parse error) confirms the whole POST body
    // round-tripped intact.
    String padding = "a".repeat(20000);
    String expression =
        "search(" + COLLECTION + ",q=\"*:*\",fl=\"id\",sort=\"id asc\",fq=\"id:" + padding + "*\")";
    assertTrue(
        "test expression should exceed a typical 8K header/URL limit", expression.length() > 16384);

    openPage(COLLECTION + "/stream", By.id("stream"));
    WebElement expr = waitFor(By.id("expr"));
    ((JavascriptExecutor) driver)
        .executeScript(
            "arguments[0].value = arguments[1];"
                + "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
            expr,
            expression);
    click(By.cssSelector("#stream button[type=submit]"));
    waitForTextContains(By.cssSelector("#stream #result"), "EOF");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testFailedRequestShowsErrorInsteadOfHanging() {
    // a nonexistent collection makes the request fail - confirms a failed request surfaces
    // something in the UI instead of leaving the screen blank forever (the original bug).
    openPage("nonexistentcoll/stream", By.id("stream"));
    WebElement expr = waitFor(By.id("expr"));
    expr.clear();
    expr.sendKeys("search(" + COLLECTION + ",q=\"*:*\",fl=\"id\",sort=\"id asc\")");
    click(By.cssSelector("#stream button[type=submit]"));
    waitForTextContains(By.cssSelector("#stream #result"), "no handler, collection, or core");
    // the request is expected to fail (404) - that's the scenario under test
    assertNoSevereConsoleErrors("404 (Not Found)");
  }
}
