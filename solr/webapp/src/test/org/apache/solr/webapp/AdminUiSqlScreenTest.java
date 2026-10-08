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

import java.util.Locale;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;

/**
 * Tests the SQL screen: executing a SQL query through the form. Requires the sql module on the
 * server classpath, provided by the webapp test dependencies.
 */
public class AdminUiSqlScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "sqlcoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
    SolrClient client = cluster.getSolrClient(COLLECTION);
    for (int i = 1; i <= 3; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "sql-doc-" + i);
      client.add(doc);
    }
    client.commit();
  }

  @Test
  public void testSqlQueryViaUi() {
    assertSqlQueryViaUi("POST", "SELECT id FROM " + COLLECTION + " LIMIT 10");
  }

  @Test
  public void testSqlQueryViaQuery() {
    assertSqlQueryViaUi("QUERY", "SELECT id FROM " + COLLECTION + " LIMIT 10");
  }

  @Test
  public void testSqlQueryViaGet() {
    assertSqlQueryViaUi("GET", "SELECT id FROM " + COLLECTION + " LIMIT 10");
  }

  @Test
  public void testLargeSqlStatementViaPost() {
    assertSqlQueryViaUi("POST", largeStatement());
  }

  @Test
  public void testLargeSqlStatementViaQuery() {
    assertSqlQueryViaUi("QUERY", largeStatement());
  }

  private String largeStatement() {
    return "SELECT /* " + "a".repeat(20000) + " */ id FROM " + COLLECTION + " LIMIT 10";
  }

  private void assertSqlQueryViaUi(String method, String statement) {
    openPage(COLLECTION + "/sqlquery", By.id("sqlquery"));
    WebElement selector = waitFor(By.id("httpMethod"));
    assertEquals("POST", selector.getDomProperty("value"));
    selector.findElement(By.cssSelector("option[value='" + method + "']")).click();
    ((JavascriptExecutor) driver)
        .executeScript(
            "window.sqlRequest = null;"
                + "var originalOpen = XMLHttpRequest.prototype.open;"
                + "var originalSend = XMLHttpRequest.prototype.send;"
                + "XMLHttpRequest.prototype.open = function(method, url) {"
                + "  if (/\\/sql(?:\\?|$)/.test(url)) {"
                + "    this.sqlRequest = window.sqlRequest = {method: method, url: url};"
                + "  }"
                + "  return originalOpen.apply(this, arguments);"
                + "};"
                + "XMLHttpRequest.prototype.send = function(body) {"
                + "  if (this.sqlRequest) this.sqlRequest.body = body;"
                + "  return originalSend.apply(this, arguments);"
                + "};");
    WebElement stmt = waitFor(By.id("sqlexp"));
    ((JavascriptExecutor) driver)
        .executeScript(
            "arguments[0].value = arguments[1];"
                + "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
            stmt,
            statement);
    click(By.xpath("//div[@id='sqlquery']//button[@type='submit']"));

    // the result grid lists all documents
    for (int i = 1; i <= 3; i++) {
      waitForPageContains("sql-doc-" + i);
    }
    assertEquals(
        method, ((JavascriptExecutor) driver).executeScript("return window.sqlRequest.method;"));
    if ("GET".equals(method)) {
      assertNull(((JavascriptExecutor) driver).executeScript("return window.sqlRequest.body;"));
      assertEquals(
          statement,
          ((JavascriptExecutor) driver)
              .executeScript(
                  "return new URL(window.sqlRequest.url, window.location.href).searchParams.get('stmt');"));
    } else {
      assertEquals(
          "stmt=" + statement,
          ((JavascriptExecutor) driver)
              .executeScript("return decodeURIComponent(window.sqlRequest.body);"));
      assertEquals(
          false,
          ((JavascriptExecutor) driver)
              .executeScript("return window.sqlRequest.url.includes('stmt=');"));
    }
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testFailedRequestShowsErrorInsteadOfCrashing() {
    // a nonexistent collection makes the request fail with a response that has no "result-set"
    // key - the same shape the server returns when the sql module/handler isn't installed
    // (SOLR-16640). Before the fix, parsing this crashed with an uncaught TypeError and the
    // screen just stayed blank with no explanation.
    openPage("nonexistentcoll/sqlquery", By.id("sqlquery"));
    WebElement stmt = waitFor(By.id("sqlexp"));
    stmt.clear();
    stmt.sendKeys("SELECT id FROM " + COLLECTION + " LIMIT 10");
    click(By.xpath("//div[@id='sqlquery']//button[@type='submit']"));
    waitForTextContains(By.id("sql-response"), "no handler, collection, or core");
    // the request is expected to fail (404) - that's the scenario under test
    assertNoSevereConsoleErrors("404 (Not Found)");
  }

  @Test
  public void testSqlModuleNotEnabledShowsFriendlyMessage() {
    // The real response when the sql module isn't on the classpath (reproduced against an
    // actual build without the module): a 500 with this exact error envelope shape, since /sql
    // is always nominally registered (lazily) for every core regardless of whether the module
    // jar is present - only the first real request reveals the class is missing. Invoking
    // showResult() directly with this captured shape, rather than needing a real sql-less
    // build, since the webapp test classpath always has the module.
    openPage(COLLECTION + "/sqlquery", By.id("sqlquery"));
    waitFor(By.id("sqlexp"));
    String classNotFoundResponse =
        "{\"error\":{\"metadata\":{\"error-class\":\"org.apache.solr.common.SolrException\","
            + "\"root-error-class\":\"java.lang.ClassNotFoundException\"},"
            + "\"errorClass\":\"org.apache.solr.common.SolrException\","
            + "\"msg\":\" Error loading class 'solr.SQLHandler'\",\"code\":500}}";
    String sqlError =
        (String)
            ((JavascriptExecutor) driver)
                .executeScript(
                    "var scope = angular.element(document.getElementById('sqlquery')).scope();"
                        + "scope.showResult(arguments[0]);"
                        + "scope.$apply();"
                        + "return scope.sqlError;",
                    classNotFoundResponse);
    assertTrue(
        "should show a friendly message, got: " + sqlError,
        sqlError != null && sqlError.toLowerCase(Locale.ROOT).contains("sql module"));
    assertTrue(
        "should say how to fix it: " + sqlError,
        sqlError.toLowerCase(Locale.ROOT).contains("enable"));
    WebElement documentation = waitFor(By.cssSelector("#sql-response span a"));
    assertEquals(
        "https://solr.apache.org/guide/solr/latest/query-guide/sql-query.html",
        documentation.getDomAttribute("href"));
    assertEquals("_out", documentation.getDomAttribute("target"));
  }
}
