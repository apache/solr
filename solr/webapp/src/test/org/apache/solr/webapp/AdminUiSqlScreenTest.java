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

/** Browser tests for the SQL screen. The test classpath includes the sql module. */
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
    openPage(COLLECTION + "/sqlquery", By.id("sqlquery"));
    WebElement stmt = waitFor(By.id("sqlexp"));
    stmt.clear();
    stmt.sendKeys("SELECT id FROM " + COLLECTION + " LIMIT 10");
    click(By.xpath("//div[@id='sqlquery']//button[@type='submit']"));

    for (int i = 1; i <= 3; i++) {
      waitForPageContains("sql-doc-" + i);
    }
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testFailedRequestShowsErrorInsteadOfCrashing() {
    // A missing collection produces an error response without a result-set.
    openPage("nonexistentcoll/sqlquery", By.id("sqlquery"));
    WebElement stmt = waitFor(By.id("sqlexp"));
    stmt.clear();
    stmt.sendKeys("SELECT id FROM " + COLLECTION + " LIMIT 10");
    click(By.xpath("//div[@id='sqlquery']//button[@type='submit']"));
    waitForTextContains(By.id("sql-response"), "no handler, collection, or core");
    assertNoSevereConsoleErrors("404 (Not Found)");
  }

  @Test
  public void testSqlModuleNotEnabledShowsFriendlyMessage() {
    // Use a captured missing-module response because the test classpath includes the sql module.
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
