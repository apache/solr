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

import java.util.Map;
import org.apache.lucene.tests.util.LuceneTestCase.Nightly;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebElement;

/**
 * Tests the Admin UI with BasicAuth enabled: the login screen flow and the Security screen,
 * including adding a user through the UI dialog.
 *
 * <p>Nightly: the login/session interplay between the browser and the auth filter is the most
 * timing-sensitive part of the UI test suite.
 */
@Nightly
public class AdminUiSecurityAuthTest extends AdminUiTestBase {

  private static final String USER = "solr";
  private static final String PASS = "SolrRocks";

  static {
    // consumed by AdminUiTestBase when starting the cluster
    securityJson =
        "{\n"
            + "  \"authentication\": {\n"
            + "    \"blockUnknown\": true,\n"
            + "    \"class\": \"solr.BasicAuthPlugin\",\n"
            + "    \"credentials\": {\"solr\": \"IV0EHq1OnNrj6gvRCwvFwTrZ1+z1oBbnQdiVC3otuq0="
            + " Ndd7LKvVBAaZIF0QAVi1ekCfAJXr1GGfLtRUXhgrF8c=\"}\n"
            + "  },\n"
            + "  \"authorization\": {\n"
            + "    \"class\": \"solr.RuleBasedAuthorizationPlugin\",\n"
            + "    \"permissions\": [{\"name\": \"security-edit\", \"role\": \"admin\"},\n"
            + "                      {\"name\": \"all\", \"role\": \"admin\"}],\n"
            + "    \"user-role\": {\"solr\": \"admin\"}\n"
            + "  }\n"
            + "}";
  }

  @Test
  public void testLoginAndSecurityScreen() throws Exception {
    // an unauthenticated visit is redirected to the login screen
    driver.get(baseUrl + "/index.html#/");
    waitFor(By.id("login"));
    WebElement username = waitFor(By.id("username"));
    username.clear();
    username.sendKeys(USER);
    WebElement password = waitFor(By.id("password"));
    password.clear();
    password.sendKeys(PASS);
    waitFor(By.xpath("//div[@id='login']//button[@type='submit']")).click();

    // after login the dashboard loads and shows the authenticated security info
    waitFor(By.id("index"));
    waitForPageContains("BasicAuthPlugin");

    // the security screen shows the configured plugins and users
    openPage("~security", By.id("securityPanel"));
    waitForPageContains("BasicAuthPlugin");
    waitForPageContains("RuleBasedAuthorizationPlugin");
    waitForPageContains(USER);

    // add a user through the dialog. The dialog is driven via the controller scope:
    // native clicks/keystrokes into this absolutely-positioned dialog proved unreliable
    // in headless mode, and per-keystroke entry is already covered by the login form.
    String newUser = "uitestuser";
    String newUserPass = "Uitest!Pass99";
    waitFor(By.id("add-user"));
    ((JavascriptExecutor) driver)
        .executeScript(
            "var scope = angular.element(document.getElementById('add-user')).scope();"
                + " scope.showAddUserDialog(); scope.$apply();");
    waitFor(By.id("add_user"));
    ((JavascriptExecutor) driver)
        .executeScript(
            "var scope = angular.element(document.getElementById('add_user')).scope();"
                + " scope.upsertUser = {username: arguments[0], password: arguments[1],"
                + " password2: arguments[1]};"
                + " scope.doUpsertUser(); scope.$apply();",
            newUser,
            newUserPass);

    waitUntil("user " + newUser + " should exist", () -> userExists(newUser));
    // the users list refreshes to include the new user
    waitForPageContains(newUser);
  }

  /** Checks via the authentication API (with credentials) whether the user exists. */
  private boolean userExists(String user) {
    try (SolrClient client = cluster.getJettySolrRunner(0).newClient()) {
      GenericSolrRequest req =
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/authentication", params());
      req.setBasicAuthCredentials(USER, PASS);
      NamedList<Object> response = client.request(req);
      Map<?, ?> authentication = (Map<?, ?>) response.get("authentication");
      Map<?, ?> credentials = (Map<?, ?>) authentication.get("credentials");
      return credentials.containsKey(user);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
