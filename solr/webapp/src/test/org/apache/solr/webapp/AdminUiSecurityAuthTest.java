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
import java.util.Map;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;

/** Tests login, security management, and API Explorer requests with BasicAuth enabled. */
public class AdminUiSecurityAuthTest extends AdminUiTestBase {

  private static final String USER = "solr";
  private static final String PASS = "SolrRocks";

  @BeforeClass
  public static void setSecurityConfig() {
    // consumed by AdminUiTestBase when the cluster starts lazily on first use
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
    openPage("", By.id("login"));
    setText(By.id("username"), USER);
    setText(By.id("password"), PASS);
    click(By.xpath("//div[@id='login']//button[@type='submit']"));

    waitFor(By.id("nodes-table"));
    waitFor(By.id("menu-section-cluster"));

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

    // add a role for the new user through the role dialog
    String newRole = "uitestrole";
    waitFor(By.id("add-role"));
    ((JavascriptExecutor) driver)
        .executeScript(
            "var scope = angular.element(document.getElementById('add-role')).scope();"
                + " scope.showAddRoleDialog();"
                + " scope.upsertRole = {name: arguments[0], selectedUsers: [arguments[1]]};"
                + " scope.doUpsertRole(); scope.$apply();",
            newRole,
            newUser);
    waitUntil(
        "user " + newUser + " should have role " + newRole,
        () -> authorizationApi().toString().contains(newRole));
    waitForPageContains(newRole);

    // grant a predefined permission to the role through the permission dialog
    String permission = "collection-admin-read";
    waitFor(By.id("add-permission"));
    ((JavascriptExecutor) driver)
        .executeScript(
            "var scope = angular.element(document.getElementById('add-permission')).scope();"
                + " scope.showAddPermDialog();"
                + " scope.selectedPredefinedPermission = arguments[0];"
                + " scope.upsertPerm.selectedRoles = [arguments[1]];"
                + " scope.doUpsertPermission(); scope.$apply();",
            permission,
            newRole);
    waitUntil(
        "permission " + permission + " should be granted to " + newRole,
        () -> permissionRole(permission).contains(newRole));
    waitForPageContains(permission);
  }

  @Test
  public void testApiExplorerUsesAdminCredentials() throws Exception {
    ensureCloudCluster();
    driver.get(baseUrl + "/index.html#/");
    ((JavascriptExecutor) driver).executeScript("sessionStorage.clear();");
    openPage("", By.id("login"));
    setText(By.id("username"), USER);
    setText(By.id("password"), PASS);
    click(By.xpath("//div[@id='login']//button[@type='submit']"));
    waitFor(By.id("nodes-table"));

    try {
      driver.get(baseUrl + "/index.html#/~api-explorer");
      driver.switchTo().frame(waitFor(By.cssSelector("#content #api-explorer iframe")));
      By collectionsGroup =
          By.xpath(
              "//button[@aria-expanded and .//div[contains(@class,'group/button-label')"
                  + " and normalize-space(.)='collections']]");
      By listCollections =
          By.xpath(
              "//*[self::a or self::button][.//div[contains(@class,'group/button-label')"
                  + " and contains(., 'List all collections in this Solr cluster')]]");
      if ("false".equals(waitFor(collectionsGroup).getDomAttribute("aria-expanded"))) {
        activateExplorerControl(collectionsGroup);
      }
      activateExplorerControl(listCollections);
      waitForPageContains("List all collections in this Solr cluster");
      // Observe the actual request and response without replacing the Solr endpoint.
      // Store only whether the credential matches, never the credential itself.
      ((JavascriptExecutor) driver)
          .executeScript(
              "const original = window.fetch;"
                  + " window.fetch = async function(input, init) {"
                  + " const request = input instanceof Request ? input : new Request(input, init);"
                  + " const response = await original.call(this, input, init);"
                  + " if (new URL(request.url).pathname === '/api/collections'"
                  + " && request.method === 'GET') {"
                  + " let collections = false;"
                  + " try { collections = Array.isArray((await response.clone().json()).collections); }"
                  + " catch (ignored) {}"
                  + " const auth = sessionStorage.getItem('auth.header');"
                  + " window.explorerResult = {status: response.status, collections: collections,"
                  + " authenticated: !!auth && request.headers.get('Authorization') === auth,"
                  + " requestedWith: request.headers.get('X-Requested-With')};"
                  + " } return response; };");
      activateExplorerControl(
          By.xpath(
              "//button[contains(@class,'show-api-client-button') and @method='get'"
                  + " and contains(., '(get /collections)')]"));
      activateExplorerControl(
          By.xpath(
              "//div[contains(@class,'scalar-address-bar')]//button"
                  + "[.//span[contains(., 'Send get request')]]"));
      waitUntil(
          "the explorer should receive the collections response",
          () ->
              Boolean.TRUE.equals(
                  ((JavascriptExecutor) driver).executeScript("return !!window.explorerResult;")));
      @SuppressWarnings("unchecked")
      Map<String, Object> result =
          (Map<String, Object>)
              ((JavascriptExecutor) driver).executeScript("return window.explorerResult;");
      assertEquals(200L, result.get("status"));
      assertEquals(Boolean.TRUE, result.get("collections"));
      assertEquals(Boolean.TRUE, result.get("authenticated"));
      assertEquals("XMLHttpRequest", result.get("requestedWith"));
      waitForTextContains(By.cssSelector("body"), "200 OK");
      driver.switchTo().defaultContent();
      driver.get(baseUrl + "/index.html#/~cloud?view=nodes");
      waitFor(By.id("nodes-table"));
    } finally {
      driver.switchTo().defaultContent();
      ((JavascriptExecutor) driver).executeScript("sessionStorage.clear();");
    }
    driver.get("about:blank");
  }

  private void activateExplorerControl(By locator) {
    ((JavascriptExecutor) driver).executeScript("arguments[0].click();", waitFor(locator));
  }

  /** Returns the authorization config as fetched with credentials. */
  private NamedList<Object> authorizationApi() {
    try {
      GenericSolrRequest req =
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/authorization", params());
      req.setBasicAuthCredentials(USER, PASS);
      return cluster.getJettySolrRunner(0).getSolrClient().request(req);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Returns the roles granted the named permission, as a string, or empty when absent. */
  @SuppressWarnings("unchecked")
  private String permissionRole(String permission) {
    Map<?, ?> authorization = (Map<?, ?>) authorizationApi().get("authorization");
    for (Map<?, ?> perm : (List<Map<?, ?>>) authorization.get("permissions")) {
      if (permission.equals(perm.get("name")) && perm.get("role") != null) {
        return perm.get("role").toString();
      }
    }
    return "";
  }

  /** Checks via the authentication API (with credentials) whether the user exists. */
  private boolean userExists(String user) {
    try {
      GenericSolrRequest req =
          new GenericSolrRequest(SolrRequest.METHOD.GET, "/admin/authentication", params());
      req.setBasicAuthCredentials(USER, PASS);
      NamedList<Object> response = cluster.getJettySolrRunner(0).getSolrClient().request(req);
      Map<?, ?> authentication = (Map<?, ?>) response.get("authentication");
      Map<?, ?> credentials = (Map<?, ?>) authentication.get("credentials");
      return credentials.containsKey(user);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
