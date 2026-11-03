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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;

/** Tests menu refresh ordering with the bundled Angular/Chosen UI and controlled API callbacks. */
public class AdminUiMenuRefreshTest extends AdminUiTestBase {

  private static final String BOOKS = "{\"collections\":[\"books\"]}";
  private static final String MAGAZINES = "{\"collections\":[\"magazines\"]}";
  private static final String NO_ALIASES = "{\"aliases\":{},\"properties\":{}}";
  private static final String CLOUD_SYSTEM =
      "{\"mode\":\"solrcloud\",\"security\":{\"permissions\":[],\"authenticationPlugin\":null}}";

  @Before
  public void prepareMenuController() {
    openPage("", By.id("index"));
    ((JavascriptExecutor) driver)
        .executeScript(
            """
            var injector = angular.element(document.body).injector();
            var t = window.menuRefreshTest = {
              requests: {cores: [], system: [], collections: [], aliases: []},
              errors: [], urls: [], route: {current: {params: {core: 'books'}}}
            };
            t.scope = injector.get('$rootScope').$new(true);
            t.constants = injector.get('Constants');
            t.timeout = injector.get('$timeout');
            injector.get('$controller')('MainController', {
              $scope: t.scope,
              $route: t.route,
              $location: {url: function(url) { t.urls.push(url); }},
              CoresV2: {getAllCoreStatus: function(params, callback) {
                t.requests.cores.push(callback);
              }},
              SystemV2: {getNodeSystemInfo: function(params, callback) {
                t.requests.system.push(callback);
              }},
              CollectionsV2: {listCollections: function(params, callback) {
                t.requests.collections.push(callback);
              }},
              AliasesV2: {getAliases: function(callback) {
                t.requests.aliases.push(callback);
              }},
              ApiErrorHandler: {handle: function(response) { t.errors.push(response); }}
            });
            // Reuse the production selector's attributes, including ng-change and Chosen.
            var selector = document.querySelector('#collection-selector select').cloneNode(false);
            selector.id = 'menu_refresh_selector';
            selector.removeAttribute('style');
            // The landing-page selector is disabled before its SolrCloud data has loaded.
            selector.removeAttribute('disabled');
            t.fixture = angular.element('<div id="menu-refresh-fixture"'
              + ' style="position:fixed;top:0;left:0;width:500px;background:white;z-index:10000"></div>');
            t.fixture.append(selector);
            angular.element(document.body).append(t.fixture);
            injector.get('$compile')(selector)(t.scope);
            t.scope.$apply(function() {
              t.initialSelection = {name: 'books', type: 'collection'};
              t.scope.collections = [t.initialSelection];
              t.scope.aliases_and_collections = t.scope.collections;
              t.scope.currentCollection = t.initialSelection;
            });
            """);
    assertSelectionText("books");
  }

  @After
  public void removeMenuController() {
    script(
        """
        if (t) {
          t.scope.$destroy();
          t.fixture.remove();
          delete window.menuRefreshTest;
        }
        """);
  }

  @Test
  public void testSelectionSurvivesRefreshAndClearsWhenRemoved() {
    startRefresh("books");
    assertState("t.scope.currentCollection === t.initialSelection");
    reply("system", 0, CLOUD_SYSTEM);
    assertState("t.requests.collections.length === 1");
    assertSelectionText("books");
    assertState("t.scope.currentCollection === t.initialSelection");
    reply("collections", 0, BOOKS);
    assertState("t.requests.aliases.length === 1");
    assertSelectionText("books");
    assertState("t.scope.currentCollection === t.initialSelection");
    reply("aliases", 0, NO_ALIASES);
    assertState("t.scope.currentCollection === t.scope.collections[0]");
    assertState("t.scope.currentCollection !== t.initialSelection");
    assertSelectionText("books");
    assertState("t.urls.length === 0");

    startRefresh("books");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 1, "{\"collections\":[]}");
    reply("aliases", 1, NO_ALIASES);
    assertState("!t.scope.currentCollection && t.scope.collections.length === 0");
    assertState("t.urls.length === 0");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testFailedRefreshPreservesSelection() {
    startRefresh("books");
    reply("system", 0, CLOUD_SYSTEM);
    replyError("collections", 0);
    assertState("t.errors.length === 1 && t.scope.currentCollection === t.initialSelection");
    assertSelectionText("books");

    startRefresh("books");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 1, BOOKS);
    replyError("aliases", 0);
    assertState("t.errors.length === 2 && t.scope.currentCollection === t.initialSelection");
    assertState("t.scope.aliases_and_collections[0] === t.initialSelection");
    assertSelectionText("books");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testRootPageRefreshClearsSelectionAfterLoading() {
    script("t.scope.resetMenu('index', t.constants.IS_ROOT_PAGE);");
    reply("system", 0, CLOUD_SYSTEM);
    reply("collections", 0, BOOKS);
    assertState("t.scope.currentCollection === t.initialSelection");
    reply("aliases", 0, NO_ALIASES);
    assertState("!t.scope.currentCollection && t.scope.collections[0].name === 'books'");
    assertState("t.urls.length === 0");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testSelectorNavigationAndEmptyInput() {
    startRefresh("books");
    reply("system", 0, CLOUD_SYSTEM);
    reply("collections", 0, BOOKS);
    reply("aliases", 0, "{\"aliases\":{\"shelf\":\"books\"},\"properties\":{}}");
    assertSelectionText("books");
    chosenSelect("menu_refresh_selector", "shelf");
    assertState("t.urls.length === 1 && t.urls[0] === '/shelf/alias-overview'");
    chosenSelect("menu_refresh_selector", "books");
    assertState("t.urls.length === 2 && t.urls[1] === '/books/collection-overview'");
    script("t.scope.showCollection(); t.scope.showCollection(null);");
    assertState("t.urls.length === 2");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testOlderAliasResponseCannotReplaceNewerSelection() {
    startRefresh("books");
    reply("system", 0, CLOUD_SYSTEM);
    reply("collections", 0, BOOKS);
    startRefresh("magazines");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 1, MAGAZINES);
    reply("aliases", 1, NO_ALIASES);
    assertSelectionText("magazines");
    reply("aliases", 0, "{\"aliases\":{\"old\":\"books\"},\"properties\":{}}");
    assertState("t.scope.currentCollection.name === 'magazines'");
    assertState("t.scope.collections[0].name === 'magazines' && t.scope.aliases.length === 0");
    assertState("t.urls.length === 0");
    assertSelectionText("magazines");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testOlderCollectionResponseDoesNotStartAliasRequest() {
    startRefresh("books");
    reply("system", 0, CLOUD_SYSTEM);
    startRefresh("magazines");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 1, MAGAZINES);
    reply("aliases", 0, NO_ALIASES);
    reply("collections", 0, BOOKS);
    assertState("t.requests.aliases.length === 1");
    assertState("t.scope.currentCollection.name === 'magazines'");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testObsoleteSystemCoreAndRouteChangeCallbacksAreIgnored() {
    startRefresh("books");
    startRefresh("magazines");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 0, MAGAZINES);
    reply("aliases", 0, NO_ALIASES);
    reply("cores", 1, "{\"status\":{\"new\":{\"name\":\"new\"}},\"initFailures\":{}}");
    reply("system", 0, CLOUD_SYSTEM.replace("solrcloud", "std"));
    replyError("cores", 0);
    assertState("!!t.scope.isCloudEnabled && t.requests.collections.length === 1");
    assertState("t.scope.cores[0].name === 'new' && t.errors.length === 0");

    startRefresh("books");
    script("t.scope.$broadcast('$routeChangeStart');");
    reply("system", 2, CLOUD_SYSTEM);
    reply("cores", 2, "{\"status\":{},\"initFailures\":{}}");
    assertState("t.requests.collections.length === 1 && t.scope.cores[0].name === 'new'");
    assertState("t.scope.currentCollection.name === 'magazines'");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testAliasOverviewLoadsAndTracksRefreshedDetails() {
    script("t.scope.$apply(function() { delete t.scope.currentCollection; });");
    loadAliasOverview("shelf");
    assertState("!t.aliasScope.selectedCollection");
    reply("system", 0, CLOUD_SYSTEM);
    reply("collections", 0, BOOKS);
    reply(
        "aliases",
        0,
        "{\"aliases\":{\"shelf\":\"books\"},\"properties\":{\"shelf\":{\"category\":\"old\"}}}");
    assertState("t.aliasScope.selectedCollection === t.scope.currentCollection");
    waitForTextContains(By.cssSelector("#menu-refresh-fixture h2"), "Alias: shelf");
    waitForTextContains(By.cssSelector("#menu-refresh-fixture dl"), "books");

    startRefresh("shelf");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 1, MAGAZINES);
    reply(
        "aliases",
        1,
        "{\"aliases\":{\"shelf\":\"magazines\"},"
            + "\"properties\":{\"shelf\":{\"category\":\"updated\"}}}");
    assertState("t.aliasScope.selectedCollection === t.scope.currentCollection");
    assertState("t.aliasScope.selectedCollection.properties.category === 'updated'");
    waitForTextContains(By.cssSelector("#menu-refresh-fixture dl"), "magazines");
    waitForTextContains(By.cssSelector("#menu-refresh-fixture dl"), "updated");
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testAliasOverviewRejectsPreviousSelectionAndClearsWhenRemoved() {
    loadAliasOverview("shelf");
    assertState("!t.aliasScope.selectedCollection");
    reply("system", 0, CLOUD_SYSTEM);
    reply("collections", 0, BOOKS);
    reply("aliases", 0, "{\"aliases\":{\"shelf\":\"books\"},\"properties\":{}}");
    assertState("t.aliasScope.selectedCollection.name === 'shelf'");

    startRefresh("shelf");
    reply("system", 1, CLOUD_SYSTEM);
    reply("collections", 1, BOOKS);
    reply("aliases", 1, NO_ALIASES);
    assertState("!t.aliasScope.selectedCollection && !t.scope.currentCollection");
    assertNoSevereConsoleErrors();
  }

  private void startRefresh(String collection) {
    script(
        """
        var collection = arguments[0];
        t.scope.$apply(function() {
          t.route.current.params.core = collection;
          t.scope.resetMenu('collection-overview', t.constants.IS_COLLECTION_PAGE);
        });
        """,
        collection);
  }

  private void reply(String kind, int index, String json) {
    reply(kind, index, json, false);
  }

  private void replyError(String kind, int index) {
    reply(kind, index, "{}", true);
  }

  private void reply(String kind, int index, String json, boolean error) {
    ((JavascriptExecutor) driver)
        .executeAsyncScript(
            """
            var t = window.menuRefreshTest;
            var done = arguments[arguments.length - 1];
            t.requests[arguments[0]][arguments[1]](
              arguments[3] ? new Error('controlled API failure') : null,
              JSON.parse(arguments[2]), {status: arguments[3] ? 500 : 200});
            // Run after the controller's real $timeout callback and its digest.
            t.timeout(function() { done(); });
            """,
            kind,
            index,
            json,
            error);
  }

  private void loadAliasOverview(String alias) {
    Object error =
        ((JavascriptExecutor) driver)
            .executeAsyncScript(
                """
                var t = window.menuRefreshTest;
                var alias = arguments[0];
                var done = arguments[arguments.length - 1];
                var injector = angular.element(document.body).injector();
                t.route.current.params.core = alias;
                t.aliasScope = t.scope.$new();
                injector.get('$controller')('AliasOverviewController', {
                  $scope: t.aliasScope, $routeParams: {core: alias}
                });
                injector.get('$templateRequest')('partials/alias_overview.html').then(function(html) {
                  t.fixture.append(injector.get('$compile')(html)(t.aliasScope));
                  done();
                }, function(error) { done(String(error)); });
                """,
                alias);
    assertNull("Could not load the alias overview template: " + error, error);
  }

  private Object script(String body, Object... arguments) {
    return ((JavascriptExecutor) driver)
        .executeScript("var t = window.menuRefreshTest;\n" + body, arguments);
  }

  private void assertState(String expression) {
    assertTrue(expression, Boolean.TRUE.equals(script("return (" + expression + ");")));
  }

  private void assertSelectionText(String name) {
    assertEquals(
        name, waitForText(By.cssSelector("#menu_refresh_selector_chosen .chosen-single span")));
  }
}
