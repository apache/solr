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

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;

/**
 * Covers the core overview page's two replication-fetch-failure paths in CoreOverviewController
 * ({@code core-overview.js}): a generic fetch failure keeps the friendly inline "not configured"
 * message (the pre-existing v1 behavior, preserved as-is), while a 401/403 instead routes through
 * the shared {@code ApiErrorHandler}, since that failure isn't actually about replication being
 * unconfigured.
 */
public class AdminUiCoreOverviewReplicationErrorsTest extends AdminUiStandaloneTestBase {

  private static final String CORE = "replicationerrors";

  /** 0 = pass through; otherwise the status the filter forces for this core's /replication call. */
  private static volatile int forcedReplicationStatus = 0;

  @BeforeClass
  public static void startStandaloneNode() throws Exception {
    Path home = buildStandaloneHome(CORE);
    JettyConfig.Builder config = JettyConfig.builder();
    config.withFilter(ForceReplicationStatusFilter.class, "/*");
    configureJettyForUi(config);
    standaloneJetty = new JettySolrRunner(home.toString(), new Properties(), config.build());
    standaloneJetty.start();
    baseUrl = standaloneJetty.getBaseUrl().toString();
  }

  @AfterClass
  public static void stopStandaloneNode() throws Exception {
    if (standaloneJetty != null) {
      standaloneJetty.stop();
      standaloneJetty = null;
    }
  }

  @Test
  public void testGenericReplicationFailureShowsInlineMessage() {
    forcedReplicationStatus = HttpServletResponse.SC_SERVICE_UNAVAILABLE;
    openPage(CORE + "/core-overview", By.id("dashboard"));

    waitForTextContains(By.cssSelector("#replication .message-container"), "not configured");
    assertNoSevereConsoleErrors("/replication");
  }

  @Test
  public void testReplicationForbiddenDoesNotShowConfigurationMessage() throws Exception {
    // Asserting the global permission banner (#authz-failures) actually appears isn't reliable
    // here: app.js's httpInterceptor unconditionally clears $rootScope.showAuthzFailures on every
    // *successful* v1 $http call (see its `ended()` handler), and core-overview fires several of
    // those (Luke, CoreInfo, Ping) concurrently with this replication call - whichever lands last
    // wins, regardless of this 403. That race is pre-existing and out of scope here; what this
    // test guards is that the misleading "Replication is not configured" message, which previously
    // showed for every failure including this one, no longer does for an auth failure.
    forcedReplicationStatus = HttpServletResponse.SC_FORBIDDEN;
    openPage(CORE + "/core-overview", By.id("dashboard"));

    // replicationMessage is explicitly set (to the string, or to null for the auth-failure
    // branch) as soon as the request settles, so polling for it being defined is a reliable
    // "request has completed" signal distinct from "hasn't started yet".
    waitUntil("the replication-details request should have completed", this::replicationRequestSettled);
    assertTrue(
        driver.findElements(By.cssSelector("#replication .message-container")).stream()
            .noneMatch(e -> e.getText().contains("not configured")));
    assertNoSevereConsoleErrors("/replication");
  }

  private boolean replicationRequestSettled() {
    Object settled =
        ((JavascriptExecutor) driver)
            .executeScript(
                "var el = document.querySelector('#replication');"
                    + "if (!el) return false;"
                    + "var scope = angular.element(el).scope();"
                    + "return scope.replicationMessage !== undefined;");
    return Boolean.TRUE.equals(settled);
  }

  /** Forces the configured HTTP status for this core's v2 replication-details call, when armed. */
  public static class ForceReplicationStatusFilter implements Filter {
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
        throws IOException, ServletException {
      HttpServletRequest req = (HttpServletRequest) request;
      if (forcedReplicationStatus != 0
          && req.getRequestURI().endsWith("/cores/" + CORE + "/replication")) {
        ((HttpServletResponse) response).sendError(forcedReplicationStatus);
        return;
      }
      chain.doFilter(request, response);
    }
  }
}
