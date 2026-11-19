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
 * Covers {@code core-overview.js}'s two replication-fetch-failure paths: a generic failure shows
 * the inline "not configured" message, while a 401/403 routes through {@code ApiErrorHandler}
 * instead.
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
    // Not asserting #authz-failures itself: app.js's httpInterceptor clears
    // $rootScope.showAuthzFailures on every successful v1 call, and core-overview fires several
    // of those (Luke, CoreInfo, Ping) alongside this one, so the banner isn't reliably still up
    // by the time we'd check it. This test only guards the message text below.
    forcedReplicationStatus = HttpServletResponse.SC_FORBIDDEN;
    openPage(CORE + "/core-overview", By.id("dashboard"));

    // replicationMessage is always set once the request settles (to the message, or to null
    // for an auth failure), so waiting for it to be defined signals the request is done.
    waitUntil(
        "the replication-details request should have completed", this::replicationRequestSettled);
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
