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

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/**
 * Tests the log-level editor on a standalone (user-managed) node.
 *
 * <p>SOLR-18317: the UI used to unconditionally send {@code nodes=all} to {@code
 * /admin/info/logging}, a parameter that only makes sense in SolrCloud mode (it broadcasts the
 * level change to every live node) and used to NPE server-side without a {@code ZkController}.
 * This exercises the fixed {@code LoggingLevelController.setLevel} against a real standalone node,
 * where the mapping test coverage ({@code AdminUiLoggingScreenTest}) only ever runs against a
 * SolrCloud cluster.
 */
public class AdminUiLoggingStandaloneTest extends AdminUiStandaloneTestBase {

  @BeforeClass
  public static void startStandaloneNode() throws Exception {
    Path home = buildStandaloneHome("collection1");
    standaloneJetty = startStandaloneJetty(home);
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
  public void testChangeLogLevelViaUi() throws Exception {
    String logger = "org.apache.solr.core";
    openPage("~logging/level", By.id("loggingtree"));

    WebElement anchor =
        waitFor(By.cssSelector("#loggingtree a.jstree-anchor[title='" + logger + "']"));
    anchor.click();
    click(By.xpath("//li[a/@title='" + logger + "']//a[normalize-space()='WARN']"));
    assertLoggerLevel(logger, "WARN");

    // revert to unset; the logger then reports the inherited level with set=false
    click(By.cssSelector("#loggingtree a.jstree-anchor[title='" + logger + "']"));
    click(By.xpath("//li[a/@title='" + logger + "']//a[normalize-space()='UNSET']"));
    assertLoggerLevel(logger, null);
    assertNoSevereConsoleErrors();
  }

  /**
   * Asserts the level a logger was explicitly set to, or with {@code expectedLevel} null, that the
   * logger has no explicit level (it then reports the inherited effective level with set=false).
   *
   * <p>Before SOLR-18317, the hardcoded {@code nodes=all} param made this request NPE server-side
   * in standalone mode, so the UI's success callback (and thus {@code $scope.refresh()}) never
   * ran; this would time out here rather than observing the new level.
   */
  @SuppressWarnings("unchecked")
  private void assertLoggerLevel(String logger, String expectedLevel) throws Exception {
    waitUntil(
        "logger " + logger + " has level " + (expectedLevel == null ? "(unset)" : expectedLevel),
        () -> {
          try {
            NamedList<Object> response = adminApi("/admin/info/logging", params());
            for (Map<?, ?> entry : (List<Map<?, ?>>) response.get("loggers")) {
              if (logger.equals(entry.get("name"))) {
                return expectedLevel == null
                    ? Boolean.FALSE.equals(entry.get("set"))
                    : expectedLevel.equals(entry.get("level"))
                        && Boolean.TRUE.equals(entry.get("set"));
              }
            }
            return false;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
