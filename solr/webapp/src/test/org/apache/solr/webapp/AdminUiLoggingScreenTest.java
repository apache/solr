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

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import org.apache.solr.common.util.NamedList;
import org.junit.Assume;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests the Logging screens: the recent-events viewer and the log level editor. */
public class AdminUiLoggingScreenTest extends AdminUiTestBase {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Test
  public void testLoggingLevelTree() {
    openPage("~logging/level", By.id("loggingtree"));
    waitForPageContains("org.apache.solr");
    waitFor(By.cssSelector("#loggingtree .jstree-anchor"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testEventsViewerShowsWarnings() throws Exception {
    // start the cluster before emitting the probe: the log watcher only exists once the
    // nodes are up, and with lazy startup this test may be the first cluster user
    ensureCloudCluster();

    // the cluster nodes run in this JVM, so the log watcher observes our own log events
    String probeMessage = "Admin UI logging viewer probe event";
    log.warn(probeMessage);

    // all test clusters in this JVM register a log-watcher appender under the same name
    // in the shared log4j config, so this cluster's watcher may be blind when other UI
    // test classes ran first; only assert the UI when the backing API sees the event
    boolean watcherSawProbe = false;
    long deadlineNanos = System.nanoTime() + WAIT_TIMEOUT.toNanos();
    while (System.nanoTime() < deadlineNanos && !watcherSawProbe) {
      watcherSawProbe =
          adminApi("/admin/info/logging", params("since", "0")).toString().contains(probeMessage);
      Thread.sleep(250);
    }
    Assume.assumeTrue(
        "This node's log watcher does not receive events (shared-JVM log4j state); skipping",
        watcherSawProbe);

    openPage("~logging", By.id("viewer"));
    waitUntil(
        "probe event should appear in the viewer",
        () -> {
          driver.navigate().refresh();
          waitFor(By.id("viewer"));
          return driver.getPageSource().contains(probeMessage);
        });
    assertNoSevereConsoleErrors();
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
