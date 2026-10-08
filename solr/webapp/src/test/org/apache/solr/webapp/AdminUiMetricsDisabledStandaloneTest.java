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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;

/**
 * SOLR-18400: the Plugins screen on a node with metrics collection switched off in solr.xml. The
 * metrics endpoint answers HTTP 510 there, which the screen must turn into an explanation rather
 * than a blank page or the global error banner.
 */
public class AdminUiMetricsDisabledStandaloneTest extends AdminUiStandaloneTestBase {

  private static final String CORE = "collection1";

  @BeforeClass
  public static void startStandaloneNode() throws Exception {
    // the base class turned metrics on for the UI screens; this suite is about them being off.
    // Must come before the node starts, and is restored after the class with the other properties
    System.setProperty("metricsEnabled", "false");
    Path home = buildStandaloneHome(CORE);
    standaloneJetty = startStandaloneJetty(home);
    baseUrl = standaloneJetty.getBaseUrl().toString();
    assertFalse(
        "fixture node should have metrics disabled",
        standaloneJetty.getCoreContainer().getConfig().getMetricsConfig().isEnabled());
  }

  @AfterClass
  public static void stopStandaloneNode() throws Exception {
    if (standaloneJetty != null) {
      standaloneJetty.stop();
      standaloneJetty = null;
    }
  }

  @Test
  public void testPluginsScreenExplainsDisabledMetrics() {
    openPage(CORE + "/plugins", By.id("plugins"));

    String message = waitForText(By.cssSelector("#plugins .message-container .message"));
    assertTrue(message, message.contains("Metrics collection is disabled"));

    // no plugin categories or entries, as there is no metrics data to build them from
    assertTrue(driver.findElements(By.cssSelector("#plugins #navigation a[rel]")).isEmpty());
    assertTrue(driver.findElements(By.cssSelector("#plugins #frame li.entry")).isEmpty());

    // the 510 is handled by the screen itself; the global error banner must stay away
    assertTrue(
        "global error banner should not show for a disabled feature",
        driver.findElements(By.id("http-exception")).isEmpty());

    // Chrome reports the failed XHR itself at SEVERE level; everything else must be clean
    assertNoSevereConsoleErrors("510");
  }
}
