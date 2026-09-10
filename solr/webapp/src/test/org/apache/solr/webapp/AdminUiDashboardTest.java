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
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.openqa.selenium.By;

/**
 * Tests that the Admin UI dashboard (route {@code #/}) renders the correct Solr version and system
 * stats, compared against the {@code /admin/info/system} API the screen is built from.
 */
public class AdminUiDashboardTest extends AdminUiTestBase {

  @Test
  public void testDashboardShowsVersionsAndSystemStats() throws Exception {
    NamedList<Object> system = adminApi("/admin/info/system", params());
    Map<?, ?> lucene = (Map<?, ?>) system.get("lucene");
    Map<?, ?> jvm = (Map<?, ?>) system.get("jvm");

    openPage("", By.id("index"));

    // Versions block matches the API values
    String solrSpecVersion = (String) lucene.get("solr-spec-version");
    assertEquals(solrSpecVersion, waitForText(By.cssSelector("#versions .solr_spec_version dd")));
    String luceneSpecVersion = (String) lucene.get("lucene-spec-version");
    assertEquals(
        luceneSpecVersion, waitForText(By.cssSelector("#versions .lucene_spec_version dd")));

    // JVM block shows the runtime name and version
    String jvmText = waitForText(By.cssSelector("#jvm .jvm_version dd"));
    assertEquals(jvm.get("name") + " " + jvm.get("version"), jvmText);

    // JVM memory bar is rendered with a non-empty max value
    String jvmMemoryMax = waitForText(By.cssSelector("#jvm-memory-bar .bar-max.val"));
    assertFalse("JVM memory bar should show a max value", jvmMemoryMax.isBlank());

    // Security block warns that security is not enabled on this vanilla cluster
    String securityText = waitFor(By.cssSelector("#security .warning-msg")).getText();
    assertTrue(
        "Expected security warning, got: " + securityText,
        securityText.contains("Security is not enabled"));

    assertNoSevereConsoleErrors();
  }
}
