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
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;

/**
 * Smoke test navigating every screen of the Admin UI, asserting that each renders its main content
 * element without severe browser console errors.
 */
public class AdminUiSmokeTest extends AdminUiTestBase {

  private static final String COLLECTION = "smoke";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 2);
  }

  @Test
  public void testNodeLevelScreens() {
    Map<String, By> screens =
        Map.of(
            "", By.id("index"),
            "~logging", By.id("logging"),
            "~logging/level", By.id("logging"),
            "~cores", By.id("cores"),
            "~collections", By.id("collections"),
            "~java-properties", By.id("java-properties"),
            "~threads", By.id("threads"),
            "~security", By.id("securityPanel"),
            "login", By.id("login"));
    screens.forEach(this::smoke);
  }

  @Test
  public void testCloudScreens() {
    Map<String, By> screens =
        Map.of(
            "~cloud?view=nodes", By.id("nodes-content"),
            "~cloud?view=tree", By.id("tree-content"),
            "~cloud?view=zkstatus", By.id("zk-status-content"),
            "~cloud?view=graph", By.id("graph-content"));
    screens.forEach(this::smoke);
  }

  @Test
  public void testSchemaDesignerScreen() {
    smoke("~schema-designer", By.id("designer"));
  }

  @Test
  public void testCollectionScreens() {
    Map<String, By> screens =
        Map.of(
            COLLECTION + "/collection-overview", By.id("dashboard"),
            COLLECTION + "/analysis", By.id("analysis"),
            COLLECTION + "/documents", By.id("documents"),
            COLLECTION + "/files", By.id("files"),
            COLLECTION + "/query", By.id("query"),
            COLLECTION + "/stream", By.id("stream"),
            COLLECTION + "/paramsets", By.id("paramsets"),
            COLLECTION + "/schema", By.id("schema"));
    screens.forEach(this::smoke);
  }

  @Test
  public void testCoreScreens() {
    // Plugins and Segments are core-level screens: their menu links use the core name
    String coreName =
        cluster.getJettySolrRunner(0).getCoreContainer().getAllCoreNames().iterator().next();
    Map<String, By> screens =
        Map.of(
            coreName + "/plugins", By.id("plugins"),
            coreName + "/segments", By.id("segments"));
    screens.forEach(this::smoke);
    // the ping widget on the overview answers 503 when no healthcheck file is configured,
    // as is the case for the _default configset
    smoke(coreName + "/core-overview", By.id("dashboard"), "/admin/ping");
  }

  private void smoke(String route, By anchor) {
    smoke(route, anchor, new String[0]);
  }

  private void smoke(String route, By anchor, String... allowedConsoleErrors) {
    try {
      openPage(route, anchor);
      assertNoSevereConsoleErrors(allowedConsoleErrors);
    } catch (AssertionError | RuntimeException e) {
      throw new AssertionError("Screen '" + route + "' failed to render: " + e.getMessage(), e);
    }
  }
}
