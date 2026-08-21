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
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/** Tests the Paramsets screen: form display and creating/deleting a paramset through the UI. */
public class AdminUiParamsetsScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "paramscoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testParamsetsScreenRenders() {
    openPage(COLLECTION + "/paramsets", By.id("paramsets"));
    waitFor(By.cssSelector("#paramsets #form"));
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testCreateAndDeleteParamsetViaUi() throws Exception {
    String paramset = "uiparams";
    openPage(COLLECTION + "/paramsets", By.id("paramsets"));

    WebElement content = waitFor(By.id("paramsetContent"));
    content.clear();
    content.sendKeys("{\"set\":{\"" + paramset + "\":{\"rows\":\"7\",\"df\":\"title_txt\"}}}");
    click(By.cssSelector("#paramsets #submit"));
    waitForTextContains(By.cssSelector("#paramsets #result"), "success");

    waitUntil("paramset should exist with rows=7", () -> paramsetRows(paramset).equals("7"));

    // select the paramset and delete it
    openPage(COLLECTION + "/paramsets?paramset=" + paramset, By.id("paramsets"));
    click(By.cssSelector("button#delete-paramset"));
    waitUntil("paramset should be gone", () -> paramsetRows(paramset).isEmpty());
    assertNoSevereConsoleErrors();
  }

  /** Returns the rows param of the paramset, or empty string when absent. */
  private String paramsetRows(String name) {
    try {
      Object rows =
          adminApi("/" + COLLECTION + "/config/params/" + name, params())
              ._get(List.of("response", "params", name, "rows"), "");
      return rows == null ? "" : rows.toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
