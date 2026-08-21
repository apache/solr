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
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;

/** Tests the Core Admin screen: core listing and the reload action. */
public class AdminUiCoreAdminScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "corescoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
  }

  @Test
  public void testCoreAdminShowsCore() throws Exception {
    NamedList<Object> response = adminApi("/admin/cores", params());
    Map<?, ?> status = (Map<?, ?>) response.get("status");
    assertFalse("Node should host at least one core", status.isEmpty());
    String coreName = status.keySet().iterator().next().toString();

    openPage("~cores", By.id("cores"));
    waitForPageContains(coreName);
    assertNoSevereConsoleErrors();
  }

  @Test
  public void testReloadCoreViaUi() {
    String coreName = coreNameOnNode0(COLLECTION);
    openPage("~cores/" + coreName, By.id("cores"));
    click(By.cssSelector("#cores #reload"));
    // the button is marked with the success class when the reload succeeded
    waitFor(By.cssSelector("#cores #reload.success"));
    assertNoSevereConsoleErrors();
  }
}
