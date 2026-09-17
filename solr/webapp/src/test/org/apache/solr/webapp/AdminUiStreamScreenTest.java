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

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

/** Tests the Stream screen: executing a streaming expression through the form. */
public class AdminUiStreamScreenTest extends AdminUiTestBase {

  private static final String COLLECTION = "streamcoll";

  @BeforeClass
  public static void setupCollection() throws Exception {
    createFixtureCollection(COLLECTION, 1, 1);
    SolrClient client = cluster.getSolrClient(COLLECTION);
    for (int i = 1; i <= 3; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "stream-doc-" + i);
      client.add(doc);
    }
    client.commit();
  }

  @Test
  public void testStreamingExpressionViaUi() {
    openPage(COLLECTION + "/stream", By.id("stream"));
    WebElement expr = waitFor(By.id("expr"));
    expr.clear();
    expr.sendKeys("search(" + COLLECTION + ",q=\"*:*\",fl=\"id\",sort=\"id asc\")");
    click(By.cssSelector("#stream button[type=submit]"));
    String response = waitForTextContains(By.cssSelector("#stream #result"), "stream-doc-1");
    assertTrue("All docs should stream: " + response, response.contains("stream-doc-3"));
    assertNoSevereConsoleErrors();
  }
}
