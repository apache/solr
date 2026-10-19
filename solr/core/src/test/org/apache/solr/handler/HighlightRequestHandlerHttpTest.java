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
package org.apache.solr.handler;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.StringRequestContent;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

@SuppressSSL
public class HighlightRequestHandlerHttpTest extends SolrTestCaseJ4 {

  private static final String CORE_NAME = "highlight";
  private static final String TEXT = "Solr highlights the text.";

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void startSolr() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr();
    solrTestRule.newCollection(CORE_NAME).withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
  }

  @Test
  public void testPostAndQueryMethodsAcceptPlainTextBody() throws Exception {
    String url =
        solrTestRule.getBaseUrl()
            + "/"
            + CORE_NAME
            + "/highlight?q=_text_%3Asolr&hl.fl=_text_&wt=json";

    assertHighlightResponse(sendBodyRequest("POST", url));
    assertHighlightResponse(sendBodyRequest("QUERY", url));
  }

  @Test
  public void testGetStreamBodyAndV2Routes() throws Exception {
    String encodedText = URLEncoder.encode(TEXT, StandardCharsets.UTF_8);
    String params = "?q=_text_%3Asolr&hl.fl=_text_&wt=json&stream.body=" + encodedText;
    String v2Url = solrTestRule.getJetty().getBaseURLV2() + "/cores/" + CORE_NAME + "/highlight";

    assertHighlightResponse(
        solrTestRule
            .getJetty()
            .getSolrClient()
            .getHttpClient()
            .GET(solrTestRule.getBaseUrl() + "/" + CORE_NAME + "/highlight" + params));

    assertHighlightResponse(
        solrTestRule.getJetty().getSolrClient().getHttpClient().GET(v2Url + params));
    assertHighlightResponse(
        sendBodyRequest("POST", v2Url + "?q=_text_%3Asolr&hl.fl=_text_&wt=json"));
    assertHighlightResponse(
        sendBodyRequest("QUERY", v2Url + "?q=_text_%3Asolr&hl.fl=_text_&wt=json"));
  }

  private static ContentResponse sendBodyRequest(String method, String url) throws Exception {
    return solrTestRule
        .getJetty()
        .getSolrClient()
        .getHttpClient()
        .newRequest(url)
        .method(method)
        .body(new StringRequestContent("text/plain; charset=UTF-8", TEXT, StandardCharsets.UTF_8))
        .send();
  }

  private static void assertHighlightResponse(ContentResponse response) {
    String body = response.getContentAsString();
    assertEquals(body, 200, response.getStatus());
    assertTrue(body, body.contains("\"highlighting\""));
    assertTrue(body, body.contains("\"_text_\""));
    assertTrue(body, body.contains("<em>Solr</em>"));
  }
}
