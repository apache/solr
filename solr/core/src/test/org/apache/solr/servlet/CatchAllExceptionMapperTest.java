/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.servlet;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.client.StringRequestContent;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/** SOLR-18066: JAX-RS API ignores hideStackTrace */
@SuppressSSL
public class CatchAllExceptionMapperTest extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void setupSolrHome() throws Exception {
    Path solrHome = createTempDir("solrhome");
    Files.writeString(
        solrHome.resolve("solr.xml"),
        "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
            + "<solr>\n"
            + "  <bool name=\"hideStackTrace\">true</bool>\n"
            + "</solr>\n");
    EnvUtils.setProperty(ALLOW_PATHS_SYSPROP, solrHome.toAbsolutePath().toString());
    solrTestRule.startSolr(solrHome);
  }

  @Test
  public void testHideStackTraceIsRespectedForCorelessV2Endpoints() throws Exception {
    // Sending a single object instead of the required List<LogLevelChange> triggers a
    // MismatchedInputException through CatchAllExceptionMapper on a coreless node-level endpoint.
    final String url = solrTestRule.getJetty().getBaseURLV2().toString() + "/node/logging/levels";
    var httpClient = solrTestRule.getJetty().getSolrClient().getHttpClient();
    var response =
        httpClient
            .newRequest(url)
            .method("PUT")
            .body(
                new StringRequestContent(
                    "application/json",
                    "{\"logger\": \"org.apache.solr\", \"level\": \"DEBUG\"}",
                    StandardCharsets.UTF_8))
            .send();

    String body = response.getContentAsString();
    assertEquals(500, response.getStatus());
    assertTrue(body.contains("MismatchedInputException"));
    assertFalse(body.contains("\"trace\""));
  }
}
