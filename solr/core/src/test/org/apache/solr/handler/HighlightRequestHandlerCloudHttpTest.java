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
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.util.ExternalPaths;
import org.eclipse.jetty.client.ContentResponse;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressSSL
public class HighlightRequestHandlerCloudHttpTest extends SolrCloudTestCase {

  private static final String CORE_NAME = "highlight";
  private static final String TEXT = "Solr highlights the text.";

  @BeforeClass
  public static void startCluster() throws Exception {
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    configureCluster(1).addConfig("default", ExternalPaths.DEFAULT_CONFIGSET).configure();
    CollectionAdminRequest.createCollection(CORE_NAME, "default", 1, 1)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(CORE_NAME, 1, 1);
  }

  @Test
  public void testGetStreamBodyOnV2CollectionRoute() throws Exception {
    String params =
        "?q=_text_%3Asolr&hl.fl=_text_&wt=json&stream.body="
            + URLEncoder.encode(TEXT, StandardCharsets.UTF_8);
    String v2CollectionUrl =
        cluster.getJettySolrRunner(0).getBaseURLV2() + "/c/" + CORE_NAME + "/highlight";

    assertHighlightResponse(
        cluster
            .getJettySolrRunner(0)
            .getSolrClient()
            .getHttpClient()
            .GET(v2CollectionUrl + params));
  }

  private static void assertHighlightResponse(ContentResponse response) {
    String body = response.getContentAsString();
    assertEquals(body, 200, response.getStatus());
    assertTrue(body, body.contains("\"highlighting\""));
    assertTrue(body, body.contains("\"_text_\""));
    assertTrue(body, body.contains("<em>Solr</em>"));
  }
}
