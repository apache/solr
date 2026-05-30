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
package org.apache.solr.servlet;

import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpHeader;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Verifies that Solr's Jetty GzipHandler correctly compresses HTTP responses when the client
 * sends {@code Accept-Encoding: gzip} and omits compression when that header is absent.
 *
 * <p>Replaces the BATS integration test {@code test_compression.bats}.
 */
public class GzipCompressionTest extends SolrCloudTestCase {

  private static final String COLLECTION = "gzip-test";

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Lower the minGzipSize threshold so any non-empty response body is eligible for compression.
    // jetty-gzip.xml reads this property at Jetty start time via <Property name="jetty.gzip.minGzipSize">.
    System.setProperty("jetty.gzip.minGzipSize", "1");

    configureCluster(1).configure();

    CollectionAdminRequest.createCollection(COLLECTION, null, 1, 1)
        .process(cluster.getSolrClient());

    // Index a few documents so the select response has a non-trivial body
    UpdateRequest req = new UpdateRequest();
    for (int i = 0; i < 10; i++) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc-" + i);
      doc.addField("name_s", "Document number " + i + " for gzip compression test");
      req.add(doc);
    }
    req.commit(cluster.getSolrClient(), COLLECTION);
  }

  @AfterClass
  public static void clearGzipSysProp() {
    System.clearProperty("jetty.gzip.minGzipSize");
  }

  private HttpClient getHttpClient() {
    return cluster.getJettySolrRunner(0).getSolrClient().getHttpClient();
  }

  private String getSelectUrl() {
    return cluster.getJettySolrRunner(0).getBaseUrl() + "/" + COLLECTION + "/select?q=*:*&rows=10";
  }

  @Test
  public void testNoCompressionWithoutAcceptEncodingHeader() throws Exception {
    ContentResponse response = getHttpClient().GET(getSelectUrl());
    assertEquals("Expected HTTP 200", 200, response.getStatus());
    assertNull(
        "Content-Encoding should not be set when Accept-Encoding header is absent",
        response.getHeaders().get(HttpHeader.CONTENT_ENCODING));
  }

  @Test
  public void testGzipCompressionWithAcceptEncodingHeader() throws Exception {
    ContentResponse response =
        getHttpClient()
            .newRequest(getSelectUrl())
            .headers(h -> h.add(HttpHeader.ACCEPT_ENCODING, "gzip"))
            .send();
    assertEquals("Expected HTTP 200", 200, response.getStatus());
    assertEquals(
        "Expected gzip Content-Encoding when Accept-Encoding: gzip was requested",
        "gzip",
        response.getHeaders().get(HttpHeader.CONTENT_ENCODING));
  }
}
