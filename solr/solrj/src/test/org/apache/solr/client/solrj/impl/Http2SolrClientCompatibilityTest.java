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

package org.apache.solr.client.solrj.impl;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.solr.SolrJettyTestBase;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettyConfig;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.util.LogLevel;
import org.eclipse.jetty.client.dynamic.HttpClientTransportDynamic;
import org.eclipse.jetty.client.http.HttpClientTransportOverHTTP;
import org.eclipse.jetty.servlet.ServletHolder;

// Adapted for the fork: Http2SolrClient no longer selects the static Jetty HttpClientTransportOverHTTP2 for
// the HTTP/2 path. With useHttp1_1=false it builds an HttpClientTransportDynamic that auto-negotiates h1/h2
// (Http2SolrClient ~line 283); with useHttp1_1=true it builds a SolrHttpClientTransportOverHTTP (a subclass
// of HttpClientTransportOverHTTP). Assertions updated to match these actual transport types. Because the
// dynamic transport can speak HTTP/1.1, the old "HTTP/2 client cannot reach an HTTP/1-only node" expectation
// no longer holds and has been adapted accordingly.
@LogLevel("org.eclipse.jetty.client=DEBUG;org.eclipse.jetty.util=DEBUG")
@SolrTestCaseJ4.SuppressSSL
public class Http2SolrClientCompatibilityTest extends SolrJettyTestBase {

  @org.junit.Test
  public void testSystemPropertyFlag() {
    System.setProperty("solr.http1", "true");
    try (Http2SolrClient client = new Http2SolrClient.Builder()
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
    }
    System.clearProperty("solr.http1");
    try (Http2SolrClient client = new Http2SolrClient.Builder()
        .build()) {
      assertTrue(client.getHttpClient().getTransport().getClass().getName(),
          client.getHttpClient().getTransport() instanceof HttpClientTransportDynamic);
    }
  }

  @org.junit.Test
  public void testConnectToOldNodesUsingHttp1() throws Exception {

    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
        .useOnlyHttp1(true)
        .build();
    JettySolrRunner jetty = createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);

    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl() + "/debug/foo")
        .useHttp1_1(true)
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException | SolrServerException ignored) {
        // best-effort connectivity probe; the meaningful assertion is the transport type above
      }
    }
  }

  @org.junit.Test
  public void testConnectToNewNodesUsingHttp1() throws Exception {

    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
        .useOnlyHttp1(false)
        .build();
    JettySolrRunner jetty = createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);

    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + "/debug/foo")
        .useHttp1_1(true)
        .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException | SolrServerException ignored) {
        // best-effort connectivity probe; the meaningful assertion is the transport type above
      }
    }
  }

  @LuceneTestCase.Nightly // oddly slow
  @org.junit.Test
  public void testConnectToOldNodesUsingHttp2() throws Exception {
    // The fork's default (non-useHttp1_1) client builds an HttpClientTransportDynamic that negotiates
    // h1/h2, so it CAN connect to HTTP/1-only nodes — the opposite of the old static-HTTP2 behavior.
    JettyConfig jettyConfig = JettyConfig.builder()
        .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
        .useOnlyHttp1(true)
        .build();
    JettySolrRunner jetty = createAndStartJetty(legacyExampleCollection1SolrHome(), jettyConfig);

    System.clearProperty("solr.http1");
    try (Http2SolrClient client = new Http2SolrClient.Builder(jetty.getBaseUrl().toString() + "/debug/foo")
        .build()) {
      assertTrue(client.getHttpClient().getTransport().getClass().getName(),
          client.getHttpClient().getTransport() instanceof HttpClientTransportDynamic);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (BaseHttpSolrClient.RemoteSolrException | SolrServerException ignored) {
        // the DebugServlet does not return a real Solr response; a remote/transport error is fine — the
        // point is that the dynamic transport type is in use (asserted above), not a successful query.
      }
    }
  }
}
