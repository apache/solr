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

package org.apache.solr.client.solrj.jetty;

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import java.util.Properties;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.LogLevel;
import org.apache.solr.util.ServletFixtures.DebugServlet;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.client.transport.HttpClientTransportOverHTTP;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.http2.FlowControlStrategy;
import org.eclipse.jetty.http2.SimpleFlowControlStrategy;
import org.eclipse.jetty.http2.client.HTTP2Client;
import org.eclipse.jetty.http2.client.transport.HttpClientTransportOverHTTP2;
import org.junit.ClassRule;

@LogLevel("org.eclipse.jetty.client=DEBUG;org.eclipse.jetty.util=DEBUG")
@SolrTestCaseJ4.SuppressSSL
public class HttpJettySolrClientCompatibilityTest extends SolrTestCaseJ4 {

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  public void testSystemPropertyFlag() {
    System.setProperty("solr.http1", "true");
    try (var client = new HttpJettySolrClient.Builder().build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
    }
    System.clearProperty("solr.http1");
    try (var client = new HttpJettySolrClient.Builder().build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP2);
    }
  }

  public void testHttp2FlowControlSystemProperties() {
    // Test with custom session and stream recv window sizes
    System.setProperty(
        HttpJettySolrClient.HTTP2_SESSION_RECV_WINDOW_SYSPROP, String.valueOf(4 * 1024 * 1024));
    System.setProperty(
        HttpJettySolrClient.HTTP2_STREAM_RECV_WINDOW_SYSPROP, String.valueOf(2 * 1024 * 1024));
    System.setProperty(HttpJettySolrClient.HTTP2_SIMPLE_FLOW_CONTROL_SYSPROP, "true");

    try (var client = new HttpJettySolrClient.Builder().build()) {
      var transport = client.getHttpClient().getTransport();
      assertTrue("Expected HTTP/2 transport", transport instanceof HttpClientTransportOverHTTP2);

      HttpClientTransportOverHTTP2 http2Transport = (HttpClientTransportOverHTTP2) transport;
      HTTP2Client http2Client = http2Transport.getHTTP2Client();

      assertEquals(
          "Session recv window should be set",
          4 * 1024 * 1024,
          http2Client.getInitialSessionRecvWindow());
      assertEquals(
          "Stream recv window should be set",
          2 * 1024 * 1024,
          http2Client.getInitialStreamRecvWindow());

      // Verify simple flow control strategy is used
      FlowControlStrategy.Factory factory = http2Client.getFlowControlStrategyFactory();
      FlowControlStrategy strategy = factory.newFlowControlStrategy();
      assertTrue(
          "Expected SimpleFlowControlStrategy", strategy instanceof SimpleFlowControlStrategy);
    } finally {
      System.clearProperty(HttpJettySolrClient.HTTP2_SESSION_RECV_WINDOW_SYSPROP);
      System.clearProperty(HttpJettySolrClient.HTTP2_STREAM_RECV_WINDOW_SYSPROP);
      System.clearProperty(HttpJettySolrClient.HTTP2_SIMPLE_FLOW_CONTROL_SYSPROP);
    }
  }

  @SuppressWarnings("try") // HTTP2Client is AutoCloseable but doesn't need closing when not started
  public void testHttp2FlowControlDefaultsUnchangedWhenPropertiesNotSet() {
    // Ensure no flow control properties are set
    System.clearProperty(HttpJettySolrClient.HTTP2_SESSION_RECV_WINDOW_SYSPROP);
    System.clearProperty(HttpJettySolrClient.HTTP2_STREAM_RECV_WINDOW_SYSPROP);
    System.clearProperty(HttpJettySolrClient.HTTP2_SIMPLE_FLOW_CONTROL_SYSPROP);

    // Get default values from a fresh HTTP2Client for comparison
    // Note: HTTP2Client doesn't need to be closed if never started
    HTTP2Client defaultHttp2Client = new HTTP2Client();
    int defaultSessionWindow = defaultHttp2Client.getInitialSessionRecvWindow();
    int defaultStreamWindow = defaultHttp2Client.getInitialStreamRecvWindow();

    try (var client = new HttpJettySolrClient.Builder().build()) {
      var transport = client.getHttpClient().getTransport();
      assertTrue("Expected HTTP/2 transport", transport instanceof HttpClientTransportOverHTTP2);

      HttpClientTransportOverHTTP2 http2Transport = (HttpClientTransportOverHTTP2) transport;
      HTTP2Client http2Client = http2Transport.getHTTP2Client();

      assertEquals(
          "Session recv window should remain at default",
          defaultSessionWindow,
          http2Client.getInitialSessionRecvWindow());
      assertEquals(
          "Stream recv window should remain at default",
          defaultStreamWindow,
          http2Client.getInitialStreamRecvWindow());
    }
  }

  public void testConnectToOldNodesUsingHttp1() throws Exception {

    JettyConfig jettyConfig =
        JettyConfig.builder()
            .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
            .useOnlyHttp1(true)
            .build();
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir(), new Properties(), jettyConfig);
    solrTestRule
        .newCollection(DEFAULT_TEST_COLLECTION_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();

    try (var client =
        new HttpJettySolrClient.Builder(solrTestRule.getBaseUrl() + "/debug/foo")
            .useHttp1_1(true)
            .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (RemoteSolrException ignored) {
      }
    } finally {
      solrTestRule.reset();
    }
  }

  public void testConnectToNewNodesUsingHttp1() throws Exception {

    JettyConfig jettyConfig =
        JettyConfig.builder()
            .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
            .useOnlyHttp1(false)
            .build();
    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir(), new Properties(), jettyConfig);
    solrTestRule
        .newCollection(DEFAULT_TEST_COLLECTION_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();

    try (var client =
        new HttpJettySolrClient.Builder(solrTestRule.getBaseUrl() + "/debug/foo")
            .useHttp1_1(true)
            .build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
      } catch (RemoteSolrException ignored) {
      }
    } finally {
      solrTestRule.reset();
    }
  }

  public void testConnectToOldNodesUsingHttp2() throws Exception {
    // if this test somehow fails, this means that the Jetty client may now be able to switch
    // between
    // HTTP/1 and HTTP/2.2 protocol dynamically therefore rolling updates will be easier we should
    // then notify this to users
    JettyConfig jettyConfig =
        JettyConfig.builder()
            .withServlet(new ServletHolder(DebugServlet.class), "/debug/*")
            .useOnlyHttp1(true)
            .build();

    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir(), new Properties(), jettyConfig);
    solrTestRule
        .newCollection(DEFAULT_TEST_COLLECTION_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();

    System.clearProperty("solr.http1");
    try (var client =
        new HttpJettySolrClient.Builder(solrTestRule.getBaseUrl() + "/debug/foo").build()) {
      assertTrue(client.getHttpClient().getTransport() instanceof HttpClientTransportOverHTTP2);
      try {
        client.query(new SolrQuery("*:*"), SolrRequest.METHOD.GET);
        fail("Jetty client with HTTP2 transport should not be able to connect to HTTP1 only nodes");
      } catch (RemoteSolrException ignored) {
        fail("Jetty client with HTTP2 transport should not be able to connect to HTTP1 only nodes");
      } catch (SolrServerException e) {
        // expected
      }
    } finally {
      solrTestRule.reset();
    }
  }
}
