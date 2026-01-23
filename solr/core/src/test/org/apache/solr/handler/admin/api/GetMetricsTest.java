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
package org.apache.solr.handler.admin.api;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.metrics.MetricsUtil;
import org.apache.solr.response.PrometheusResponseWriter;
import org.apache.solr.util.SSLTestConfig;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpFields.Mutable;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for {@link GetMetrics}
 *
 * <p>These tests are not using any extension of HttpSolrClientBase:
 *
 * <ul>
 *   <li>to avoid dependency on SolrJ, SolrRequest, SolrParams, NamedList
 *   <li>to be able to send the "Accept" header
 * </ul>
 *
 * <p>Tests in this class make plain RESTful HTTP GET requests, with "Accept" header and query
 * parameters, to test GetMetrics/MetricsApi.
 *
 * <p>See also: TestMetricsRequest, in SolrJ
 */
public class GetMetricsTest extends SolrTestCaseJ4 {

  // No need for the full output
  private static final int MAX_OUTPUT = 1024;

  private static final int TIMEOUT = 15000;

  private static HttpClient jettyHttpClient;
  private static String metricsV2Url;
  private static MiniSolrCloudCluster cluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path tempDir = createTempDir();
    copyMinConf(tempDir);
    Files.copy(
        SolrTestCaseJ4.TEST_PATH().resolve("solr.xml"),
        tempDir.resolve("solr.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    MiniSolrCloudCluster.Builder clusterBuilder = new MiniSolrCloudCluster.Builder(2, tempDir);
    cluster = clusterBuilder.withSolrXml(tempDir.resolve("solr.xml")).build();

    metricsV2Url = cluster.getJettySolrRunner(0).getBaseURLV2().toString().concat("/metrics");

    // useSsl = true, clientAuth = false
    SSLTestConfig sslConfig = new SSLTestConfig(true, false);
    // trustAll = true
    SslContextFactory.Client factory = new SslContextFactory.Client(true);
    try {
      factory.setSslContext(sslConfig.buildClientSSLContext());
    } catch (KeyManagementException
        | UnrecoverableKeyException
        | NoSuchAlgorithmException
        | KeyStoreException e) {
      throw new IllegalStateException(
          "Unable to setup https scheme for HTTPClient to test SSL.", e);
    }

    jettyHttpClient = new HttpClient();
    jettyHttpClient.setConnectTimeout(TIMEOUT);
    jettyHttpClient.setSslContextFactory(factory);
    jettyHttpClient.setMaxConnectionsPerDestination(1);
    jettyHttpClient.setMaxRequestsQueuedPerDestination(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    jettyHttpClient.destroy();
    cluster.shutdown();
  }

  @Before
  public void beforeTest() throws Exception {
    // stop and start Jetty client for each test, otherwise, it seems responses get mixed!
    jettyHttpClient.start();
  }

  @After
  public void afterTest() throws Exception {
    jettyHttpClient.stop();
  }

  @Test
  public void testGetMetricsDefault()
      throws IOException,
          InterruptedException,
          ExecutionException,
          TimeoutException,
          SolrServerException {
    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .method(HttpMethod.GET)
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());

    String str = readMaxOut(response.getContent());
    System.out.println("testGetMetricsDefault: " + str);
    Assert.assertTrue(str.contains("# HELP"));
    Assert.assertTrue(str.contains("# TYPE"));
  }

  @Test
  public void testGetMetricsPrometheus()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .method(HttpMethod.GET)
              .headers(
                  new Consumer<Mutable>() {

                    @Override
                    public void accept(Mutable arg0) {
                      arg0.add(HttpHeader.ACCEPT, PrometheusResponseWriter.CONTENT_TYPE_PROMETHEUS);
                    }
                  })
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("text/plain", response.getMediaType());
  }

  @Test
  public void testGetMetricsOpenMetrics()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .method(HttpMethod.GET)
              .headers(
                  new Consumer<Mutable>() {

                    @Override
                    public void accept(Mutable arg0) {
                      arg0.add(
                          HttpHeader.ACCEPT, PrometheusResponseWriter.CONTENT_TYPE_OPEN_METRICS);
                    }
                  })
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("application/openmetrics-text", response.getMediaType());
  }

  @Test
  public void testGetMetricsCategoryParams() throws IOException {
    String expected = """
        category="QUERY"
        """;

    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .param(MetricsUtil.CATEGORY_PARAM, "QUERY")
              .method(HttpMethod.GET)
              .headers(
                  new Consumer<Mutable>() {

                    @Override
                    public void accept(Mutable arg0) {
                      arg0.add(HttpHeader.ACCEPT, PrometheusResponseWriter.CONTENT_TYPE_PROMETHEUS);
                    }
                  })
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());

    String str = readMaxOut(response.getContent());
    System.out.println("testGetMetricsCategoryParams: " + str);
    Assert.assertTrue(str.contains(expected.trim()));
    Assert.assertFalse(str.contains("category=\"CORE\""));
    Assert.assertFalse(str.contains("category=\"UPDATE\""));
  }

  @Test
  public void testGetMetricsProxyToNode() throws IOException {
    URL otherUrl = cluster.getJettySolrRunner(1).getBaseURLV2();
    String otherNode = otherUrl.getHost() + ":" + otherUrl.getPort() + "_solr";

    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .param(MetricsUtil.NODE_PARAM, otherNode)
              .method(HttpMethod.GET)
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    // HTTP 204: no content to test
    Assert.assertEquals(204, response.getStatus());

    String unknownNode = "unknown.host:1234_solr";
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .timeout(TIMEOUT, TimeUnit.MILLISECONDS)
              .param(MetricsUtil.NODE_PARAM, unknownNode)
              .method(HttpMethod.GET)
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    // Unknown host is ignored, returns the default response
    Assert.assertEquals(200, response.getStatus());
  }

  private static String readMaxOut(byte[] bytes) throws IOException {
    int max = bytes.length > MAX_OUTPUT ? MAX_OUTPUT : bytes.length;
    String str = "";
    try (ByteArrayOutputStream out = new ByteArrayOutputStream(max); ) {
      out.write(bytes, 0, max);
      str = out.toString(StandardCharsets.UTF_8);
    }
    return str;
  }
}
