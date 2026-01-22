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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.metrics.MetricsUtil;
import org.apache.solr.response.PrometheusResponseWriter;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.client.ContentResponse;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.http.HttpFields.Mutable;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

/** Unit tests for {@link GetMetrics} */
public class GetMetricsTest extends SolrTestCaseJ4 {

  @ClassRule public static final SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  // no need for the full output
  private static final int MAX_OUTPUT = 1024;

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
    MiniSolrCloudCluster.Builder builder = new MiniSolrCloudCluster.Builder(2, tempDir);
    cluster = builder.withSolrXml(tempDir.resolve("solr.xml")).build();

    //    EnvUtils.setProperty(
    //        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    //    solrTestRule.startSolr(LuceneTestCase.createTempDir());
    //
    // solrTestRule.newCollection("core1").withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();
    //
    // solrTestRule.newCollection("core2").withConfigSet(ExternalPaths.DEFAULT_CONFIGSET).create();

    // SolrJettyTestRule doesn't set the V2 URL
    // solrTestRule.getBaseUrl().replace("/solr", "/api").concat("/metrics");
    metricsV2Url = cluster.getJettySolrRunner(0).getBaseURLV2().toString().concat("/metrics");
    System.out.println("Metrics URL: " + metricsV2Url);

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
    jettyHttpClient.setConnectTimeout(DEFAULT_CONNECTION_TIMEOUT);
    jettyHttpClient.setSslContextFactory(factory);
    jettyHttpClient.start();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    jettyHttpClient.stop();
    cluster.shutdown();
  }

  @Test
  public void testGetMetricsDefault()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    String expectedHelp = "# HELP solr_core_executor_thread_pool_size";
    String expectedType = "# TYPE solr_core_executor_thread_pool_size";
    String expectedMetric =
        """
        solr_core_executor_thread_pool_size{category="QUERY",name="httpShardExecutor",otel_scope_name="org.apache.solr",type="core"}
        """;

    ContentResponse response = null;
    try {
      response = jettyHttpClient.newRequest(metricsV2Url).method(HttpMethod.GET).send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());

    Path tmpFile = createTempFile();
    try (BufferedOutputStream tmpOut =
        new BufferedOutputStream(
            Files.newOutputStream(
                tmpFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
      writeMaxOut(tmpOut, response.getContent());
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(MAX_OUTPUT);
      String str = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedMetric.trim()));
    }
  }

  @Test
  public void testGetMetricsPrometheus()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    String expectedHelp = "# HELP solr_core_executor_thread_pool_size";
    String expectedType = "# TYPE solr_core_executor_thread_pool_size";
    String expectedMetric =
        """
        solr_core_executor_thread_pool_size{category="QUERY",name="httpShardExecutor",otel_scope_name="org.apache.solr",type="core"}
        """;

    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
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

    Path tmpFile = createTempFile();
    try (BufferedOutputStream tmpOut =
        new BufferedOutputStream(
            Files.newOutputStream(
                tmpFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
      writeMaxOut(tmpOut, response.getContent());
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(MAX_OUTPUT);
      String str = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedMetric.trim()));
    }
  }

  @Test
  public void testGetMetricsOpenMetrics()
      throws IOException, InterruptedException, TimeoutException, ExecutionException {
    String expectedHelp = "# HELP solr_core_executor_thread_pool_size";
    String expectedType = "# TYPE solr_core_executor_thread_pool_size";
    String expectedMetric =
        """
        solr_core_executor_thread_pool_size{category="QUERY",name="httpShardExecutor",otel_scope_name="org.apache.solr",type="core"}
        """;

    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
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

    Path tmpFile = createTempFile();
    try (BufferedOutputStream tmpOut =
        new BufferedOutputStream(
            Files.newOutputStream(
                tmpFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
      writeMaxOut(tmpOut, response.getContent());
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(MAX_OUTPUT);
      String str = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedMetric.trim()));
    }
  }

  @Test
  public void testGetMetricsNameParams() throws IOException {
    String requestedName = "solr_core_executor_thread_pool_size";

    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
              .param(MetricsUtil.METRIC_NAME_PARAM, requestedName)
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

    Path tmpFile = createTempFile();
    try (BufferedOutputStream tmpOut =
        new BufferedOutputStream(
            Files.newOutputStream(
                tmpFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
      writeMaxOut(tmpOut, response.getContent());
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(MAX_OUTPUT);
      String str = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(str);
      Assert.assertTrue(str.contains(requestedName));
      Assert.assertFalse(str.contains("solr_core_disk_space_megabytes"));
    }
  }

  @Test
  public void testGetMetricsCategoryParams() throws IOException {
    String expected =
        """
        solr_core_executor_thread_pool_size{category="QUERY",name="httpShardExecutor",otel_scope_name="org.apache.solr",type="core"}
        """;

    ContentResponse response = null;
    try {
      response =
          jettyHttpClient
              .newRequest(metricsV2Url)
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

    Path tmpFile = createTempFile();
    try (BufferedOutputStream tmpOut =
        new BufferedOutputStream(
            Files.newOutputStream(
                tmpFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
      writeMaxOut(tmpOut, response.getContent());
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(MAX_OUTPUT);
      String str = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(str);
      Assert.assertTrue(str.contains(expected.trim()));
      Assert.assertFalse(str.contains("solr_core_disk_space_megabytes"));
    }
  }

  @Test
  @Ignore("Only supports V1")
  public void testGetMetricsProxyToNode() throws IOException {
    String expectedHelp = "# HELP solr_core_executor_thread_pool_size";
    String expectedType = "# TYPE solr_core_executor_thread_pool_size";
    String expectedDiskSpaceMetric =
        """
        solr_core_executor_thread_pool_size{category="CORE",core="collection2",otel_scope_name="org.apache.solr",type="total_space"}
        """;
    // TODO: fix AdminHandlersProxy to support V2
    URL otherUrl = cluster.getJettySolrRunner(1).getBaseURLV2(); // fails and hangs
    String otherNode = otherUrl.getHost() + ":" + otherUrl.getPort() + "_solr";
    ContentResponse response = null;
    try {
      // request to V1 can work, but this test should be for V2
      response =
          jettyHttpClient
              .newRequest(
                  cluster.getJettySolrRunner(0).getBaseUrl().toString().concat("/admin/metrics"))
              .param(MetricsUtil.NODE_PARAM, otherNode)
              .method(HttpMethod.GET)
              .send();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      Assert.fail("Should not throw exception: " + e.getClass() + ".  message: " + e.getMessage());
      return;
    }
    Assert.assertEquals(200, response.getStatus());

    Path tmpFile = createTempFile();
    try (BufferedOutputStream tmpOut =
        new BufferedOutputStream(
            Files.newOutputStream(
                tmpFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE))) {
      writeMaxOut(tmpOut, response.getContent());
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(MAX_OUTPUT);
      String str = new String(bytes, StandardCharsets.UTF_8);
      System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedDiskSpaceMetric.trim()));
    }
  }

  private static void writeMaxOut(OutputStream tmpOut, byte[] bytes) throws IOException {
    int max = bytes.length > MAX_OUTPUT ? MAX_OUTPUT : bytes.length;
    tmpOut.write(bytes, 0, max);
  }
}
