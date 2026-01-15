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

import jakarta.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.metrics.MetricsUtil;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GetMetricsTest extends SolrTestCaseJ4 {

  private static CoreContainer cc;
  private static Path outputPath;

  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig-minimal.xml", "schema.xml");
    h.getCoreContainer().waitForLoadingCoresToFinish(30000);
    cc = h.getCoreContainer();
    outputPath = createTempDir();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    SolrTestCaseJ4.teardownTestCases();
    try {
      Files.delete(outputPath);
    } catch (IOException e) {
      // oh, well...
    }
  }

  @Test
  public void testGetMetricsDefault() throws IOException {
    String expectedHelp = "# HELP solr_core_disk_space_megabytes";
    String expectedType = "# TYPE solr_core_disk_space_megabytes";
    String expectedDiskSpaceMetric =
        """
        solr_core_disk_space_megabytes{category="CORE",core="collection1",otel_scope_name="org.apache.solr",type="total_space"}
        """;
    SolrQueryRequest req = new SolrQueryRequestBase(h.getCore(), new ModifiableSolrParams()) {};
    SolrQueryResponse resp = new SolrQueryResponse();

    GetMetrics get = new GetMetrics(cc, req, resp);
    StreamingOutput output = get.getMetrics();
    Assert.assertNotNull(output);

    Path tmpFile = Files.createTempFile(outputPath, "test-", "-GetMetricsDefault");
    try (OutputStream tmpOut =
        Files.newOutputStream(
            tmpFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      output.write(tmpOut);
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(1024);
      String str = new String(bytes, StandardCharsets.UTF_8);
      // System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedDiskSpaceMetric.trim()));
    }
    Files.delete(tmpFile);
  }

  @Test
  public void testGetMetricsPrometheus() throws IOException {
    String expectedHelp = "# HELP solr_core_disk_space_megabytes";
    String expectedType = "# TYPE solr_core_disk_space_megabytes";
    String expectedDiskSpaceMetric =
        """
        solr_core_disk_space_megabytes{category="CORE",core="collection1",otel_scope_name="org.apache.solr",type="total_space"}
        """;
    SolrQueryRequest req = req(CommonParams.WT, MetricsUtil.PROMETHEUS_METRICS_WT);
    SolrQueryResponse resp = new SolrQueryResponse();

    GetMetrics get = new GetMetrics(cc, req, resp);
    StreamingOutput output = get.getMetrics();
    Assert.assertNotNull(output);

    Path tmpFile = Files.createTempFile(outputPath, "test-", "-GetMetricsPrometheus");
    try (OutputStream tmpOut =
        Files.newOutputStream(
            tmpFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      output.write(tmpOut);
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(1024);
      String str = new String(bytes, StandardCharsets.UTF_8);
      // System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedDiskSpaceMetric.trim()));
    }
    Files.delete(tmpFile);
  }

  @Test
  public void testGetMetricsOpenMetrics() throws IOException {
    String expectedHelp = "# HELP solr_core_disk_space_megabytes";
    String expectedType = "# TYPE solr_core_disk_space_megabytes";
    String expectedDiskSpaceMetric =
        """
        solr_core_disk_space_megabytes{category="CORE",core="collection1",otel_scope_name="org.apache.solr",type="total_space"}
        """;
    SolrQueryRequest req = req(CommonParams.WT, MetricsUtil.OPEN_METRICS_WT);
    SolrQueryResponse resp = new SolrQueryResponse();

    GetMetrics get = new GetMetrics(cc, req, resp);
    StreamingOutput output = get.getMetrics();
    Assert.assertNotNull(output);

    Path tmpFile = Files.createTempFile(outputPath, "test-", "-GetMetricsOpenMetrics");
    try (OutputStream tmpOut =
        Files.newOutputStream(
            tmpFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      output.write(tmpOut);
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(1024);
      String str = new String(bytes, StandardCharsets.UTF_8);
      // System.out.println(str);
      Assert.assertTrue(str.contains(expectedHelp));
      Assert.assertTrue(str.contains(expectedType));
      Assert.assertTrue(str.contains(expectedDiskSpaceMetric.trim()));
    }
    Files.delete(tmpFile);
  }

  @Test
  public void testGetMetricsNameParams() throws IOException {
    String requestedName = "solr_core_executor_thread_pool_size";
    SolrQueryRequest request4Name =
        new SolrQueryRequestBase(
            h.getCore(),
            new ModifiableSolrParams(
                Map.of(MetricsUtil.METRIC_NAME_PARAM, new String[] {requestedName}))) {};
    SolrQueryResponse resp = new SolrQueryResponse();

    GetMetrics get = new GetMetrics(cc, request4Name, resp);
    StreamingOutput output = get.getMetrics();
    Assert.assertNotNull(output);

    Path tmpFile = Files.createTempFile(outputPath, "test-", "-GetMetricsNameParams");
    try (OutputStream tmpOut =
        Files.newOutputStream(
            tmpFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      output.write(tmpOut);
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(1024);
      String str = new String(bytes, StandardCharsets.UTF_8);
      Assert.assertTrue(str.contains(requestedName));
      Assert.assertFalse(str.contains("solr_core_disk_space_megabytes"));
    }
    Files.delete(tmpFile);
  }

  @Test
  public void testGetMetricsCategoryParams() throws IOException {
    String expected =
        """
        solr_core_executor_thread_pool_size{category="QUERY",name="httpShardExecutor",otel_scope_name="org.apache.solr",type="core"}
        """;
    SolrQueryRequest request4Category =
        new SolrQueryRequestBase(
            h.getCore(),
            new ModifiableSolrParams(
                Map.of(MetricsUtil.CATEGORY_PARAM, new String[] {"QUERY"}))) {};
    SolrQueryResponse resp = new SolrQueryResponse();

    GetMetrics get = new GetMetrics(cc, request4Category, resp);
    StreamingOutput output = get.getMetrics();
    Assert.assertNotNull(output);

    Path tmpFile = Files.createTempFile(outputPath, "test-", "-GetMetricsCategoryParams");
    try (OutputStream tmpOut =
        Files.newOutputStream(
            tmpFile,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE)) {
      output.write(tmpOut);
    }
    try (InputStream tmpIn = Files.newInputStream(tmpFile, StandardOpenOption.READ)) {
      byte[] bytes = tmpIn.readNBytes(1024);
      String str = new String(bytes, StandardCharsets.UTF_8);
      Assert.assertTrue(str.contains(expected.trim()));
      Assert.assertFalse(str.contains("solr_core_disk_space_megabytes"));
    }
    Files.delete(tmpFile);
  }
}
