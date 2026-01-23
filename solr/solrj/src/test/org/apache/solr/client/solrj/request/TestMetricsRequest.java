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
package org.apache.solr.client.solrj.request;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.commons.io.file.PathUtils;
import org.apache.solr.client.solrj.SolrClient.SolrClientFunction;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.client.solrj.response.InputStreamResponseParser;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.metrics.MetricsUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/** Test {@link MetricsRequest}. */
public class TestMetricsRequest extends SolrCloudTestCase {

  private static final String METRICS_V2_PATH = "/metrics";

  private static HttpJettySolrClient httpClient;
  private static MiniSolrCloudCluster cluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Path tempDir = createTempDir();
    Path testFilesDir = getFile("solrj/solr/collection1").getParent();
    PathUtils.copyDirectory(testFilesDir, tempDir);
    Files.copy(
        testFilesDir.resolve("solr-metrics-enabled.xml"),
        tempDir.resolve("solr.xml"),
        StandardCopyOption.REPLACE_EXISTING);
    MiniSolrCloudCluster.Builder clusterBuilder = new MiniSolrCloudCluster.Builder(2, tempDir);
    cluster = clusterBuilder.withSolrXml(tempDir.resolve("solr.xml")).build();

    HttpJettySolrClient.Builder clientBuilder =
        new HttpJettySolrClient.Builder(cluster.getJettySolrRunner(0).getBaseUrl().toString());
    httpClient = clientBuilder.build();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    httpClient.close();
    cluster.shutdown();
  }

  @Test
  public void testGetMetricsV2()
      throws IOException,
          InterruptedException,
          ExecutionException,
          TimeoutException,
          SolrServerException {
    MetricsRequest solrRequest = new MetricsRequest(METRICS_V2_PATH);
    String str =
        httpClient.requestWithBaseUrl(
            cluster.getJettySolrRunner(0).getBaseURLV2().toString(),
            new SolrClientFunction<HttpJettySolrClient, String>() {

              @Override
              public String apply(HttpJettySolrClient c) throws IOException, SolrServerException {
                return InputStreamResponseParser.consumeResponseToString(c.request(solrRequest));
              }
            });

    // System.out.println("testGetMetricsV2: " + str);
    Assert.assertTrue(str.contains("# HELP"));
    Assert.assertTrue(str.contains("# TYPE"));
  }

  @Test
  public void testGetMetricsV2ParamName()
      throws IOException,
          InterruptedException,
          ExecutionException,
          TimeoutException,
          SolrServerException {
    String requestedName = "solr_disk_space_megabytes";
    String notRequested = "solr_client_request_duration_milliseconds_bucket";
    SolrParams params =
        new ModifiableSolrParams(
            Map.of(MetricsUtil.METRIC_NAME_PARAM, new String[] {requestedName}));
    MetricsRequest solrRequest = new MetricsRequest(METRICS_V2_PATH, params);
    String str =
        httpClient.requestWithBaseUrl(
            cluster.getJettySolrRunner(0).getBaseURLV2().toString(),
            new SolrClientFunction<HttpJettySolrClient, String>() {

              @Override
              public String apply(HttpJettySolrClient c) throws IOException, SolrServerException {
                return InputStreamResponseParser.consumeResponseToString(c.request(solrRequest));
              }
            });

    System.out.println("testGetMetricsV2ParamName: " + str);
    Assert.assertTrue(str.contains("# HELP"));
    Assert.assertTrue(str.contains("# TYPE"));
    Assert.assertTrue(str.contains(requestedName));
    Assert.assertFalse(str.contains(notRequested));
  }

  @Test
  public void testGetMetricsV1()
      throws IOException,
          InterruptedException,
          ExecutionException,
          TimeoutException,
          SolrServerException {
    MetricsRequest solrRequest = new MetricsRequest();
    String str =
        httpClient.requestWithBaseUrl(
            cluster.getJettySolrRunner(0).getBaseUrl().toString(),
            new SolrClientFunction<HttpJettySolrClient, String>() {

              @Override
              public String apply(HttpJettySolrClient c) throws IOException, SolrServerException {
                return InputStreamResponseParser.consumeResponseToString(c.request(solrRequest));
              }
            });

    // System.out.println("testGetMetricsV1: " + str);
    Assert.assertTrue(str.contains("# HELP"));
    Assert.assertTrue(str.contains("# TYPE"));
  }
}
