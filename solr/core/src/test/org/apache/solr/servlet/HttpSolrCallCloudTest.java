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

import jakarta.servlet.UnavailableException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractFullDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.util.SuppressForbidden;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

@SolrTestCaseJ4.SuppressSSL
public class HttpSolrCallCloudTest extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARD = 3;
  private static final int REPLICA_FACTOR = 2;

  @BeforeClass
  public static void setupCluster() throws Exception {
    SolrTestCaseJ4.assumeWorkingMockito();
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "config", NUM_SHARD, REPLICA_FACTOR)
        .process(cluster.getSolrClient());
    AbstractFullDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION, cluster.getZkStateReader(), false, true, 30);
  }

  @Test
  public void testCoreChosen() throws Exception {
    assertCoreChosen(NUM_SHARD, newRequest("/collection1/update"));
    assertCoreChosen(NUM_SHARD, newRequest("/collection1/update/json"));
    assertCoreChosen(NUM_SHARD * REPLICA_FACTOR, newRequest("/collection1/select"));
  }

  // https://issues.apache.org/jira/browse/SOLR-16019
  @Test
  public void testWrongUtf8InQ() throws Exception {
    var baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    var request =
        URI.create(baseUrl.toString() + "/" + COLLECTION + "/select?q=%C0")
            .toURL(); // Illegal UTF-8 string
    var connection = (HttpURLConnection) request.openConnection();
    assertEquals(400, connection.getResponseCode());
  }

  private void assertCoreChosen(int numCores, HttpServletRequest testRequest)
      throws UnavailableException {
    JettySolrRunner jettySolrRunner = cluster.getJettySolrRunner(0);
    Set<String> coreNames = new HashSet<>();
    SolrDispatchFilter dispatchFilter = jettySolrRunner.getSolrDispatchFilter();
    for (int i = 0; i < NUM_SHARD * REPLICA_FACTOR * 20; i++) {
      if (coreNames.size() == numCores) return;
      HttpSolrCall httpSolrCall =
          new HttpSolrCall(
              dispatchFilter, dispatchFilter.getCores(), testRequest, newResponse(), false);
      try {
        httpSolrCall.init();
      } catch (Exception e) {
      } finally {
        coreNames.add(httpSolrCall.core.getName());
        httpSolrCall.destroy();
      }
    }
    assertEquals(numCores, coreNames.size());
  }

  private static HttpServletRequest newRequest(String path) {
    HttpServletRequest req = Mockito.mock(HttpServletRequest.class);
    Mockito.when(req.getMethod()).thenReturn("GET");
    Mockito.when(req.getRequestURI()).thenReturn(path);
    Mockito.when(req.getServletPath()).thenReturn(path);
    Mockito.when(req.getQueryString()).thenReturn("version=2");
    Mockito.when(req.getContentType()).thenReturn("application/json");
    Mockito.when(req.getHeader("Content-Type")).thenReturn("application/json");
    try {
      Mockito.when(req.getInputStream())
          .thenReturn(ServletUtils.ClosedServletInputStream.CLOSED_SERVLET_INPUT_STREAM);
    } catch (IOException e) { // impossible; only required because we mock methods that throw
      throw new RuntimeException(e);
    }
    return req;
  }

  @SuppressForbidden(reason = "tests needn't comply")
  private static HttpServletResponse newResponse() {
    HttpServletResponse resp = Mockito.mock(HttpServletResponse.class);
    try {
      Mockito.when(resp.getOutputStream())
          .thenReturn(ServletUtils.ClosedServletOutputStream.CLOSED_SERVLET_OUTPUT_STREAM);
      Mockito.when(resp.getWriter())
          .thenReturn(new PrintWriter(System.out, false, StandardCharsets.UTF_8));
    } catch (IOException e) { // impossible; only required because we mock methods that throw
      throw new RuntimeException(e);
    }
    return resp;
  }
}
