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

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.UnavailableException;
import javax.servlet.WriteListener;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.embedded.JettySolrRunner;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class HttpSolrCallCloudTest extends SolrCloudTestCase {
  private static final String COLLECTION = "collection1";
  private static final int NUM_SHARD = 3;
  private static final int REPLICA_FACTOR = 2;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1)
        .addConfig(
            "config", TEST_PATH().resolve("configsets").resolve("cloud-minimal").resolve("conf"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTION, "config", NUM_SHARD, REPLICA_FACTOR)
        .process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(
        COLLECTION, cluster.getZkStateReader(), false, true, 30);
  }

  @Test
  public void testCoreChosen() throws Exception {
    assertCoreChosen(NUM_SHARD, new TestRequest("/collection1/update"));
    assertCoreChosen(NUM_SHARD, new TestRequest("/collection1/update/json"));
    assertCoreChosen(NUM_SHARD * REPLICA_FACTOR, new TestRequest("/collection1/select"));
  }

  // https://issues.apache.org/jira/browse/SOLR-16019
  @Test
  public void testWrongUtf8InQ() throws Exception {
    var baseUrl = cluster.getJettySolrRunner(0).getBaseUrl();
    var request =
        new URL(baseUrl.toString() + "/" + COLLECTION + "/select?q=%C0"); // Illegal UTF-8 string
    var connection = (HttpURLConnection) request.openConnection();
    assertEquals(400, connection.getResponseCode());
  }

  private void assertCoreChosen(int numCores, TestRequest testRequest) throws UnavailableException {
    JettySolrRunner jettySolrRunner = cluster.getJettySolrRunner(0);
    Set<String> coreNames = new HashSet<>();
    SolrDispatchFilter dispatchFilter = jettySolrRunner.getSolrDispatchFilter();
    for (int i = 0; i < NUM_SHARD * REPLICA_FACTOR * 20; i++) {
      if (coreNames.size() == numCores) return;
      HttpSolrCall httpSolrCall =
          new HttpSolrCall(
              dispatchFilter, dispatchFilter.getCores(), testRequest, new TestResponse(), false);
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

  private static class TestResponse extends Response {

    public TestResponse() {
      super(null, null);
    }

    @Override
    public ServletOutputStream getOutputStream() {
      return new ServletOutputStream() {
        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {}

        @Override
        public void write(int b) {}
      };
    }

    @Override
    public boolean isCommitted() {
      return true;
    }
  }

  private static class TestRequest extends Request {
    private String path;

    public TestRequest(String path) {
      super(null, null);
      this.path = path;
    }

    @Override
    public String getQueryString() {
      return "version=2";
    }

    @Override
    public String getContentType() {
      return "application/json";
    }

    @Override
    public String getServletPath() {
      return path;
    }

    @Override
    public String getRequestURI() {
      return path;
    }

    @Override
    public ServletInputStream getInputStream() {
      return new ServletInputStream() {
        @Override
        public boolean isFinished() {
          return true;
        }

        @Override
        public boolean isReady() {
          return true;
        }

        @Override
        public void setReadListener(ReadListener readListener) {}

        @Override
        public int read() {
          return 0;
        }
      };
    }
  }
}
