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

import static org.apache.solr.core.CoreContainer.ALLOW_PATHS_SYSPROP;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.net.http.HttpConnectTimeoutException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.JavaBinUpdateRequestCodec;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.EnvUtils;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.ExternalPaths;
import org.apache.solr.util.SolrJettyTestRule;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConcurrentUpdateSolrClientTestBase extends SolrTestCaseJ4 {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public abstract HttpSolrClientBase solrClient(Integer overrideIdleTimeoutMs);

  public abstract ConcurrentUpdateBaseSolrClient concurrentClient(
      HttpSolrClientBase solrClient,
      String baseUrl,
      String defaultCollection,
      int queueSize,
      int threadCount,
      boolean disablePollQueue);

  public abstract ConcurrentUpdateBaseSolrClient outcomeCountingConcurrentClient(
      String serverUrl,
      int queueSize,
      int threadCount,
      HttpSolrClientBase solrClient,
      AtomicInteger successCounter,
      AtomicInteger failureCounter,
      StringBuilder errors);

  /** Mock endpoint where the CUSS being tested in this class sends requests. */
  public static class TestServlet extends HttpServlet
      implements JavaBinUpdateRequestCodec.StreamingUpdateHandler {
    private static final long serialVersionUID = 1L;

    public static void clear() {
      lastMethod = null;
      headers = null;
      parameters = null;
      errorCode = null;
      numReqsRcvd.set(0);
      numDocsRcvd.set(0);
    }

    public static Integer errorCode = null;
    public static String lastMethod = null;
    public static HashMap<String, String> headers = null;
    public static Map<String, String[]> parameters = null;
    public static AtomicInteger numReqsRcvd = new AtomicInteger(0);
    public static AtomicInteger numDocsRcvd = new AtomicInteger(0);

    public static void setErrorCode(Integer code) {
      errorCode = code;
    }

    private void setHeaders(HttpServletRequest req) {
      Enumeration<String> headerNames = req.getHeaderNames();
      headers = new HashMap<>();
      while (headerNames.hasMoreElements()) {
        final String name = headerNames.nextElement();
        headers.put(name, req.getHeader(name));
      }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      numReqsRcvd.incrementAndGet();
      lastMethod = "post";
      recordRequest(req, resp);

      InputStream reqIn = req.getInputStream();
      JavaBinUpdateRequestCodec javabin = new JavaBinUpdateRequestCodec();
      for (; ; ) {
        try {
          javabin.unmarshal(reqIn, this);
        } catch (EOFException e) {
          break; // this is expected
        }
      }
    }

    private void recordRequest(HttpServletRequest req, HttpServletResponse resp) {
      setHeaders(req);
      if (null != errorCode) {
        try {
          resp.sendError(errorCode);
        } catch (IOException e) {
          throw new RuntimeException("sendError IO fail in TestServlet", e);
        }
      }
    }

    @Override
    public void update(
        SolrInputDocument document, UpdateRequest req, Integer commitWithin, Boolean override) {
      numDocsRcvd.incrementAndGet();
    }
  } // end TestServlet

  static class SendDocsRunnable implements Runnable {

    private String id;
    private int numDocs;
    private SolrClient cuss;
    private String collection;

    SendDocsRunnable(String id, int numDocs, SolrClient cuss) {
      this(id, numDocs, cuss, null);
    }

    SendDocsRunnable(String id, int numDocs, SolrClient cuss, String collection) {
      this.id = id;
      this.numDocs = numDocs;
      this.cuss = cuss;
      this.collection = collection;
    }

    @Override
    public void run() {
      for (int d = 0; d < numDocs; d++) {
        SolrInputDocument doc = new SolrInputDocument();
        String docId = id + "_" + d;
        doc.setField("id", docId);
        UpdateRequest req = new UpdateRequest();
        req.add(doc);
        try {
          if (this.collection == null) cuss.request(req);
          else cuss.request(req, this.collection);
        } catch (Throwable t) {
          log.error("error making request", t);
        }
      }
    }
  }

  @ClassRule public static SolrJettyTestRule solrTestRule = new SolrJettyTestRule();

  @BeforeClass
  public static void beforeTest() throws Exception {
    JettyConfig jettyConfig =
        JettyConfig.builder().withServlet(new ServletHolder(TestServlet.class), "/cuss/*").build();

    EnvUtils.setProperty(
        ALLOW_PATHS_SYSPROP, ExternalPaths.SERVER_HOME.toAbsolutePath().toString());
    solrTestRule.startSolr(createTempDir(), new Properties(), jettyConfig);
    solrTestRule
        .newCollection(DEFAULT_TEST_COLLECTION_NAME)
        .withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET)
        .create();
  }

  @AfterClass
  public static void afterTest() throws Exception {}

  @Test
  public void testConcurrentUpdate() throws Exception {
    TestServlet.clear();

    String serverUrl = solrTestRule.getBaseUrl() + "/cuss/foo";

    int cussThreadCount = 2;
    int cussQueueSize = 100;

    // for tracking callbacks from CUSS
    final AtomicInteger successCounter = new AtomicInteger(0);
    final AtomicInteger errorCounter = new AtomicInteger(0);
    final StringBuilder errors = new StringBuilder();

    try (var http2Client = solrClient(null);
        var concurrentClient =
            outcomeCountingConcurrentClient(
                serverUrl,
                cussQueueSize,
                cussThreadCount,
                http2Client,
                successCounter,
                errorCounter,
                errors)) {

      // ensure it doesn't block where there's nothing to do yet
      concurrentClient.blockUntilFinished();

      int poolSize = 5;
      ExecutorService threadPool =
          ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("testCUSS"));

      int numDocs = 100;
      int numRunnables = 5;
      for (int r = 0; r < numRunnables; r++)
        threadPool.execute(new SendDocsRunnable(String.valueOf(r), numDocs, concurrentClient));

      // ensure all docs are sent
      threadPool.awaitTermination(5, TimeUnit.SECONDS);
      threadPool.shutdown();

      // wait until all requests are processed by CUSS
      concurrentClient.blockUntilFinished();
      concurrentClient.shutdownNow();

      assertEquals("post", TestServlet.lastMethod);

      // expect all requests to be successful
      int expectedSuccesses = TestServlet.numReqsRcvd.get();
      assertTrue(expectedSuccesses > 0); // at least one request must have been sent

      assertEquals(
          "Expected no errors but got " + errorCounter.get() + ", due to: " + errors.toString(),
          0,
          errorCounter.get());
      assertEquals(
          "Expected " + expectedSuccesses + " successes, but got " + successCounter.get(),
          successCounter.get(),
          expectedSuccesses);

      int expectedDocs = numDocs * numRunnables;
      assertEquals(
          "Expected CUSS to send " + expectedDocs + " but got " + TestServlet.numDocsRcvd.get(),
          TestServlet.numDocsRcvd.get(),
          expectedDocs);
    }
  }

  @Test
  public void testCollectionParameters() throws IOException, SolrServerException {

    int cussThreadCount = 2;
    int cussQueueSize = 10;

    try (var http2Client = solrClient(null);
        var concurrentClient =
            concurrentClient(
                http2Client,
                solrTestRule.getBaseUrl(),
                null,
                cussQueueSize,
                cussThreadCount,
                false)) {

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "collection");
      concurrentClient.add("collection1", doc);
      concurrentClient.commit("collection1");

      assertEquals(
          1,
          concurrentClient
              .query("collection1", new SolrQuery("id:collection"))
              .getResults()
              .getNumFound());
    }

    try (var http2Client = solrClient(null);
        var concurrentClient =
            concurrentClient(
                http2Client,
                solrTestRule.getBaseUrl(),
                DEFAULT_TEST_CORENAME,
                cussQueueSize,
                cussThreadCount,
                false)) {

      assertEquals(
          1, concurrentClient.query(new SolrQuery("id:collection")).getResults().getNumFound());
    }
  }

  @Test
  public void testConcurrentCollectionUpdate() throws Exception {

    int cussThreadCount = 2;
    int cussQueueSize = 100;
    int numDocs = 100;
    int numRunnables = 5;
    int expected = numDocs * numRunnables;

    try (var http2Client = solrClient(null);
        var concurrentClient =
            concurrentClient(
                http2Client,
                solrTestRule.getBaseUrl(),
                null,
                cussQueueSize,
                cussThreadCount,
                true)) {

      // ensure it doesn't block where there's nothing to do yet
      concurrentClient.blockUntilFinished();

      // Delete all existing documents.
      concurrentClient.deleteByQuery("collection1", "*:*");

      int poolSize = 5;
      ExecutorService threadPool =
          ExecutorUtil.newMDCAwareFixedThreadPool(poolSize, new SolrNamedThreadFactory("testCUSS"));

      for (int r = 0; r < numRunnables; r++)
        threadPool.execute(
            new SendDocsRunnable(String.valueOf(r), numDocs, concurrentClient, "collection1"));

      // ensure all docs are sent
      threadPool.awaitTermination(5, TimeUnit.SECONDS);
      threadPool.shutdown();

      concurrentClient.commit("collection1");

      assertEquals(
          expected,
          concurrentClient.query("collection1", new SolrQuery("*:*")).getResults().getNumFound());

      // wait until all requests are processed by CUSS
      concurrentClient.blockUntilFinished();
      concurrentClient.shutdownNow();
    }

    try (var http2Client = solrClient(null);
        var concurrentClient =
            concurrentClient(
                http2Client,
                solrTestRule.getBaseUrl(),
                DEFAULT_TEST_CORENAME,
                cussQueueSize,
                cussThreadCount,
                false)) {

      assertEquals(
          expected, concurrentClient.query(new SolrQuery("*:*")).getResults().getNumFound());
    }
  }

  /**
   * Test that connection timeout information is passed to the HttpSolrClient that handles non add
   * operations.
   */
  @Test(timeout = 10000)
  public void testSocketTimeoutOnCommit() throws IOException, SolrServerException {
    InetAddress localHost = InetAddress.getLocalHost();
    try (ServerSocket server = new ServerSocket(0, 1, localHost);
        var http2Client = solrClient(1);
        var client =
            concurrentClient(
                http2Client,
                "http://"
                    + localHost.getHostAddress()
                    + ":"
                    + server.getLocalPort()
                    + "/noOneThere",
                null,
                10,
                1,
                false)) {
      // Expecting an exception
      client.commit();
      fail();
    } catch (SolrServerException e) {
      if (!(e.getCause() instanceof SocketTimeoutException // not sure if Jetty throws this
          || e.getCause() instanceof TimeoutException // Jetty throws this
          || e.getCause() instanceof HttpConnectTimeoutException // jdk client throws this
      )) {
        throw e;
      }
      // else test passes
    }
  }
}
