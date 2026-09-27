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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
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

  public abstract HttpSolrClient solrClient(Integer overrideIdleTimeoutMs);

  public abstract ConcurrentUpdateBaseSolrClient concurrentClient(
      HttpSolrClient solrClient,
      String baseUrl,
      String defaultCollection,
      int queueSize,
      int threadCount,
      boolean disablePollQueue);

  public abstract ConcurrentUpdateBaseSolrClient outcomeCountingConcurrentClient(
      String serverUrl,
      int queueSize,
      int threadCount,
      HttpSolrClient solrClient,
      AtomicInteger successCounter,
      AtomicInteger failureCounter,
      StringBuilder errors);

  public abstract ConcurrentUpdateBaseSolrClient errorHandlerConcurrentClient(
      String serverUrl,
      int queueSize,
      int threadCount,
      HttpSolrClient solrClient,
      ConcurrentUpdateBaseSolrClient.UpdateErrorHandler errorHandler);

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
    solrTestRule.newCollection().withConfigSet(ExternalPaths.TECHPRODUCTS_CONFIGSET).create();
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

  /**
   * Against a real Solr: a valid document succeeds and is not reported, while documents the server
   * rejects (a non-numeric value for the pint "popularity" field) are reported by id -- so only the
   * failed ids reach the handler.
   */
  @Test
  public void testFailedDocsAreRecoverableViaErrorHandler() throws Exception {
    List<String> failedIds = new CopyOnWriteArrayList<>();

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(
                solrTestRule.getBaseUrl(),
                10,
                2,
                httpClient,
                (ex, ids, collection) -> failedIds.addAll(ids))) {

      SolrInputDocument good = new SolrInputDocument();
      good.addField("id", "good-1");
      concurrentClient.add("collection1", good);
      concurrentClient.blockUntilFinished();
      assertTrue("a successful doc must not be reported: " + failedIds, failedIds.isEmpty());

      for (int i = 1; i <= 5; i++) {
        SolrInputDocument bad = new SolrInputDocument();
        bad.addField("id", "bad-" + i);
        bad.addField("popularity", "not-an-int"); // rejected: pint field
        concurrentClient.add("collection1", bad);
      }
      concurrentClient.blockUntilFinished();
    }

    assertEquals("only the 5 rejected docs should be reported: " + failedIds, 5, failedIds.size());
    for (int i = 1; i <= 5; i++) {
      assertTrue("missing bad-" + i + " in " + failedIds, failedIds.contains("bad-" + i));
    }
    assertFalse("the successful doc must not be reported", failedIds.contains("good-1"));
  }

  /** Overriding idOf reports failures by a uniqueKey field other than "id". */
  @Test
  public void testErrorHandlerWithCustomIdField() throws Exception {
    TestServlet.clear();
    TestServlet.setErrorCode(500); // every request fails server-side

    String serverUrl = solrTestRule.getBaseUrl() + "/cuss/foo";
    List<String> failedIds = new CopyOnWriteArrayList<>();
    ConcurrentUpdateBaseSolrClient.UpdateErrorHandler handler =
        new ConcurrentUpdateBaseSolrClient.UpdateErrorHandler() {
          @Override
          public void onError(Throwable ex, List<String> ids, String collection) {
            failedIds.addAll(ids);
          }

          @Override
          public String idOf(SolrInputDocument doc) {
            Object v = doc.getFieldValue("record_uuid");
            return v == null ? null : v.toString();
          }
        };

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(serverUrl, 10, 2, httpClient, handler)) {

      for (int i = 1; i <= 5; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "ignored-" + i); // not the uniqueKey the handler reads
        doc.addField("record_uuid", "uuid-" + i);
        concurrentClient.add("collection1", doc);
      }
      concurrentClient.blockUntilFinished();
    }

    assertEquals("all 5 docs should be reported by record_uuid", 5, failedIds.size());
    for (int i = 1; i <= 5; i++) {
      assertTrue("missing uuid-" + i + " in " + failedIds, failedIds.contains("uuid-" + i));
    }
  }

  /**
   * An error not tied to identifiable documents (a send failure where the document has no
   * resolvable id) reaches the general onError(Throwable) callback, not the per-id one.
   */
  @Test
  public void testGeneralErrorCallbackForNonDocumentError() throws Exception {
    String unreachable = "http://localhost:1/solr"; // nothing listening -> connect failure
    AtomicInteger richCalls = new AtomicInteger();
    AtomicInteger generalCalls = new AtomicInteger();
    ConcurrentUpdateBaseSolrClient.UpdateErrorHandler handler =
        new ConcurrentUpdateBaseSolrClient.UpdateErrorHandler() {
          @Override
          public void onError(Throwable ex, List<String> ids, String collection) {
            richCalls.incrementAndGet();
          }

          @Override
          public void onError(Throwable ex) {
            generalCalls.incrementAndGet();
          }
        };

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(unreachable, 10, 2, httpClient, handler)) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("name", "no id here"); // no id -> idOf returns null -> nothing to report per-id
      concurrentClient.add("collection1", doc);
      concurrentClient.blockUntilFinished();
    }

    assertTrue("general callback should fire for a non-document error", generalCalls.get() > 0);
    assertEquals(
        "the per-id callback should not fire for a non-document error", 0, richCalls.get());
  }

  /**
   * A connection failure surfaces the target URL so the failing server is identifiable, rather than
   * a bare socket error with no context.
   */
  @Test
  public void testConnectionErrorIncludesUrl() throws Exception {
    String unreachable = "http://127.0.0.1:1/solr"; // nothing listening -> connect failure
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    ConcurrentUpdateBaseSolrClient.UpdateErrorHandler handler =
        new ConcurrentUpdateBaseSolrClient.UpdateErrorHandler() {
          @Override
          public void onError(Throwable ex, List<String> ids, String collection) {
            errors.add(ex);
          }

          @Override
          public void onError(Throwable ex) {
            errors.add(ex);
          }
        };

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(unreachable, 10, 2, httpClient, handler)) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc-1");
      concurrentClient.add("collection1", doc);
      concurrentClient.blockUntilFinished();
    }

    assertFalse("expected a connection error to be reported", errors.isEmpty());
    boolean anyMentionsUrl =
        errors.stream()
            .anyMatch(
                e -> {
                  for (Throwable t = e; t != null; t = t.getCause()) {
                    if (t.getMessage() != null && t.getMessage().contains("127.0.0.1:1")) {
                      return true;
                    }
                  }
                  return false;
                });
    assertTrue(
        "connection error should identify the target server; got: " + errors, anyMentionsUrl);
  }

  /**
   * A connection error is reported as a SolrServerException that preserves the original cause, so
   * SolrCmdDistributor's checkRetry can unwrap it via getRootCause to decide whether to retry.
   */
  @Test
  public void testConnectionErrorIsSolrServerExceptionPreservingCause() throws Exception {
    String unreachable = "http://127.0.0.1:1/solr";
    List<Throwable> errors = new CopyOnWriteArrayList<>();
    ConcurrentUpdateBaseSolrClient.UpdateErrorHandler handler =
        new ConcurrentUpdateBaseSolrClient.UpdateErrorHandler() {
          @Override
          public void onError(Throwable ex, List<String> ids, String collection) {
            errors.add(ex);
          }

          @Override
          public void onError(Throwable ex) {
            errors.add(ex);
          }
        };

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(unreachable, 10, 2, httpClient, handler)) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("id", "doc-1");
      concurrentClient.add("collection1", doc);
      concurrentClient.blockUntilFinished();
    }

    assertFalse("expected a connection error to be reported", errors.isEmpty());
    Throwable reported = errors.get(0);
    assertTrue(
        "connection error should be wrapped as SolrServerException; got: " + reported,
        reported instanceof SolrServerException);
    assertNotNull("the original cause must be preserved", reported.getCause());
  }

  /**
   * A naive lambda handler only registers for per-id failures; an error not tied to identifiable
   * documents is logged by the default general callback and does not stop the client.
   */
  @Test
  public void testNaiveLambdaSurvivesNonDocumentError() throws Exception {
    String unreachable = "http://localhost:1/solr";
    List<String> failedIds = new CopyOnWriteArrayList<>();

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(
                unreachable, 10, 2, httpClient, (ex, ids, collection) -> failedIds.addAll(ids))) {
      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("name", "no id here");
      concurrentClient.add("collection1", doc);
      concurrentClient.blockUntilFinished(); // returns normally; default callback logged the error
    }

    assertTrue(
        "an error not tied to documents must not reach the per-id lambda: " + failedIds,
        failedIds.isEmpty());
  }

  /**
   * A handler that throws is contained and does not stop the client: every failed doc is still
   * reported and blockUntilFinished() returns normally.
   */
  @Test
  public void testThrowingErrorHandlerDoesNotStopClient() throws Exception {
    TestServlet.clear();
    TestServlet.setErrorCode(500); // every request fails server-side

    String serverUrl = solrTestRule.getBaseUrl() + "/cuss/foo";
    List<String> seenIds = new CopyOnWriteArrayList<>();

    try (var httpClient = solrClient(null);
        var concurrentClient =
            errorHandlerConcurrentClient(
                serverUrl,
                10,
                2,
                httpClient,
                (ex, ids, collection) -> {
                  seenIds.addAll(ids);
                  throw new RuntimeException("handler blew up");
                })) {

      for (int i = 1; i <= 5; i++) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "doc-" + i);
        concurrentClient.add("collection1", doc);
      }
      concurrentClient.blockUntilFinished();
    }

    for (int i = 1; i <= 5; i++) {
      assertTrue("missing doc-" + i + " in " + seenIds, seenIds.contains("doc-" + i));
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
