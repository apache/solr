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

import static org.apache.solr.common.params.CommonParams.SOLR_REQUEST_CONTEXT_PARAM;
import static org.apache.solr.common.params.CommonParams.SOLR_REQUEST_TYPE_PARAM;
import static org.apache.solr.servlet.RateLimitManager.DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import javax.servlet.http.HttpServletRequest;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.core.RateLimiterConfig;
import org.eclipse.jetty.server.Request;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRequestRateLimiter extends SolrCloudTestCase {
  private static final String FIRST_COLLECTION = "c1";
  private static final String SECOND_COLLECTION = "c2";

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(1).addConfig(FIRST_COLLECTION, configset("cloud-minimal")).configure();
  }

  @Test
  public void testConcurrentQueries() throws Exception {
    try (CloudSolrClient client =
        cluster.basicSolrClientBuilder().withDefaultCollection(FIRST_COLLECTION).build()) {

      CollectionAdminRequest.createCollection(FIRST_COLLECTION, 1, 1).process(client);
      cluster.waitForActiveCollection(FIRST_COLLECTION, 1, 1);

      SolrDispatchFilter solrDispatchFilter = cluster.getJettySolrRunner(0).getSolrDispatchFilter();

      RateLimiterConfig rateLimiterConfig =
          new RateLimiterConfig(
              SolrRequest.SolrRequestType.QUERY,
              true,
              1,
              DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS,
              5 /* allowedRequests */,
              true /* isSlotBorrowing */);
      // We are fine with a null FilterConfig here since we ensure that MockBuilder never invokes
      // its parent here
      RateLimitManager.Builder builder =
          new MockBuilder(
              null /* dummy SolrZkClient */, new MockRequestRateLimiter(rateLimiterConfig));
      RateLimitManager rateLimitManager = builder.build();

      solrDispatchFilter.replaceRateLimitManager(rateLimitManager);

      int numDocs = TEST_NIGHTLY ? 10000 : 100;

      processTest(client, numDocs, 350 /* number of queries */);

      MockRequestRateLimiter mockQueryRateLimiter =
          (MockRequestRateLimiter)
              rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY);

      assertEquals(350, mockQueryRateLimiter.incomingRequestCount.get());

      assertTrue(mockQueryRateLimiter.acceptedNewRequestCount.get() > 0);
      assertTrue(
          (mockQueryRateLimiter.acceptedNewRequestCount.get()
                  == mockQueryRateLimiter.incomingRequestCount.get()
              || mockQueryRateLimiter.rejectedRequestCount.get() > 0));
      assertEquals(
          mockQueryRateLimiter.incomingRequestCount.get(),
          mockQueryRateLimiter.acceptedNewRequestCount.get()
              + mockQueryRateLimiter.rejectedRequestCount.get());
    }
  }

  @Test
  @SuppressWarnings("try")
  public void testSlotBorrowingAcquisitionTimeout()
      throws InterruptedException, IOException, ExecutionException {
    RateLimitManager mgr = new RateLimitManager();
    Random r = random();
    int slotLimit = r.nextInt(20) + 1;
    int guaranteed = r.nextInt(slotLimit);
    int slotAcqTimeMillis = 1000; // 1 second -- large enough to be reliably measurable
    RateLimiterConfig queryConfig =
        new RateLimiterConfig(
            SolrRequest.SolrRequestType.QUERY,
            true,
            guaranteed,
            slotAcqTimeMillis,
            slotLimit /* allowedRequests */,
            true /* isSlotBorrowing */);
    // set allowed/guaranteed to the same, and very low, to force it to mainly borrow. It would also
    // be theoretically possible to optimize a single-request-type config to bypass slot-borrowing
    // logic altogether, so configuring a second ratelimiter eliminates the possibility that at
    // some point the test could come to not evaluate what it's intended to evaluate.
    RateLimiterConfig updateConfig =
        new RateLimiterConfig(
            SolrRequest.SolrRequestType.UPDATE,
            true,
            1,
            slotAcqTimeMillis,
            1 /* allowedRequests */,
            true /* isSlotBorrowing */);
    mgr.registerRequestRateLimiter(
        new RequestRateLimiter(queryConfig), SolrRequest.SolrRequestType.QUERY);
    mgr.registerRequestRateLimiter(
        new RequestRateLimiter(updateConfig), SolrRequest.SolrRequestType.UPDATE);

    RequestRateLimiter.SlotReservation[] acquired =
        new RequestRateLimiter.SlotReservation[slotLimit + 1];
    long threshold = TimeUnit.MILLISECONDS.toNanos(slotAcqTimeMillis);

    long waitNanos = TimeUnit.MILLISECONDS.toNanos(slotAcqTimeMillis);

    ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool("slotBorrowing");
    List<Future<?>> futures = new ArrayList<>(slotLimit + 1);
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec)) {
      CountDownLatch cdl = new CountDownLatch(slotLimit);
      for (int i = 0; i < slotLimit; i++) {
        int idx = i;
        futures.add(
            exec.submit(
                () -> {
                  try {
                    long start = System.nanoTime();
                    RequestRateLimiter.SlotReservation res = mgr.handleRequest(QUERY_REQ);
                    assertNotNull(res);
                    acquired[idx] = res;
                    // we should never have to wait to acquire a slot.
                    assertTrue(System.nanoTime() - start < threshold);
                  } finally {
                    cdl.countDown();
                  }
                  return null;
                }));
      }

      cdl.await();

      for (Future<?> f : futures) {
        f.get();
      }

      futures.clear();

      long start = System.nanoTime();
      assertNull(mgr.handleRequest(QUERY_REQ)); // we shouldn't acquire a slot
      assertTrue(System.nanoTime() - start > waitNanos); // we should have waited a while though!

      for (int i = 0; i < slotLimit; i++) {
        acquired[i].close();
      }

      assertTrue(mgr.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY).isEmpty());
      assertTrue(mgr.getRequestRateLimiter(SolrRequest.SolrRequestType.UPDATE).isEmpty());

      long borrowThreshold = waitNanos + threshold;
      int otherAcquire = slotLimit - guaranteed + 1;
      CountDownLatch otherLatch = new CountDownLatch(otherAcquire);
      for (int i = 0; i < otherAcquire; i++) {
        int idx = i;
        futures.add(
            exec.submit(
                () -> {
                  try {
                    long startL = System.nanoTime();
                    RequestRateLimiter.SlotReservation res = mgr.handleRequest(UPDATE_REQ);
                    assertNotNull(res);
                    acquired[idx] = res;
                    // we should never have to wait to acquire a slot -- borrow many of these
                    long waited = System.nanoTime() - startL;
                    assertTrue(
                        idx + " waited " + TimeUnit.NANOSECONDS.toMillis(waited) + "ms",
                        waited < borrowThreshold);
                  } finally {
                    otherLatch.countDown();
                  }
                  return null;
                }));
      }

      otherLatch.await();

      for (Future<?> f : futures) {
        f.get();
      }

      futures.clear();

      start = System.nanoTime();
      assertNull(mgr.handleRequest(UPDATE_REQ)); // no more borrowable slots!
      long waited = System.nanoTime() - start;
      assertTrue(
          "waited " + TimeUnit.NANOSECONDS.toMillis(waited) + "ms",
          waited > waitNanos); // we should have waited a while though!

      CountDownLatch guaranteedLatch = new CountDownLatch(slotLimit - otherAcquire + 1);
      for (int i = otherAcquire; i <= slotLimit; i++) {
        int idx = i;
        futures.add(
            exec.submit(
                () -> {
                  try {
                    long startL = System.nanoTime();
                    RequestRateLimiter.SlotReservation res = mgr.handleRequest(QUERY_REQ);
                    assertNotNull(res);
                    acquired[idx] = res;
                    // we should never have to wait to acquire guaranteed slots
                    assertTrue(System.nanoTime() - startL < threshold);
                  } finally {
                    guaranteedLatch.countDown();
                  }
                  return null;
                }));
      }

      guaranteedLatch.await();

      for (Future<?> f : futures) {
        f.get();
      }
    }

    long start = System.nanoTime();
    assertNull(mgr.handleRequest(QUERY_REQ)); // slots are all gone!
    assertTrue(System.nanoTime() - start > waitNanos); // we should have waited a while though!

    // now cleanup
    for (RequestRateLimiter.SlotReservation res : acquired) {
      res.close();
    }

    assertTrue(mgr.getRequestRateLimiter(SolrRequest.SolrRequestType.QUERY).isEmpty());
    assertTrue(mgr.getRequestRateLimiter(SolrRequest.SolrRequestType.UPDATE).isEmpty());
  }

  private static final HttpServletRequest QUERY_REQ = new DummyRequest(null, "QUERY");
  private static final HttpServletRequest UPDATE_REQ = new DummyRequest(null, "UPDATE");

  private static class DummyRequest extends Request {

    private final String ctx;
    private final String type;

    public DummyRequest(String ctx, String type) {
      super(null, null);
      this.ctx = ctx;
      this.type = type;
    }

    @Override
    public String getHeader(String name) {
      switch (name) {
        case SOLR_REQUEST_CONTEXT_PARAM:
          return ctx;
        case SOLR_REQUEST_TYPE_PARAM:
          return type;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  @Nightly
  public void testSlotBorrowing() throws Exception {
    try (CloudSolrClient client =
        cluster.basicSolrClientBuilder().withDefaultCollection(SECOND_COLLECTION).build()) {

      CollectionAdminRequest.createCollection(SECOND_COLLECTION, 1, 1).process(client);
      cluster.waitForActiveCollection(SECOND_COLLECTION, 1, 1);

      SolrDispatchFilter solrDispatchFilter = cluster.getJettySolrRunner(0).getSolrDispatchFilter();

      RateLimiterConfig queryRateLimiterConfig =
          new RateLimiterConfig(
              SolrRequest.SolrRequestType.QUERY,
              true,
              1,
              DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS,
              5 /* allowedRequests */,
              true /* isSlotBorrowing */);
      RateLimiterConfig indexRateLimiterConfig =
          new RateLimiterConfig(
              SolrRequest.SolrRequestType.UPDATE,
              true,
              1,
              DEFAULT_SLOT_ACQUISITION_TIMEOUT_MS,
              5 /* allowedRequests */,
              true /* isSlotBorrowing */);
      // We are fine with a null FilterConfig here since we ensure that MockBuilder never invokes
      // its parent
      RateLimitManager.Builder builder =
          new MockBuilder(
              null /*dummy SolrZkClient */,
              new MockRequestRateLimiter(queryRateLimiterConfig),
              new MockRequestRateLimiter(indexRateLimiterConfig));
      RateLimitManager rateLimitManager = builder.build();

      solrDispatchFilter.replaceRateLimitManager(rateLimitManager);

      int numDocs = 10000;

      processTest(client, numDocs, 400 /* Number of queries */);

      MockRequestRateLimiter mockIndexRateLimiter =
          (MockRequestRateLimiter)
              rateLimitManager.getRequestRateLimiter(SolrRequest.SolrRequestType.UPDATE);

      assertTrue(
          "Incoming slots borrowed count did not match. Expected > 0  incoming "
              + mockIndexRateLimiter.borrowedSlotCount.get(),
          mockIndexRateLimiter.borrowedSlotCount.get() > 0);
    }
  }

  private void processTest(SolrClient client, int numDocuments, int numQueries) throws Exception {

    for (int i = 0; i < numDocuments; i++) {
      SolrInputDocument doc = new SolrInputDocument();

      doc.setField("id", i);
      doc.setField("text", "foo");
      client.add(doc);
    }

    client.commit();

    ExecutorService executor = ExecutorUtil.newMDCAwareCachedThreadPool("threadpool");
    List<Callable<Boolean>> callableList = new ArrayList<>();
    List<Future<Boolean>> futures;

    try {
      for (int i = 0; i < numQueries; i++) {
        callableList.add(
            () -> {
              try {
                QueryResponse response = client.query(new SolrQuery("*:*"));

                assertEquals(numDocuments, response.getResults().getNumFound());
              } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
              }

              return true;
            });
      }

      futures = executor.invokeAll(callableList);

      for (Future<?> future : futures) {
        try {
          assertNotNull(future.get());
        } catch (ExecutionException e) {
          assertThat(e.getCause().getCause(), instanceOf(RemoteSolrException.class));
          RemoteSolrException rse = (RemoteSolrException) e.getCause().getCause();
          assertEquals(SolrException.ErrorCode.TOO_MANY_REQUESTS.code, rse.code());
          assertThat(
              rse.getMessage(), containsString("non ok status: 429, message:Too Many Requests"));
        }
      }
    } finally {
      executor.shutdown();
    }
  }

  private static class MockRequestRateLimiter extends RequestRateLimiter {
    final AtomicInteger incomingRequestCount;
    final AtomicInteger acceptedNewRequestCount;
    final AtomicInteger rejectedRequestCount;
    final AtomicInteger borrowedSlotCount;

    public MockRequestRateLimiter(RateLimiterConfig config) {
      super(config);

      this.incomingRequestCount = new AtomicInteger(0);
      this.acceptedNewRequestCount = new AtomicInteger(0);
      this.rejectedRequestCount = new AtomicInteger(0);
      this.borrowedSlotCount = new AtomicInteger(0);
    }

    @Override
    public SlotReservation handleRequest() throws InterruptedException {
      incomingRequestCount.getAndIncrement();

      SlotReservation response = super.handleRequest();

      if (response != null) {
        acceptedNewRequestCount.getAndIncrement();
      } else {
        rejectedRequestCount.getAndIncrement();
      }

      return response;
    }

    @Override
    public SlotReservation allowSlotBorrowing() throws InterruptedException {
      SlotReservation result = super.allowSlotBorrowing();

      if (result != null) {
        borrowedSlotCount.incrementAndGet();
      }

      return result;
    }
  }

  private static class MockBuilder extends RateLimitManager.Builder {
    private final RequestRateLimiter queryRequestRateLimiter;
    private final RequestRateLimiter indexRequestRateLimiter;

    public MockBuilder(SolrZkClient zkClient, RequestRateLimiter queryRequestRateLimiter) {
      super(zkClient);

      this.queryRequestRateLimiter = queryRequestRateLimiter;
      this.indexRequestRateLimiter = null;
    }

    public MockBuilder(
        SolrZkClient zkClient,
        RequestRateLimiter queryRequestRateLimiter,
        RequestRateLimiter indexRequestRateLimiter) {
      super(zkClient);

      this.queryRequestRateLimiter = queryRequestRateLimiter;
      this.indexRequestRateLimiter = indexRequestRateLimiter;
    }

    @Override
    public RateLimitManager build() {
      RateLimitManager rateLimitManager = new RateLimitManager();

      rateLimitManager.registerRequestRateLimiter(
          queryRequestRateLimiter, SolrRequest.SolrRequestType.QUERY);

      if (indexRequestRateLimiter != null) {
        rateLimitManager.registerRequestRateLimiter(
            indexRequestRateLimiter, SolrRequest.SolrRequestType.UPDATE);
      }

      return rateLimitManager;
    }
  }

  @Test
  @SuppressWarnings("try")
  public void testAdjustingConfig() throws IOException, InterruptedException {
    Random r = random();
    int maxAllowed = 32;
    int allowed = r.nextInt(maxAllowed) + 1;
    int guaranteed = r.nextInt(allowed + 1);
    int borrowLimit = allowed - guaranteed;
    RateLimiterConfig config =
        new RateLimiterConfig(
            SolrRequest.SolrRequestType.QUERY,
            true,
            guaranteed,
            20,
            allowed /* allowedRequests */,
            true /* isSlotBorrowing */);
    RequestRateLimiter limiter = new RequestRateLimiter(config);
    ExecutorService exec = ExecutorUtil.newMDCAwareCachedThreadPool("tests");
    try (Closeable c = () -> ExecutorUtil.shutdownAndAwaitTermination(exec)) {
      for (int j = 0; j < 5; j++) {
        int allowedF = allowed;
        int borrowLimitF = borrowLimit;
        RequestRateLimiter limiterF = limiter;
        AtomicBoolean finish = new AtomicBoolean();
        AtomicInteger outstanding = new AtomicInteger();
        AtomicInteger outstandingBorrowed = new AtomicInteger();
        LongAdder executed = new LongAdder();
        LongAdder skipped = new LongAdder();
        LongAdder borrowedExecuted = new LongAdder();
        LongAdder borrowedSkipped = new LongAdder();
        List<Future<Void>> futures = new ArrayList<>();
        int nativeClients = r.nextInt(allowed << 1);
        for (int i = nativeClients; i > 0; i--) {
          Random tRandom = new Random(r.nextLong());
          futures.add(
              exec.submit(
                  () -> {
                    while (!finish.get()) {
                      try (RequestRateLimiter.SlotReservation slotReservation =
                          limiterF.handleRequest()) {
                        if (slotReservation != null) {
                          executed.increment();
                          int ct = outstanding.incrementAndGet();
                          assertTrue(ct + " <= " + allowedF, ct <= allowedF);
                          ct = outstandingBorrowed.get();
                          assertTrue(ct + " <= " + borrowLimitF, ct <= borrowLimitF);
                          Thread.sleep(tRandom.nextInt(200));
                          int ct1 = outstandingBorrowed.get();
                          assertTrue(ct1 + " <= " + borrowLimitF, ct1 <= borrowLimitF);
                          int ct2 = outstanding.getAndDecrement();
                          assertTrue(ct2 + " <= " + allowedF, ct2 <= allowedF);
                        } else {
                          skipped.increment();
                          Thread.sleep(tRandom.nextInt(10));
                        }
                      }
                    }
                    return null;
                  }));
        }
        int borrowClients = r.nextInt(allowed << 1);
        for (int i = borrowClients; i > 0; i--) {
          Random tRandom = new Random(r.nextLong());
          futures.add(
              exec.submit(
                  () -> {
                    while (!finish.get()) {
                      try (RequestRateLimiter.SlotReservation slotReservation =
                          limiterF.allowSlotBorrowing()) {
                        if (slotReservation != null) {
                          borrowedExecuted.increment();
                          int ct = outstanding.incrementAndGet();
                          assertTrue(ct + " <= " + allowedF, ct <= allowedF);
                          ct = outstandingBorrowed.incrementAndGet();
                          assertTrue(ct + " <= " + borrowLimitF, ct <= borrowLimitF);
                          Thread.sleep(tRandom.nextInt(200));
                          int ct1 = outstandingBorrowed.getAndDecrement();
                          assertTrue(ct1 + " <= " + borrowLimitF, ct1 <= borrowLimitF);
                          int ct2 = outstanding.getAndDecrement();
                          assertTrue(ct2 + " <= " + allowedF, ct2 <= allowedF);
                        } else {
                          borrowedSkipped.increment();
                          Thread.sleep(tRandom.nextInt(10));
                        }
                      }
                    }
                    return null;
                  }));
        }
        Thread.sleep(5000); // let it run for a while
        finish.set(true);
        List<Exception> exceptions = new ArrayList<>();
        for (Future<Void> f : futures) {
          try {
            f.get(1, TimeUnit.SECONDS);
          } catch (Exception e) {
            exceptions.add(e);
          }
        }
        if (!exceptions.isEmpty()) {
          for (Exception e : exceptions) {
            e.printStackTrace(System.err);
          }
          fail("found " + exceptions.size() + " exceptions");
        }
        assertEquals(0, outstanding.get());
        assertEquals(0, outstandingBorrowed.get());
        assertTrue(limiter.isEmpty());
        allowed = r.nextInt(maxAllowed) + 1;
        guaranteed = r.nextInt(allowed + 1);
        borrowLimit = allowed - guaranteed;
        config =
            new RateLimiterConfig(
                SolrRequest.SolrRequestType.QUERY,
                true,
                guaranteed,
                20,
                allowed /* allowedRequests */,
                true /* isSlotBorrowing */);
        limiter = new RequestRateLimiter(config);
      }
    }
  }
}
