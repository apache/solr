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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.util.AsyncListener;
import org.apache.solr.client.solrj.util.Cancellable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.MapSolrParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/** Test the LBHttp2SolrClient. */
public class LBHttp2SolrClientTest extends SolrTestCase {

  /**
   * Test method for {@link LBHttp2SolrClient.Builder} that validates that the query param keys
   * passed in by the base <code>Http2SolrClient
   * </code> instance are used by the LBHttp2SolrClient.
   */
  @Test
  public void testLBHttp2SolrClientWithTheseParamNamesInTheUrl() {
    String url = "http://127.0.0.1:8080";
    Set<String> urlParamNames = new HashSet<>(2);
    urlParamNames.add("param1");

    try (Http2SolrClient http2SolrClient =
            new Http2SolrClient.Builder(url).withTheseParamNamesInTheUrl(urlParamNames).build();
        LBHttp2SolrClient testClient =
            new LBHttp2SolrClient.Builder(http2SolrClient, new LBSolrClient.Endpoint(url))
                .build()) {

      assertArrayEquals(
          "Wrong urlParamNames found in lb client.",
          urlParamNames.toArray(),
          testClient.getUrlParamNames().toArray());
      assertArrayEquals(
          "Wrong urlParamNames found in base client.",
          urlParamNames.toArray(),
          http2SolrClient.getUrlParamNames().toArray());
    }
  }

  @Test
  public void testAsyncDeprecated() {
    testAsync(true);
  }

  @Test
  public void testAsync() {
    testAsync(false);
  }

  @Test
  public void testAsyncWithFailures() {

    // TODO: This demonstrates that the failing endpoint always gets retried, but
    // I would expect it to be labelled as a "zombie" and be skipped with additional iterations.

    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    Http2SolrClient.Builder b = new Http2SolrClient.Builder("http://base.url");
    try (MockHttp2SolrClient client = new MockHttp2SolrClient("http://base.url", b);
        LBHttp2SolrClient testClient = new LBHttp2SolrClient.Builder(client, ep1, ep2).build()) {

      for (int j = 0; j < 2; j++) {
        // j: first time Endpoint One will retrun error code 500.
        // second time Endpoint One will be healthy

        String basePathToSucceed;
        if (j == 0) {
          client.basePathToFail = ep1.getBaseUrl();
          basePathToSucceed = ep2.getBaseUrl();
        } else {
          client.basePathToFail = ep2.getBaseUrl();
          basePathToSucceed = ep1.getBaseUrl();
        }

        for (int i = 0; i < 10; i++) {
          // i: we'll try 10 times to see if it behaves the same every time.

          QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", "" + i)));
          LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
          String iterMessage = "iter j/i " + j + "/" + i;
          try {
            testClient.requestAsync(req).get(1, TimeUnit.MINUTES);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            fail("interrupted");
          } catch (TimeoutException | ExecutionException e) {
            fail(iterMessage + " Response ended in failure: " + e);
          }
          if (j == 0) {
            // The first endpoint gives an exception, so it retries.
            assertEquals(iterMessage, 2, client.lastBasePaths.size());

            String failedBasePath = client.lastBasePaths.remove(0);
            assertEquals(iterMessage, client.basePathToFail, failedBasePath);
          } else {
            // The first endpoint does not give the exception, it doesn't retry.
            assertEquals(iterMessage, 1, client.lastBasePaths.size());
          }
          String successBasePath = client.lastBasePaths.remove(0);
          assertEquals(iterMessage, basePathToSucceed, successBasePath);
        }
      }
    }
  }

  private void testAsync(boolean useDeprecatedApi) {
    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    Http2SolrClient.Builder b = new Http2SolrClient.Builder("http://base.url");
    try (MockHttp2SolrClient client = new MockHttp2SolrClient("http://base.url", b);
        LBHttp2SolrClient testClient = new LBHttp2SolrClient.Builder(client, ep1, ep2).build()) {

      int limit = 10; // For simplicity use an even limit
      int halfLimit = limit / 2; // see TODO below

      CountDownLatch latch = new CountDownLatch(limit); // deprecated API use
      List<LBTestAsyncListener> listeners = new ArrayList<>(); // deprecated API use
      List<CompletableFuture<LBSolrClient.Rsp>> responses = new ArrayList<>();

      for (int i = 0; i < limit; i++) {
        QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", "" + i)));
        LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
        if (useDeprecatedApi) {
          LBTestAsyncListener listener = new LBTestAsyncListener(latch);
          listeners.add(listener);
          testClient.asyncReq(req, listener);
        } else {
          responses.add(testClient.requestAsync(req));
        }
      }

      if (useDeprecatedApi) {
        try {
          // This is just a formality.  This is a single-threaded test.
          latch.await(1, TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          fail("interrupted");
        }
      }

      QueryRequest[] queryRequests = new QueryRequest[limit];
      int numEndpointOne = 0;
      int numEndpointTwo = 0; // see TODO below
      for (int i = 0; i < limit; i++) {
        SolrRequest<?> lastSolrReq = client.lastSolrRequests.get(i);
        assertTrue(lastSolrReq instanceof QueryRequest);
        QueryRequest lastQueryReq = (QueryRequest) lastSolrReq;
        int index = Integer.parseInt(lastQueryReq.getParams().get("q"));
        assertNull("Found same request twice: " + index, queryRequests[index]);
        queryRequests[index] = lastQueryReq;
        if (lastQueryReq.getBasePath().equals(ep1.toString())) {
          numEndpointOne++;
        } else if (lastQueryReq.getBasePath().equals(ep2.toString())) {
          numEndpointTwo++;
        }
        NamedList<Object> lastResponse;
        if (useDeprecatedApi) {
          LBTestAsyncListener lastAsyncListener = listeners.get(index);
          assertTrue(lastAsyncListener.onStartCalled);
          assertNull(lastAsyncListener.failure);
          assertNotNull(lastAsyncListener.success);
          lastResponse = lastAsyncListener.success.getResponse();
        } else {
          LBSolrClient.Rsp lastRsp = null;
          try {
            lastRsp = responses.get(index).get();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            fail("interrupted");
          } catch (ExecutionException ee) {
            fail("Response " + index + " ended in failure: " + ee);
          }
          lastResponse = lastRsp.getResponse();
        }

        // The Mock will return {"response": index}.
        assertEquals("" + index, lastResponse.get("response"));
      }

      // TODO: LBHttp2SolrClient creates a new "endpoint iterator" per request, thus
      //       all requests go to the first endpoint.  I am not sure whether this was
      //       intended.  Maybe, we should be re-using the logic from
      //       LBSolrClient#request to pick use (default) round-robin
      //       and retain the ability for users to override "pickServer".
      //
      // assertEquals("expected 1/2 requests to go to endpoint one.", halfLimit, numEndpointOne);
      // assertEquals("expected 1/2 requests to go to endpoint two.", halfLimit, numEndpointTwo);
      assertEquals(limit, numEndpointOne);

      assertEquals(limit, client.lastSolrRequests.size());
      assertEquals(limit, client.lastCollections.size());
    }
  }

  @Deprecated(forRemoval = true)
  public static class LBTestAsyncListener implements AsyncListener<LBSolrClient.Rsp> {
    private final CountDownLatch latch;
    private volatile boolean countDownCalled = false;
    public boolean onStartCalled = false;
    public LBSolrClient.Rsp success = null;
    public Throwable failure = null;

    public LBTestAsyncListener(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void onStart() {
      onStartCalled = true;
    }

    @Override
    public void onSuccess(LBSolrClient.Rsp entries) {
      success = entries;
      countdown();
    }

    @Override
    public void onFailure(Throwable throwable) {
      failure = throwable;
      countdown();
    }

    private void countdown() {
      if (countDownCalled) {
        throw new IllegalStateException("Already counted down.");
      }
      latch.countDown();
      countDownCalled = true;
    }
  }

  public static class MockHttp2SolrClient extends Http2SolrClient {

    public List<SolrRequest<?>> lastSolrRequests = new ArrayList<>();

    public List<String> lastBasePaths = new ArrayList<>();

    public List<String> lastCollections = new ArrayList<>();

    public String basePathToFail = null;

    protected MockHttp2SolrClient(String serverBaseUrl, Builder builder) {
      // TODO: Consider creating an interface for Http*SolrClient
      // so mocks can Implement, not Extend, and not actually need to
      // build an (unused) client
      super(serverBaseUrl, builder);
    }

    @Override
    public Cancellable asyncRequest(
        SolrRequest<?> solrRequest,
        String collection,
        AsyncListener<NamedList<Object>> asyncListener) {
      throw new UnsupportedOperationException("do not use deprecated method.");
    }

    @Override
    public CompletableFuture<NamedList<Object>> requestAsync(
        final SolrRequest<?> solrRequest, String collection) {
      CompletableFuture<NamedList<Object>> cf = new CompletableFuture<>();
      lastSolrRequests.add(solrRequest);
      lastBasePaths.add(solrRequest.getBasePath());
      lastCollections.add(collection);
      if (solrRequest.getBasePath().equals(basePathToFail)) {
        cf.completeExceptionally(
            new SolrException(SolrException.ErrorCode.SERVER_ERROR, "We should retry this."));
      } else {
        cf.complete(generateResponse(solrRequest));
      }
      return cf;
    }

    private NamedList<Object> generateResponse(SolrRequest<?> solrRequest) {
      String id = solrRequest.getParams().get("q");
      return new NamedList<>(Collections.singletonMap("response", id));
    }
  }
}
