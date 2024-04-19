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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
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
  public void testAsyncWithFailures() {

    // This demonstrates that the failing endpoint always gets retried, and it is up to the user
    // to remove any failing nodes if desired.

    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    Http2SolrClient.Builder b =
        new Http2SolrClient.Builder("http://base.url").withConnectionTimeout(10, TimeUnit.SECONDS);
    ;
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
          if (i == 0) {
            // When j=0, "endpoint one" fails.  The first time around (i) it tries the first, then
            // the second.
            // With j=0 and i>0, it only tries "endpoint two".
            // When j=1 and i=0, "endpoint two" starts failing. So it tried both it and "endpoint
            // one"
            // With j=1 and i>0, it only tries "endpoint one".
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

  @Test
  public void testAsync() {
    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    Http2SolrClient.Builder b =
        new Http2SolrClient.Builder("http://base.url").withConnectionTimeout(10, TimeUnit.SECONDS);
    try (MockHttp2SolrClient client = new MockHttp2SolrClient("http://base.url", b);
        LBHttp2SolrClient testClient = new LBHttp2SolrClient.Builder(client, ep1, ep2).build()) {

      int limit = 10; // For simplicity use an even limit
      List<CompletableFuture<LBSolrClient.Rsp>> responses = new ArrayList<>();

      for (int i = 0; i < limit; i++) {
        QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", "" + i)));
        LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
        responses.add(testClient.requestAsync(req));
      }

      QueryRequest[] queryRequests = new QueryRequest[limit];
      int numEndpointOne = 0;
      int numEndpointTwo = 0;
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

        LBSolrClient.Rsp lastRsp = null;
        try {
          lastRsp = responses.get(index).get();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          fail("interrupted");
        } catch (ExecutionException ee) {
          fail("Response " + index + " ended in failure: " + ee);
        }
        NamedList<Object> lastResponse = lastRsp.getResponse();

        // The Mock will return {"response": index}.
        assertEquals("" + index, lastResponse.get("response"));
      }

      // It is the user's responsibility to shuffle the endpoints when using
      // async.  LB Http Solr Client always will try the passed-in endpoints
      // in order.  In this case, endpoint 1 gets all the requests!
      assertEquals(limit, numEndpointOne);
      assertEquals(0, numEndpointTwo);

      assertEquals(limit, client.lastSolrRequests.size());
      assertEquals(limit, client.lastCollections.size());
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
