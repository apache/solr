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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import org.apache.solr.client.solrj.SolrServerException;
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

    var httpSolrClientBuilder =
        new Http2SolrClient.Builder(url).withTheseParamNamesInTheUrl(urlParamNames);
    var endpoint = new LBSolrClient.Endpoint(url);
    try (var testClient =
        new LBHttp2SolrClient.Builder<Http2SolrClient.Builder>(httpSolrClientBuilder, endpoint)
            .build()) {

      assertArrayEquals(
          "Wrong urlParamNames found in lb client.",
          urlParamNames.toArray(),
          testClient.getUrlParamNames().toArray());
      assertArrayEquals(
          "Wrong urlParamNames found in base client.",
          urlParamNames.toArray(),
          testClient.getClient(endpoint).getUrlParamNames().toArray());
    }
  }

  @Test
  public void testSynchronous() throws Exception {
    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    var httpSolrClientBuilder =
        new MockHttpSolrClientBuilder().withConnectionTimeout(10, TimeUnit.SECONDS);

    try (LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient =
        new LBHttp2SolrClient.Builder<>(httpSolrClientBuilder, ep1, ep2).build()) {

      String lastEndpoint = null;
      for (int i = 0; i < 10; i++) {
        String qValue = "Query Number: " + i;
        QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", qValue)));
        LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
        LBSolrClient.Rsp response = testClient.request(req);

        String expectedEndpoint =
            ep1.toString().equals(lastEndpoint) ? ep2.toString() : ep1.toString();
        assertEquals(
            "There should be round-robin load balancing.", expectedEndpoint, response.server);
        checkSynchonousResponseContent(response, qValue);
      }
    }
  }

  @Test
  public void testSynchronousWithFalures() throws Exception {
    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    var httpSolrClientBuilder =
        new MockHttpSolrClientBuilder().withConnectionTimeout(10, TimeUnit.SECONDS);

    try (LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient =
        new LBHttp2SolrClient.Builder<>(httpSolrClientBuilder, ep1, ep2).build()) {

      setEndpointToFail(testClient, ep1);
      setEndpointToSucceed(testClient, ep2);
      String qValue = "First time";

      for (int i = 0; i < 5; i++) {
        LBSolrClient.Req req =
            new LBSolrClient.Req(
                new QueryRequest(new MapSolrParams(Map.of("q", qValue))), endpointList);
        LBSolrClient.Rsp response = testClient.request(req);
        assertEquals(
            "The healthy node 'endpoint two' should have served the request: " + i,
            ep2.getBaseUrl(),
            response.server);
        checkSynchonousResponseContent(response, qValue);
      }

      setEndpointToFail(testClient, ep2);
      setEndpointToSucceed(testClient, ep1);
      qValue = "Second time";

      for (int i = 0; i < 5; i++) {
        LBSolrClient.Req req =
            new LBSolrClient.Req(
                new QueryRequest(new MapSolrParams(Map.of("q", qValue))), endpointList);
        LBSolrClient.Rsp response = testClient.request(req);
        assertEquals(
            "The healthy node 'endpoint one' should have served the request: " + i,
            ep1.getBaseUrl(),
            response.server);
        checkSynchonousResponseContent(response, qValue);
      }
    }
  }

  @Test
  public void testAsyncWithFailures() {

    // This demonstrates that the failing endpoint always gets retried, and it is up to the user
    // to remove any failing nodes if desired.

    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    var httpSolrClientBuilder =
        new MockHttpSolrClientBuilder().withConnectionTimeout(10, TimeUnit.SECONDS);

    try (LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient =
        new LBHttp2SolrClient.Builder<>(httpSolrClientBuilder, ep1, ep2).build()) {

      for (int j = 0; j < 2; j++) {
        // first time Endpoint One will return error code 500.
        // second time Endpoint One will be healthy

        LBSolrClient.Endpoint endpointToSucceed;
        LBSolrClient.Endpoint endpointToFail;
        if (j == 0) {
          setEndpointToFail(testClient, ep1);
          setEndpointToSucceed(testClient, ep2);
          endpointToSucceed = ep2;
          endpointToFail = ep1;
        } else {
          setEndpointToFail(testClient, ep2);
          setEndpointToSucceed(testClient, ep1);
          endpointToSucceed = ep1;
          endpointToFail = ep2;
        }
        List<String> successEndpointLastBasePaths =
            basePathsForEndpoint(testClient, endpointToSucceed);
        List<String> failEndpointLastBasePaths = basePathsForEndpoint(testClient, endpointToFail);

        for (int i = 0; i < 10; i++) {
          // i: we'll try 10 times.  It should behave the same with iter 2-10. .

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
            // When i=0, it must try both endpoints to find success:
            //
            //     with j=0, endpoint one is tried first because it
            //               is first one the list, but it fails.
            //     with j=1, endpoint two is tried first because
            //               it is the only known healthy node, but
            //               now it is failing.
            assertEquals(iterMessage, 1, successEndpointLastBasePaths.size());
            assertEquals(iterMessage, 1, failEndpointLastBasePaths.size());
          } else {
            // With i>0,
            //     With j=0 and i>0, it only tries "endpoint two".
            //     With j=1 and i>0, it only tries "endpoint one".
            assertEquals(iterMessage, 1, successEndpointLastBasePaths.size());
            assertTrue(iterMessage, failEndpointLastBasePaths.isEmpty());
          }
          successEndpointLastBasePaths.clear();
          failEndpointLastBasePaths.clear();
        }
      }
    }
  }

  @Test
  public void testAsync() {
    LBSolrClient.Endpoint ep1 = new LBSolrClient.Endpoint("http://endpoint.one");
    LBSolrClient.Endpoint ep2 = new LBSolrClient.Endpoint("http://endpoint.two");
    List<LBSolrClient.Endpoint> endpointList = List.of(ep1, ep2);

    var httpSolrClientBuilder =
        new MockHttpSolrClientBuilder().withConnectionTimeout(10, TimeUnit.SECONDS);

    try (LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient =
        new LBHttp2SolrClient.Builder<>(httpSolrClientBuilder, ep1, ep2).build()) {

      int limit = 10; // For simplicity use an even limit
      List<CompletableFuture<LBSolrClient.Rsp>> responses = new ArrayList<>();

      for (int i = 0; i < limit; i++) {
        QueryRequest queryRequest = new QueryRequest(new MapSolrParams(Map.of("q", "" + i)));
        LBSolrClient.Req req = new LBSolrClient.Req(queryRequest, endpointList);
        responses.add(testClient.requestAsync(req));
      }

      QueryRequest[] queryRequests = new QueryRequest[limit];
      List<SolrRequest<?>> lastSolrRequests = lastSolrRequests(testClient, ep1, ep2);
      assertEquals(limit, lastSolrRequests.size());

      for (int i = 0; i < limit; i++) {
        SolrRequest<?> lastSolrReq = lastSolrRequests.get(i);
        assertTrue(lastSolrReq instanceof QueryRequest);
        QueryRequest lastQueryReq = (QueryRequest) lastSolrReq;
        int index = Integer.parseInt(lastQueryReq.getParams().get("q"));
        assertNull("Found same request twice: " + index, queryRequests[index]);
        queryRequests[index] = lastQueryReq;

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
      // async.  LB Http Solr Client will always try the passed-in endpoints
      // in order.  In this case, endpoint 1 gets all the requests!
      List<String> ep1BasePaths = basePathsForEndpoint(testClient, ep1);
      List<String> ep2BasePaths = basePathsForEndpoint(testClient, ep2);
      assertEquals(limit, basePathsForEndpoint(testClient, ep1).size());
      assertEquals(0, basePathsForEndpoint(testClient, ep2).size());
    }
  }

  private void checkSynchonousResponseContent(LBSolrClient.Rsp response, String qValue) {
    assertEquals("There should be one element in the response.", 1, response.getResponse().size());
    assertEquals(
        "The response key 'response' should echo the query.",
        qValue,
        response.getResponse().get("response"));
  }

  private void setEndpointToFail(
      LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient, LBSolrClient.Endpoint ep) {
    ((MockHttpSolrClient) testClient.getClient(ep)).allRequestsShallFail = true;
  }

  private void setEndpointToSucceed(
      LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient, LBSolrClient.Endpoint ep) {
    ((MockHttpSolrClient) testClient.getClient(ep)).allRequestsShallFail = false;
  }

  private List<String> basePathsForEndpoint(
      LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient, LBSolrClient.Endpoint ep) {
    return ((MockHttpSolrClient) testClient.getClient(ep)).lastBasePaths;
  }

  private List<SolrRequest<?>> lastSolrRequests(
      LBHttp2SolrClient<MockHttpSolrClientBuilder> testClient, LBSolrClient.Endpoint... endpoints) {
    return Arrays.stream(endpoints)
        .map(testClient::getClient)
        .map(MockHttpSolrClient.class::cast)
        .flatMap(c -> c.lastSolrRequests.stream())
        .toList();
  }

  public static class MockHttpSolrClientBuilder
      extends HttpSolrClientBuilderBase<MockHttpSolrClientBuilder, MockHttpSolrClient> {

    @Override
    public MockHttpSolrClient build() {
      return new MockHttpSolrClient(baseSolrUrl, this);
    }
  }

  public static class MockHttpSolrClient extends HttpSolrClientBase {

    public List<SolrRequest<?>> lastSolrRequests = new ArrayList<>();

    public List<String> lastBasePaths = new ArrayList<>();

    public List<String> lastCollections = new ArrayList<>();

    public boolean allRequestsShallFail;

    public String tmpBaseUrl = null;

    public boolean closeCalled;

    protected MockHttpSolrClient(String serverBaseUrl, MockHttpSolrClientBuilder builder) {
      super(serverBaseUrl, builder);
    }

    @Override
    public NamedList<Object> request(final SolrRequest<?> request, String collection)
        throws SolrServerException, IOException {
      lastSolrRequests.add(request);
      lastBasePaths.add(tmpBaseUrl);
      lastCollections.add(collection);
      if (allRequestsShallFail) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "We should retry this.");
      }
      return generateResponse(request);
    }

    @Override
    public CompletableFuture<NamedList<Object>> requestAsync(
        final SolrRequest<?> solrRequest, String collection) {
      CompletableFuture<NamedList<Object>> cf = new CompletableFuture<>();
      lastSolrRequests.add(solrRequest);
      lastBasePaths.add(tmpBaseUrl);
      lastCollections.add(collection);
      if (allRequestsShallFail) {
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

    @Override
    public void close() throws IOException {
      closeCalled = true;
    }

    @Override
    protected boolean isFollowRedirects() {
      return false;
    }

    @Override
    protected boolean processorAcceptsMimeType(
        Collection<String> processorSupportedContentTypes, String mimeType) {
      return false;
    }

    @Override
    protected String allProcessorSupportedContentTypesCommaDelimited(
        Collection<String> processorSupportedContentTypes) {
      return null;
    }

    @Override
    protected void updateDefaultMimeTypeForParser() {
      // no-op
    }
  }
}
