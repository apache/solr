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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RequestNotSentException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBSolrClient.AttemptLimitExceededException;
import org.apache.solr.client.solrj.impl.LBSolrClient.Endpoint;
import org.apache.solr.client.solrj.impl.LBSolrClient.Req;
import org.apache.solr.client.solrj.impl.LBSolrClient.Rsp;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/** Deterministic failures and deferred completions exercise the async retry boundary. */
public class LBAsyncSolrClientAttemptLimitTest extends SolrTestCase {
  private static final List<Endpoint> ENDPOINTS =
      List.of(
          new Endpoint("http://one.invalid/solr"),
          new Endpoint("http://two.invalid/solr"),
          new Endpoint("http://three.invalid/solr"));

  @Test
  public void testAsyncPreservesAttemptLimitFailure() throws Exception {
    for (int limit : new int[] {1, 2, 3}) {
      try (var transport = new HttpJdkSolrClient.Builder().build();
          var client = new RecordingClient(transport)) {
        for (Endpoint endpoint : ENDPOINTS) {
          client.fail(endpoint);
        }
        AttemptLimitExceededException failure =
            assertExhausted(
                client.requestAsync(new Req(new QueryRequest(), ENDPOINTS, limit)), limit);
        assertEquals(ENDPOINTS.subList(0, limit), client.attempted);
        assertSame(client.failures.get(ENDPOINTS.get(limit - 1)), failure.getCause());
      }
    }
  }

  @Test
  public void testAsyncLimitIncludesZombieFallback() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      client.markZombie(ENDPOINTS.get(0));
      client.markZombie(ENDPOINTS.get(2));
      client.fail(ENDPOINTS.get(1));
      client.fail(ENDPOINTS.get(0));
      AttemptLimitExceededException failure =
          assertExhausted(client.requestAsync(new Req(new QueryRequest(), ENDPOINTS, 2)), 2);
      assertEquals(List.of(ENDPOINTS.get(1), ENDPOINTS.get(0)), client.attempted);
      assertSame(client.failures.get(ENDPOINTS.get(0)), failure.getCause());
    }
  }

  @Test
  public void testZombieAllowanceCanStopBeforeTotalLimit() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.markZombie(endpoint);
        client.fail(endpoint);
      }
      Req req = new Req(new QueryRequest(), ENDPOINTS, 3);
      req.setNumDeadServersToTry(1);
      ExecutionException failure =
          expectThrows(
              ExecutionException.class,
              () -> client.requestAsync(req).get(1, TimeUnit.MINUTES));
      assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
      assertSame(client.failures.get(ENDPOINTS.get(0)), failure.getCause());
    }
  }

  @Test
  public void testAsyncSkippingZombieDoesNotSpendAttempt() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      client.markZombie(ENDPOINTS.get(0));
      client.requestAsync(new Req(new QueryRequest(), ENDPOINTS, 1)).get(1, TimeUnit.MINUTES);
      assertEquals(List.of(ENDPOINTS.get(1)), client.attempted);
    }
  }

  @Test
  public void testAsyncSucceedsOnLastPermittedAttempt() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      client.fail(ENDPOINTS.get(0));
      Rsp response =
          client.requestAsync(new Req(new QueryRequest(), ENDPOINTS, 2)).get(1, TimeUnit.MINUTES);
      assertEquals(ENDPOINTS.get(1).toString(), response.getServer());
      assertEquals(ENDPOINTS.subList(0, 2), client.attempted);
    }
  }

  @Test
  public void testAsyncNonPositiveLimitsDispatchNothing() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      for (int limit : new int[] {0, -1}) {
        AttemptLimitExceededException failure =
            assertExhausted(client.requestAsync(new Req(new QueryRequest(), ENDPOINTS, limit)), 0);
        assertEquals(limit, failure.getMaxAttempts());
        assertNull(failure.getCause());
      }
      assertTrue(client.attempted.isEmpty());
    }
  }

  @Test
  public void testConcurrentExecutionsHaveIndependentAllowances() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      client.deferResponses = true;
      Req req = new Req(new QueryRequest(), ENDPOINTS, 1);
      CompletableFuture<Rsp> first = client.requestAsync(req);
      CompletableFuture<Rsp> second = client.requestAsync(req);
      assertEquals(List.of(ENDPOINTS.get(0), ENDPOINTS.get(0)), client.attempted);
      for (CompletableFuture<NamedList<Object>> pending : client.pending) {
        pending.completeExceptionally(new IOException("Disconnected"));
      }
      assertExhausted(first, 1);
      assertExhausted(second, 1);
      assertEquals(2, client.attempted.size());
    }
  }

  @Test
  public void testCancellationDoesNotDispatchAnotherAttempt() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      client.deferResponses = true;
      CompletableFuture<Rsp> response =
          client.requestAsync(new Req(new QueryRequest(), ENDPOINTS, 3));
      assertTrue(response.cancel(true));
      assertTrue(client.pending.get(0).isCancelled());
      client.pending.get(0).completeExceptionally(new IOException("Disconnected"));
      assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
    }
  }

  @Test
  public void testProcessingLimitFailureStopsFailover() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      SolrServerException processingLimit =
          new SolrServerException("Limits exceeded!", new IOException("Disconnected"));
      client.failures.put(ENDPOINTS.get(0), processingLimit);
      ExecutionException failure =
          expectThrows(
              ExecutionException.class,
              () ->
                  client
                      .requestAsync(new Req(new QueryRequest(), ENDPOINTS, 3))
                      .get(1, TimeUnit.MINUTES));
      assertTrue(failure.getCause() instanceof SolrServerException);
      assertSame(processingLimit, failure.getCause());
      assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
    }
  }

  @Test
  public void testAttemptLimitDoesNotMakeUnsafeRequestsRetryable() throws Exception {
    for (SolrRequest<?> request :
        List.of(new UpdateRequest(), new CoreAdminRequest.RequestRecovery())) {
      try (var transport = new HttpJdkSolrClient.Builder().build();
          var client = new RecordingClient(transport)) {
        client.fail(ENDPOINTS.get(0));
        ExecutionException failure =
            expectThrows(
                ExecutionException.class,
                () ->
                    client
                        .requestAsync(new Req(request, ENDPOINTS, 3))
                        .get(1, TimeUnit.MINUTES));
        assertSame(client.failures.get(ENDPOINTS.get(0)), failure.getCause());
        assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
      }
    }
  }

  @Test
  public void testUnboundedRequestRetainsLastFailure() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.fail(endpoint);
      }
      ExecutionException failure =
          expectThrows(
              ExecutionException.class,
              () ->
                  client
                      .requestAsync(new Req(new QueryRequest(), ENDPOINTS))
                      .get(1, TimeUnit.MINUTES));
      assertEquals(ENDPOINTS, client.attempted);
      assertSame(client.failures.get(ENDPOINTS.get(2)), failure.getCause());
    }
  }

  @Test
  public void testUnsentUpdateStillConsumesAttempt() throws Exception {
    try (var transport = new HttpJdkSolrClient.Builder().build();
        var client = new RecordingClient(transport)) {
      client.failures.put(
          ENDPOINTS.get(0), new RequestNotSentException("Not sent", new IOException()));
      AttemptLimitExceededException failure =
          assertExhausted(client.requestAsync(new Req(new UpdateRequest(), ENDPOINTS, 1)), 1);
      assertSame(client.failures.get(ENDPOINTS.get(0)), failure.getCause());
      assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
    }
  }

  private static AttemptLimitExceededException assertExhausted(
      CompletableFuture<Rsp> response, int attempts) {
    ExecutionException failure =
        expectThrows(ExecutionException.class, () -> response.get(1, TimeUnit.MINUTES));
    assertTrue(failure.getCause() instanceof AttemptLimitExceededException);
    AttemptLimitExceededException exhausted = (AttemptLimitExceededException) failure.getCause();
    assertEquals(attempts, exhausted.getAttempts());
    return exhausted;
  }

  private static class RecordingClient extends LBAsyncSolrClient {
    private final List<Endpoint> attempted = new ArrayList<>();
    private final Map<Endpoint, Exception> failures = new HashMap<>();
    private final List<CompletableFuture<NamedList<Object>>> pending = new ArrayList<>();
    private boolean deferResponses;

    private RecordingClient(HttpJdkSolrClient transport) {
      super(new LBSolrClient.Builder<>(transport));
    }

    private void fail(Endpoint endpoint) {
      failures.put(
          endpoint, new SolrServerException("Failed " + endpoint, new IOException("Disconnected")));
    }

    private void markZombie(Endpoint endpoint) {
      makeServerAZombie(new EndpointWrapper(endpoint));
    }

    @Override
    protected synchronized void makeServerAZombie(EndpointWrapper wrapper) {
      // These tests complete failures explicitly and do not run background health checks.
      zombieServers.put(wrapper.getEndpoint().toString(), wrapper);
    }

    @Override
    protected CompletableFuture<NamedList<Object>> requestAsyncWithUrl(
        SolrClient client, String baseUrl, SolrRequest<?> request) {
      Endpoint endpoint = Endpoint.from(baseUrl);
      attempted.add(endpoint);
      CompletableFuture<NamedList<Object>> response = new CompletableFuture<>();
      if (deferResponses) {
        pending.add(response);
      } else if (failures.containsKey(endpoint)) {
        response.completeExceptionally(failures.get(endpoint));
      } else {
        response.complete(new NamedList<>());
      }
      return response;
    }
  }
}
