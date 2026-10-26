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
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBSolrClient.AttemptLimitExceededException;
import org.apache.solr.client.solrj.impl.LBSolrClient.Endpoint;
import org.apache.solr.client.solrj.impl.LBSolrClient.Req;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/** Dispatch-count coverage for explicit-endpoint and standalone LB requests. */
public class LBSolrClientAttemptLimitTest extends SolrTestCase {
  private static final List<Endpoint> ENDPOINTS =
      List.of(
          new Endpoint("http://one.invalid/solr"),
          new Endpoint("http://two.invalid/solr"),
          new Endpoint("http://three.invalid/solr"));

  @Test
  public void testStandaloneLimitStopsBeforeZombieFallback() {
    try (RecordingClient client = new RecordingClient()) {
      client.markZombie(ENDPOINTS.get(2));
      client.fail(ENDPOINTS.get(0));
      client.fail(ENDPOINTS.get(1));
      AttemptLimitExceededException failure =
          expectThrows(
              AttemptLimitExceededException.class,
              () -> client.request(new QueryRequest(), null, 2));
      assertEquals(ENDPOINTS.subList(0, 2), client.attempted);
      assertExhausted(failure, client, 2);
    }
  }

  @Test
  public void testStandaloneLimitWithAllZombies() {
    try (RecordingClient client = new RecordingClient()) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.markZombie(endpoint);
        client.fail(endpoint);
      }
      AttemptLimitExceededException failure =
          expectThrows(
              AttemptLimitExceededException.class,
              () -> client.request(new QueryRequest(), null, 1));
      assertExhausted(failure, client, 1);
    }
  }

  @Test
  public void testStandaloneSucceedsOnLastPermittedAttempt() throws Exception {
    try (RecordingClient client = new RecordingClient()) {
      client.fail(ENDPOINTS.get(0));
      assertNotNull(client.request(new QueryRequest(), null, 2));
      assertEquals(ENDPOINTS.subList(0, 2), client.attempted);
    }
  }

  @Test
  public void testStandaloneDoesNotRetryExhaustedLiveSnapshot() {
    try (RecordingClient client = new RecordingClient()) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.fail(endpoint);
      }
      SolrServerException failure =
          expectThrows(
              SolrServerException.class, () -> client.request(new QueryRequest(), null, 5));
      assertFalse(failure instanceof AttemptLimitExceededException);
      assertEquals(ENDPOINTS, client.attempted);
      assertSame(client.failures.get(ENDPOINTS.get(2)), failure.getCause());
    }
  }

  @Test
  public void testStandaloneUnboundedRequestCanUseZombieFallback() throws Exception {
    try (RecordingClient client = new RecordingClient()) {
      client.markZombie(ENDPOINTS.get(2));
      client.fail(ENDPOINTS.get(0));
      client.fail(ENDPOINTS.get(1));
      assertNotNull(client.request(new QueryRequest(), null));
      assertEquals(ENDPOINTS, client.attempted);
    }
  }

  @Test
  public void testEndpointLimitIncludesZombieFallback() {
    try (RecordingClient client = new RecordingClient()) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.markZombie(endpoint);
        client.fail(endpoint);
      }
      AttemptLimitExceededException failure =
          expectThrows(
              AttemptLimitExceededException.class,
              () -> client.request(new Req(new QueryRequest(), ENDPOINTS, 2)));
      assertEquals(ENDPOINTS.subList(0, 2), client.attempted);
      assertExhausted(failure, client, 2);
    }
  }

  @Test
  public void testZombieAllowanceCanStopBeforeTotalLimit() {
    try (RecordingClient client = new RecordingClient()) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.markZombie(endpoint);
        client.fail(endpoint);
      }
      Req req = new Req(new QueryRequest(), ENDPOINTS, 3);
      req.setNumDeadServersToTry(1);
      SolrServerException failure =
          expectThrows(SolrServerException.class, () -> client.request(req));
      assertFalse(failure instanceof AttemptLimitExceededException);
      assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
      assertSame(client.failures.get(ENDPOINTS.get(0)), failure.getCause());
    }
  }

  @Test
  public void testSkippingZombieDoesNotSpendAttempt() throws Exception {
    try (RecordingClient client = new RecordingClient()) {
      client.markZombie(ENDPOINTS.get(0));
      client.request(new Req(new QueryRequest(), ENDPOINTS, 1));
      assertEquals(List.of(ENDPOINTS.get(1)), client.attempted);
    }
  }

  @Test
  public void testEndpointRequestSucceedsOnLastPermittedAttempt() throws Exception {
    try (RecordingClient client = new RecordingClient()) {
      client.fail(ENDPOINTS.get(0));
      client.request(new Req(new QueryRequest(), ENDPOINTS, 2));
      assertEquals(ENDPOINTS.subList(0, 2), client.attempted);
    }
  }

  @Test
  public void testReusedEndpointRequestGetsFreshAllowance() {
    try (RecordingClient client = new RecordingClient()) {
      for (Endpoint endpoint : ENDPOINTS) {
        client.fail(endpoint);
      }
      Req req = new Req(new QueryRequest(), ENDPOINTS, 2);
      for (int i = 0; i < 2; i++) {
        client.attempted.clear();
        AttemptLimitExceededException failure =
            expectThrows(AttemptLimitExceededException.class, () -> client.request(req));
        assertExhausted(failure, client, 2);
      }
    }
  }

  @Test
  public void testNonPositiveLimitsDispatchNothing() {
    for (int limit : new int[] {0, -1}) {
      try (RecordingClient client = new RecordingClient()) {
        client.markZombie(ENDPOINTS.get(2));
        AttemptLimitExceededException standaloneFailure =
            expectThrows(
                AttemptLimitExceededException.class,
                () -> client.request(new QueryRequest(), null, limit));
        assertEquals(0, standaloneFailure.getAttempts());
        assertNull(standaloneFailure.getCause());
        AttemptLimitExceededException endpointFailure =
            expectThrows(
                AttemptLimitExceededException.class,
                () -> client.request(new Req(new QueryRequest(), ENDPOINTS, limit)));
        assertEquals(0, endpointFailure.getAttempts());
        assertNull(endpointFailure.getCause());
        assertTrue(client.attempted.isEmpty());
      }
    }
  }

  @Test
  public void testAttemptLimitDoesNotMakeUnsafeRequestsRetryable() {
    for (SolrRequest<?> request :
        List.of(new UpdateRequest(), new CoreAdminRequest.RequestRecovery())) {
      try (RecordingClient client = new RecordingClient()) {
        client.fail(ENDPOINTS.get(0));
        SolrServerException failure =
            expectThrows(
                SolrServerException.class, () -> client.request(new Req(request, ENDPOINTS, 3)));
        assertEquals(List.of(ENDPOINTS.get(0)), client.attempted);
        assertSame(client.failures.get(ENDPOINTS.get(0)), failure);
      }
    }
  }

  private static void assertExhausted(
      AttemptLimitExceededException failure, RecordingClient client, int limit) {
    assertEquals(limit, client.attempted.size());
    assertEquals(limit, failure.getAttempts());
    assertEquals(limit, failure.getMaxAttempts());
    assertSame(
        client.failures.get(client.attempted.get(client.attempted.size() - 1)), failure.getCause());
  }

  private static class RecordingClient extends LBSolrClient {
    private final List<Endpoint> attempted = new ArrayList<>();
    private final Map<Endpoint, SolrServerException> failures = new HashMap<>();

    private RecordingClient() {
      super(ENDPOINTS);
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
      // Keep health checks out of these deterministic dispatch counts.
      zombieServers.put(wrapper.getEndpoint().toString(), wrapper);
      updateAliveList();
    }

    @Override
    protected SolrClient getClient(Endpoint endpoint) {
      return new SolrClient() {
        @Override
        public NamedList<Object> request(SolrRequest<?> request, String collection)
            throws SolrServerException {
          attempted.add(endpoint);
          SolrServerException failure = failures.get(endpoint);
          if (failure != null) {
            throw failure;
          }
          return new NamedList<>();
        }

        @Override
        public void close() {}
      };
    }
  }
}
