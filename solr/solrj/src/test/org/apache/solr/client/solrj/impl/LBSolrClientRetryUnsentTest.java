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
import java.util.List;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RequestNotSentException;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrRequest.SolrRequestType;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

/**
 * A failure that proves the request never reached the server is safe to replay even when
 * LBSolrClient would otherwise refuse to retry the request. {@link RequestNotSentException} is that
 * proof.
 */
public class LBSolrClientRetryUnsentTest extends SolrTestCase {

  private static final LBSolrClient.Endpoint DEAD_HOST_1 =
      new LBSolrClient.Endpoint("http://127.0.0.1:1/solr");
  private static final LBSolrClient.Endpoint DEAD_HOST_2 =
      new LBSolrClient.Endpoint("http://127.0.0.1:2/solr");

  /** Fails whatever endpoint is tried first with {@code failure}; any later endpoint succeeds. */
  private static class FailFirstEndpoint extends LBSolrClient {
    final List<String> attempted = new ArrayList<>();
    private final HttpSolrClient transport;

    FailFirstEndpoint(Exception failure) {
      super(List.of(DEAD_HOST_1, DEAD_HOST_2));
      // A real transport, so the LB asks its real classification rather than a stand-in.
      this.transport =
          new HttpJdkSolrClient(DEAD_HOST_1.getBaseUrl(), new HttpJdkSolrClient.Builder()) {
            @Override
            public NamedList<Object> requestWithBaseUrl(
                String baseUrl, SolrRequest<?> solrRequest, String collection)
                throws SolrServerException, IOException {
              attempted.add(baseUrl);
              if (attempted.size() > 1) {
                return new NamedList<>();
              }
              if (failure instanceof SolrServerException sse) {
                throw sse;
              }
              throw (IOException) failure;
            }
          };
    }

    @Override
    protected HttpSolrClient getClient(Endpoint endpoint) {
      return transport;
    }

    @Override
    public void close() {
      super.close();
      IOUtils.closeQuietly(transport);
    }
  }

  private static SolrServerException unsentException() {
    IOException onTheWire = new IOException("Broken pipe");
    return new SolrServerException(
        "Connection failed before the request was sent to: " + DEAD_HOST_1.getUrl(),
        new RequestNotSentException(onTheWire.getMessage(), onTheWire));
  }

  private static SolrServerException maybeSentException() {
    return new SolrServerException(
        "IOException occurred when talking to server at: " + DEAD_HOST_1.getUrl(),
        new IOException("Broken pipe"));
  }

  private static List<String> requestReturningAttemptedUrls(
      Exception failure, SolrRequest<?> request) throws Exception {
    try (FailFirstEndpoint client = new FailFirstEndpoint(failure)) {
      client.request(new LBSolrClient.Req(request, List.of(DEAD_HOST_1, DEAD_HOST_2)));
      return client.attempted;
    }
  }

  @Test
  public void testUpdateIsRetriedWhenRequestWasNeverSent() throws Exception {
    assertEquals(
        List.of(DEAD_HOST_1.getBaseUrl(), DEAD_HOST_2.getBaseUrl()),
        requestReturningAttemptedUrls(unsentException(), new UpdateRequest().add("id", "1")));
  }

  /** LBSolrClient classifies {@link SolrRequestType#UPDATE} as non-retryable. */
  @Test
  public void testRequestThatMayHaveBeenReceivedIsNotRetried() {
    LBSolrClient.Req req =
        new LBSolrClient.Req(new UpdateRequest().add("id", "1"), List.of(DEAD_HOST_1, DEAD_HOST_2));
    try (FailFirstEndpoint client = new FailFirstEndpoint(maybeSentException())) {
      expectThrows(SolrServerException.class, () -> client.request(req));
      assertEquals(List.of(DEAD_HOST_1.getBaseUrl()), client.attempted);
    }
  }

  @Test
  public void testQueryIsStillRetriedOnAnyIOException() throws Exception {
    assertEquals(
        List.of(DEAD_HOST_1.getBaseUrl(), DEAD_HOST_2.getBaseUrl()),
        requestReturningAttemptedUrls(maybeSentException(), new QueryRequest()));
  }

  /**
   * A transport may throw an {@link IOException} directly rather than wrapping it in a {@link
   * SolrServerException}, as HttpJdkSolrClient does. LBAsyncSolrClient has always handled that; the
   * synchronous path used to let it reach the catch-all and abort with no failover.
   */
  @Test
  public void testQueryIsRetriedOnBareIOException() throws Exception {
    assertEquals(
        List.of(DEAD_HOST_1.getBaseUrl(), DEAD_HOST_2.getBaseUrl()),
        requestReturningAttemptedUrls(new IOException("Broken pipe"), new QueryRequest()));
  }

  @Test
  public void testUpdateIsNotRetriedOnBareIOException() {
    LBSolrClient.Req req =
        new LBSolrClient.Req(new UpdateRequest().add("id", "1"), List.of(DEAD_HOST_1, DEAD_HOST_2));
    try (FailFirstEndpoint client = new FailFirstEndpoint(new IOException("Broken pipe"))) {
      expectThrows(IOException.class, () -> client.request(req));
      assertEquals(List.of(DEAD_HOST_1.getBaseUrl()), client.attempted);
    }
  }

  /**
   * A query must fail over whenever the transport proves the request unsent, even if the deepest
   * cause isn't an {@link IOException}.
   */
  @Test
  public void testQueryIsRetriedWhenUnsentButRootCauseIsNotIO() throws Exception {
    IllegalStateException sessionClosed = new IllegalStateException("session closed");
    SolrServerException failure =
        new SolrServerException(
            "Connection failed before the request was sent to: " + DEAD_HOST_1.getUrl(),
            new RequestNotSentException(sessionClosed.getMessage(), sessionClosed));
    assertEquals(
        List.of(DEAD_HOST_1.getBaseUrl(), DEAD_HOST_2.getBaseUrl()),
        requestReturningAttemptedUrls(failure, new QueryRequest()));
  }

  /**
   * Parity with LBAsyncSolrClient, which already retried a bare {@link RequestNotSentException}.
   */
  @Test
  public void testUpdateIsRetriedOnBareRequestNotSentException() throws Exception {
    IOException onTheWire = new IOException("Broken pipe");
    assertEquals(
        List.of(DEAD_HOST_1.getBaseUrl(), DEAD_HOST_2.getBaseUrl()),
        requestReturningAttemptedUrls(
            new RequestNotSentException(onTheWire.getMessage(), onTheWire),
            new UpdateRequest().add("id", "1")));
  }
}
