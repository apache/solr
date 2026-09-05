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
import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.nio.channels.ClosedChannelException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RequestNotSentException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.HttpJettySolrClient;
import org.apache.solr.common.util.NamedList;
import org.eclipse.jetty.io.EofException;
import org.junit.Test;

/**
 * {@link SolrClient#wasRequestUnsent} and {@link SolrClient#wasCommError} are pure functions of the
 * failure, so each transport's answers can be asserted directly rather than raced for through an
 * integration test. No server is needed; the clients are never asked to send anything.
 *
 * <p>The negative cases matter most: {@code wasRequestUnsent} returning false means "cannot tell",
 * and treating a failure as unsent when it isn't would replay a non-idempotent update.
 */
public class SolrClientErrorClassificationTest extends SolrTestCase {

  private static final String DEAD_URL = "http://127.0.0.1:1/solr";

  private static SolrServerException wrapped(Throwable cause) {
    return new SolrServerException("wrapped", cause);
  }

  private static RequestNotSentException unsent() {
    return new RequestNotSentException("Broken pipe", new IOException("Broken pipe"));
  }

  /** Every HTTP transport inherits these from {@link HttpSolrClient}. */
  private static void assertSharedHttpClassification(HttpSolrClient client) {
    // The transport stated the answer; it holds whether it is the failure or nested inside one.
    assertTrue(client.wasRequestUnsent(unsent()));
    assertTrue(client.wasCommError(unsent()));
    assertTrue(client.wasRequestUnsent(wrapped(unsent())));
    assertTrue(client.wasCommError(wrapped(unsent())));

    // Nothing was written because nothing connected.
    assertTrue(client.wasRequestUnsent(new ConnectException("Connection refused")));
    assertTrue(client.wasCommError(new ConnectException("Connection refused")));

    // A comm error, but no proof either way about delivery.
    assertFalse(client.wasRequestUnsent(new SocketException("Connection reset")));
    assertTrue(client.wasCommError(new SocketException("Connection reset")));
    assertFalse(client.wasRequestUnsent(new UnknownHostException("nosuchhost")));
    assertTrue(client.wasCommError(new UnknownHostException("nosuchhost")));

    // A bare IOException may have been sent and applied, so it proves nothing.
    assertFalse(client.wasRequestUnsent(new IOException("Broken pipe")));
    assertFalse(client.wasCommError(new IOException("Broken pipe")));
  }

  @Test
  public void testHttpJdkSolrClientClassification() throws Exception {
    try (HttpJdkSolrClient client = new HttpJdkSolrClient.Builder(DEAD_URL).build()) {
      assertSharedHttpClassification(client);

      // The connection was never established, so the request cannot have been written.
      assertTrue(client.wasRequestUnsent(new HttpConnectTimeoutException("timed out")));
      assertTrue(client.wasCommError(new HttpConnectTimeoutException("timed out")));
    }
  }

  @Test
  public void testHttpJettySolrClientClassification() throws Exception {
    try (HttpJettySolrClient client = new HttpJettySolrClient.Builder(DEAD_URL).build()) {
      assertSharedHttpClassification(client);

      // Jetty's connection-lost types are communication errors, but a connection can end after the
      // request was fully written, so they must never claim it was unsent. Whether it was is
      // answered at the throw site by the request-commit listener instead.
      for (Throwable lost :
          new Throwable[] {
            new EofException("Connection reset by peer"),
            new ClosedChannelException(),
            // The shape HttpJettySolrClient raises once an HTTP/2 session is lost after commit.
            new EofException("HTTP/2 session closed", new IllegalStateException("session closed"))
          }) {
        assertTrue(lost.getClass().getName(), client.wasCommError(lost));
        assertTrue(lost.getClass().getName(), client.wasCommError(wrapped(lost)));
        assertFalse(lost.getClass().getName(), client.wasRequestUnsent(lost));
        assertFalse(lost.getClass().getName(), client.wasRequestUnsent(wrapped(lost)));
      }
    }
  }

  /** A plain {@link SolrClient} cannot tell, and must never claim otherwise. */
  @Test
  public void testDefaultIsAlwaysFalse() {
    SolrClient client =
        new SolrClient() {
          @Override
          public NamedList<Object> request(SolrRequest<?> request, String collection) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void close() {}
        };
    assertFalse(client.wasRequestUnsent(unsent()));
    assertFalse(client.wasCommError(new SocketException("Connection reset")));
  }
}
