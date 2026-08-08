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
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import org.apache.solr.SolrTestCaseJ4;

/** Unit tests for {@link CloudSolrClient#wasCommError(Throwable)}, which drives request retries. */
public class CloudSolrClientCommErrorTest extends SolrTestCaseJ4 {

  public void testCommErrorClassification() throws Exception {
    try (ProbeClient client = new ProbeClient()) {
      // connection-level failures: the target node is gone, so retrying on a fresh node makes sense
      assertTrue(client.wasCommError(new SocketException("connection reset")));
      assertTrue(client.wasCommError(new UnknownHostException("no such host")));
      // a connection dropped mid-request (e.g. an HTTP/2 GOAWAY on server shutdown) surfaces as a
      // ClosedChannelException; it is the same "node went away" case and should be retried too
      assertTrue(client.wasCommError(new ClosedChannelException()));

      // not comm errors: these should not trigger a comm-error retry
      assertFalse(client.wasCommError(new IOException("some other io problem")));
      assertFalse(client.wasCommError(new RuntimeException("boom")));
    }
  }

  /** Minimal concrete subclass so the protected wasCommError can be exercised in isolation. */
  private static final class ProbeClient extends CloudSolrClient implements AutoCloseable {
    ProbeClient() {
      super(true, true, false);
    }

    @Override
    protected LBSolrClient getLbClient() {
      return null;
    }

    @Override
    public ClusterStateProvider getClusterStateProvider() {
      return null;
    }

    @Override
    public HttpSolrClient getHttpClient() {
      return null;
    }
  }
}
