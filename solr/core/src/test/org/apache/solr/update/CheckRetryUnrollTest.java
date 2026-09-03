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
package org.apache.solr.update;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.ZkCoreNodeProps;
import org.junit.Test;

/**
 * Whether a retriable failure is retried should not depend on which exception is outermost.
 *
 * <p>Covers {@link SolrCmdDistributor.StdNode}. {@link SolrCmdDistributor.ForwardNode} carries the
 * same asymmetry and is changed the same way, but needs a live ZkStateReader to construct, so it
 * stays covered by SolrCmdDistributorTest rather than here.
 */
public class CheckRetryUnrollTest extends SolrTestCase {

  private static Replica replica() {
    Map<String, Object> props = new HashMap<>();
    props.put("base_url", "http://127.0.0.1:8983/solr");
    props.put("core", "collection1");
    props.put("node_name", "127.0.0.1:8983_solr");
    props.put("type", "NRT");
    props.put("state", "active");
    return new Replica("core_node1", props, "collection1", "shard1");
  }

  private static SolrCmdDistributor.Node node() {
    return new SolrCmdDistributor.StdNode(
        new ZkCoreNodeProps(replica()), "collection1", "shard1", /* maxRetries= */ 1);
  }

  private static boolean retries(Exception e) {
    SolrCmdDistributor.SolrError err = new SolrCmdDistributor.SolrError();
    err.e = e;
    return node().checkRetry(err);
  }

  @Test
  public void testRetriesWhenSocketExceptionIsWrappedInSolrServerException() {
    // the shape checkRetry already unwraps
    assertTrue(
        retries(new SolrServerException("wrapped", new SocketException("Connection reset"))));
  }

  @Test
  public void testRetriesWhenSocketExceptionIsTopLevel() {
    assertTrue(retries(new SocketException("Connection reset")));
  }

  @Test
  public void testRetriesWhenSocketExceptionIsWrappedInSomethingElse() {
    // the async (Jetty) path delivers a connection failure as an ExecutionException; the socket
    // cause is just as retriable as in the two cases above, but the outer type is not
    // SolrServerException so the leaf test never sees it.
    assertTrue(retries(new ExecutionException(new ConnectException("Connection refused"))));
  }

  @Test
  public void testRetriesWhenSocketExceptionIsNestedDeeply() {
    assertTrue(
        retries(
            new ExecutionException(
                new RuntimeException("io", new SocketException("Connection reset")))));
  }

  @Test
  public void testDoesNotRetryOnAServerErrorRootedInSomethingElse() {
    // control: the SolrServerException shape that already worked must keep its answer
    assertFalse(retries(new SolrServerException("wrapped", new IllegalStateException("nope"))));
  }

  @Test
  public void testClosedChannelExceptionIsStillNotRetriableEitherWay() {
    // Documents a limit of this change rather than a fix. ClosedChannelException is what the JDK
    // transport actually reports as the root cause of a dropped update connection, and it is not a
    // SocketException, so it stays non-retriable however the chain is inspected. Widening
    // isRetriableException is a separate behaviour decision.
    assertFalse(retries(new ExecutionException(new ClosedChannelException())));
    assertFalse(retries(new SolrServerException("wrapped", new ClosedChannelException())));
  }

  @Test
  public void testAnUnretriableNodeNeverRetriesHoweverTheChainLooks() {
    // The count ceiling lives in Req.shouldRetry, not here, but checkRetry has its own gate: a node
    // built with maxRetries=0 has retry==false and must refuse before the exception is even looked
    // at. Unrolling must not bypass that.
    SolrCmdDistributor.Node noRetries =
        new SolrCmdDistributor.StdNode(new ZkCoreNodeProps(replica()), "collection1", "shard1");
    SolrCmdDistributor.SolrError err = new SolrCmdDistributor.SolrError();
    err.e = new ExecutionException(new ConnectException("Connection refused"));
    assertFalse(noRetries.checkRetry(err));
  }

  @Test
  public void testRetriesWhenTheRetriableTypeIsNotTheRootCause() {
    // Jetty's ClientConnector wraps the underlying failure in a SocketException of its own
    // (ClientConnector#connect), so the retriable frame can sit above the root cause. Going
    // straight
    // to the root cause would miss it.
    assertTrue(
        retries(
            new ExecutionException(
                new SocketException("Could not connect to host", new IOException("underlying")))));
  }

  @Test
  public void testTerminatesOnACyclicCauseChain() {
    // A cause chain can be made cyclic, which is why the scan is bounded -- see the TODO on
    // SolrException#getRootCause. This must return rather than spin.
    Exception first = new Exception("first");
    Exception second = new Exception("second", first);
    try {
      first.initCause(second);
    } catch (IllegalStateException | IllegalArgumentException alreadySet) {
      // some JDKs refuse; nothing to assert then
      return;
    }
    assertFalse(retries(second));
  }

  @Test
  public void testDoesNotRetryWhenNothingInTheChainIsRetriable() {
    // control: unrolling must not make everything retriable
    assertFalse(retries(new ExecutionException(new IllegalStateException("not retriable"))));
    assertFalse(retries(new IllegalArgumentException("not retriable")));
  }
}
