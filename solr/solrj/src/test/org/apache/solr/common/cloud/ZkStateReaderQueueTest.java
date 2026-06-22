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
package org.apache.solr.common.cloud;

import java.util.concurrent.CompletionException;

import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

/**
 * Unit tests for {@link ZkStateReaderQueue#isTransientZkFailure(Throwable)}, the predicate that
 * decides whether a failed cluster-state fetch is re-enqueued. Re-enqueuing a non-transient failure
 * (e.g. NoNode) would hot-spin forever; failing to re-enqueue a transient failure leaves the node's
 * view of the collection permanently stale (the bug this guards).
 */
public class ZkStateReaderQueueTest extends SolrTestCase {

  @Test
  public void testConnectionLevelFailuresAreTransient() {
    assertTrue(ZkStateReaderQueue.isTransientZkFailure(
        new KeeperException.ConnectionLossException()));
    assertTrue(ZkStateReaderQueue.isTransientZkFailure(
        new KeeperException.OperationTimeoutException()));
    assertTrue(ZkStateReaderQueue.isTransientZkFailure(
        new KeeperException.SessionExpiredException()));
    assertTrue(ZkStateReaderQueue.isTransientZkFailure(
        new KeeperException.SessionMovedException()));
  }

  @Test
  public void testStructuralFailuresAreNotTransient() {
    assertFalse(ZkStateReaderQueue.isTransientZkFailure(
        new KeeperException.NoNodeException()));
    assertFalse(ZkStateReaderQueue.isTransientZkFailure(
        new KeeperException.BadVersionException()));
    assertFalse(ZkStateReaderQueue.isTransientZkFailure(
        new IllegalStateException("boom")));
    assertFalse(ZkStateReaderQueue.isTransientZkFailure(null));
  }

  @Test
  public void testTransienceIsDetectedThroughWrapperChain() {
    // The fetch futures wrap the original KeeperException in SolrException / CompletionException;
    // classification must unwrap the cause chain rather than only inspecting the top exception.
    Throwable wrapped = new CompletionException(
        new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            new KeeperException.ConnectionLossException()));
    assertTrue(ZkStateReaderQueue.isTransientZkFailure(wrapped));

    Throwable wrappedStructural = new CompletionException(
        new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            new KeeperException.NoNodeException()));
    assertFalse(ZkStateReaderQueue.isTransientZkFailure(wrappedStructural));
  }
}
