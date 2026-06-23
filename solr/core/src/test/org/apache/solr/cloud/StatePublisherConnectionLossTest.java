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
package org.apache.solr.cloud;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

/**
 * Unit test for the StatePublisher durable-publish + dedup interaction (review findings #1 / #4).
 *
 * <p>StatePublisher keeps a short-lived per-id dedup window so a caller that re-submits the same
 * (id, state) within {@code solr.statePublisher.dedupMaxAgeMs} is not republished. The dedup entry
 * is stamped at submit time, BEFORE the bulk message is actually persisted to the overseer queue.
 *
 * <p>{@code processMessage} now persists the batch DURABLY via the synchronous
 * {@link SolrZkClient#create(String, byte[], CreateMode, boolean)} with {@code retryOnConnLoss=true}
 * (review P0 #1): the previous fire-and-forget async create dropped edge-triggered transitions
 * permanently when the create never reached the queue. A plain {@code ConnectionLoss} is retried
 * inside {@code create(...)} until ZK accepts the write, so it never surfaces here. A TERMINAL
 * failure that DOES escape (e.g. {@link KeeperException.SessionExpiredException}, which is not
 * retried) leaves the stamped dedup entry stale — it claims a publish happened that did not. So on
 * any escaping {@link KeeperException} the publisher clears the dedup cache (a backstop so the
 * caller's identical retry is not suppressed within the age window) and then PROPAGATES the
 * exception so the run loop logs it.
 *
 * <p>This test drives a real {@code Worker.processMessage} whose durable {@code create(...)} throws a
 * terminal {@code SessionExpired} and asserts both behaviors: the dedup record is cleared, and the
 * exception is propagated (not swallowed).
 */
public class StatePublisherConnectionLossTest extends SolrTestCaseJ4 {

  /** Raw short state for DOWN; any non-LEADER state exercises the dedup path identically. */
  private static final int DOWN = 5;

  @Test
  @SuppressWarnings("unchecked")
  public void testTerminalPersistFailureClearsDedupAndPropagates() throws Exception {
    ZkStateReader reader = mock(ZkStateReader.class);
    SolrZkClient zk = mock(SolrZkClient.class);
    when(reader.getZkClient()).thenReturn(zk);
    // The durable persist throws a terminal SessionExpired (ConnectionLoss is retried inside
    // create(...) and would never escape, so it cannot exercise the clear-dedup escape path).
    doThrow(new KeeperException.SessionExpiredException())
        .when(zk)
        .create(anyString(), any(byte[].class), any(CreateMode.class), anyBoolean());

    StatePublisher sp = new StatePublisher(reader, null);

    final String core = "core_node1";
    final long now = 1_000_000L;

    Method isDuplicatePublish =
        StatePublisher.class.getDeclaredMethod(
            "isDuplicatePublish", String.class, int.class, long.class);
    isDuplicatePublish.setAccessible(true);

    // The first publish records the (core, DOWN) dedup entry; an identical publish within the age
    // window is suppressed. This is the window that must NOT survive a failed persist.
    assertFalse(
        "first publish records, does not dedup",
        (boolean) isDuplicatePublish.invoke(sp, core, DOWN, now));
    assertTrue(
        "identical publish within the age window is deduped",
        (boolean) isDuplicatePublish.invoke(sp, core, DOWN, now));

    // Drive a real processMessage whose durable create() throws a terminal SessionExpired.
    // processMessage is private on the private inner Worker; reach it via reflection so production
    // visibility is unchanged.
    Class<?> workerClass = Class.forName("org.apache.solr.cloud.StatePublisher$Worker");
    Constructor<?> workerCtor = workerClass.getDeclaredConstructor(StatePublisher.class);
    workerCtor.setAccessible(true);
    Object worker = workerCtor.newInstance(sp);
    Method processMessage = workerClass.getDeclaredMethod("processMessage", Map.class);
    processMessage.setAccessible(true);

    Map<String, Object> bulk = new HashMap<>();
    bulk.put("operation", "state");
    bulk.put(core, Integer.toString(DOWN));

    // The terminal KeeperException must PROPAGATE out of processMessage (the run loop logs it),
    // not be swallowed. Reflection wraps it in InvocationTargetException.
    InvocationTargetException ite =
        expectThrows(InvocationTargetException.class, () -> processMessage.invoke(worker, bulk));
    assertTrue(
        "terminal persist failure must propagate as a KeeperException, got: " + ite.getCause(),
        ite.getCause() instanceof KeeperException);

    // The durable persist was actually attempted (guards against a false pass where create() never ran).
    verify(zk, times(1))
        .create(anyString(), any(byte[].class), any(CreateMode.class), anyBoolean());

    // The stale dedup entry must have been cleared, so the caller's identical retry of the failed
    // transition is NOT suppressed within the age window (returns false == not a duplicate).
    assertFalse(
        "terminal persist failure must clear dedup so the retry is not suppressed",
        (boolean) isDuplicatePublish.invoke(sp, core, DOWN, now));
  }
}
