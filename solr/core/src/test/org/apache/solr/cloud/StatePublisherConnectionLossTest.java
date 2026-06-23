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
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

/**
 * Unit test for the StatePublisher connection-loss dedup path (review finding #4).
 *
 * <p>StatePublisher keeps a short-lived per-id dedup window so a caller that re-submits the same
 * (id, state) within {@code solr.statePublisher.dedupMaxAgeMs} is not republished. The dedup entry
 * is stamped at submit time, BEFORE the bulk message is actually persisted to the overseer queue.
 * If the persist fails — including the SYNCHRONOUS {@link KeeperException.ConnectionLossException}
 * thrown straight out of {@code SolrZkClient.create(...)} before the async callback ever fires —
 * the stamped dedup entry is stale: it claims a publish happened that did not. Left in place, the
 * dedup window would suppress the caller's identical retry of that failed (non-LEADER) transition,
 * and the transition would be lost permanently.
 *
 * <p>This test drives a real {@code Worker.processMessage} whose {@code create(...)} throws a
 * synchronous ConnectionLoss and asserts that the dedup record is cleared so the retry is no longer
 * suppressed within the age window.
 */
public class StatePublisherConnectionLossTest extends SolrTestCaseJ4 {

  /** Raw short state for DOWN; any non-LEADER state exercises the dedup path identically. */
  private static final int DOWN = 5;

  @Test
  @SuppressWarnings("unchecked")
  public void testSynchronousConnectionLossClearsStaleDedupRecord() throws Exception {
    ZkStateReader reader = mock(ZkStateReader.class);
    SolrZkClient zk = mock(SolrZkClient.class);
    when(reader.getZkClient()).thenReturn(zk);
    // The persist throws ConnectionLoss synchronously, before the async rc-callback can run.
    doThrow(new KeeperException.ConnectionLossException())
        .when(zk)
        .create(
            anyString(),
            any(byte[].class),
            any(CreateMode.class),
            any(AsyncCallback.Create2Callback.class));

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

    // Drive a real processMessage whose synchronous create() throws ConnectionLoss. processMessage
    // is private on the private inner Worker; reach it via reflection so production visibility is
    // unchanged.
    Class<?> workerClass = Class.forName("org.apache.solr.cloud.StatePublisher$Worker");
    Constructor<?> workerCtor = workerClass.getDeclaredConstructor(StatePublisher.class);
    workerCtor.setAccessible(true);
    Object worker = workerCtor.newInstance(sp);
    Method processMessage = workerClass.getDeclaredMethod("processMessage", Map.class);
    processMessage.setAccessible(true);

    Map<String, Object> bulk = new HashMap<>();
    bulk.put("operation", "state");
    bulk.put(core, Integer.toString(DOWN));
    // The synchronous ConnectionLoss must be swallowed by the catch block, not propagated.
    processMessage.invoke(worker, bulk);

    // The persist was actually attempted (guards against a false pass where create() never ran).
    verify(zk, times(1))
        .create(
            anyString(),
            any(byte[].class),
            any(CreateMode.class),
            any(AsyncCallback.Create2Callback.class));

    // The stale dedup entry must have been cleared, so the caller's identical retry of the failed
    // transition is NOT suppressed within the age window (returns false == not a duplicate).
    assertFalse(
        "synchronous ConnectionLoss must clear dedup so the retry is not suppressed",
        (boolean) isDuplicatePublish.invoke(sp, core, DOWN, now));
  }
}
