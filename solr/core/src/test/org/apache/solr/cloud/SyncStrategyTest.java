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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.client.solrj.RemoteSolrException;
import org.junit.Test;

/**
 * Unit tests for the recovery-request retry in {@link SyncStrategy}: only the exact "core is still
 * loading" 503 from REQUESTRECOVERY may be retried, and only up to the attempt limit.
 */
public class SyncStrategyTest extends SolrTestCase {

  private static final String REPLICA_URL = "http://localhost:8983/solr/core1";
  private static final String CORE_NAME = "core1";

  private static RemoteSolrException stillLoading() {
    return new RemoteSolrException(
        "http://localhost:8983/solr", 503, "Core core1 is still loading", null);
  }

  private static void sendWithRetry(SyncStrategy.RecoveryRequestSender sender) {
    SyncStrategy.sendRecoveryRequestWithRetry(sender, () -> false, 0, REPLICA_URL, CORE_NAME);
  }

  @Test
  public void testRetriesStillLoadingThenSucceeds() {
    AtomicInteger sends = new AtomicInteger();
    sendWithRetry(
        () -> {
          if (sends.incrementAndGet() == 1) {
            throw stillLoading();
          }
        });
    assertEquals(2, sends.get());
  }

  @Test
  public void testStillLoadingIsRetriedUpToAttemptLimit() {
    AtomicInteger sends = new AtomicInteger();
    // Exhausting the attempts is not an error: the failure is logged and dropped.
    sendWithRetry(
        () -> {
          sends.incrementAndGet();
          throw stillLoading();
        });
    assertEquals(3, sends.get());
  }

  @Test
  public void testOther503IsNotRetried() {
    AtomicInteger sends = new AtomicInteger();
    sendWithRetry(
        () -> {
          sends.incrementAndGet();
          throw new RemoteSolrException(
              "http://localhost:8983/solr", 503, "some other service unavailable failure", null);
        });
    assertEquals(1, sends.get());
  }

  @Test
  public void testLoadingMessageWithWrongCodeIsNotRetried() {
    AtomicInteger sends = new AtomicInteger();
    sendWithRetry(
        () -> {
          sends.incrementAndGet();
          throw new RemoteSolrException(
              "http://localhost:8983/solr", 500, "Core core1 is still loading", null);
        });
    assertEquals(1, sends.get());
  }

  @Test
  public void testErrorFromFirstAttemptPropagates() {
    AtomicInteger sends = new AtomicInteger();
    AssertionError err =
        expectThrows(
            AssertionError.class,
            () ->
                sendWithRetry(
                    () -> {
                      sends.incrementAndGet();
                      throw new AssertionError("boom");
                    }));
    assertEquals("boom", err.getMessage());
    assertEquals(1, sends.get());
  }

  @Test
  public void testErrorFromRetryAttemptPropagates() {
    AtomicInteger sends = new AtomicInteger();
    expectThrows(
        AssertionError.class,
        () ->
            sendWithRetry(
                () -> {
                  if (sends.incrementAndGet() == 1) {
                    throw stillLoading();
                  }
                  throw new AssertionError("boom on retry");
                }));
    assertEquals(2, sends.get());
  }

  @Test
  public void testClosedStopsRetry() {
    AtomicInteger sends = new AtomicInteger();
    SyncStrategy.sendRecoveryRequestWithRetry(
        () -> {
          sends.incrementAndGet();
          throw stillLoading();
        },
        () -> true,
        0,
        REPLICA_URL,
        CORE_NAME);
    assertEquals(1, sends.get());
  }

  @Test
  public void testInterruptDuringWaitStopsRetry() throws Exception {
    AtomicInteger sends = new AtomicInteger();
    AtomicBoolean interruptPreserved = new AtomicBoolean();
    Thread t =
        new Thread(
            () -> {
              SyncStrategy.sendRecoveryRequestWithRetry(
                  () -> {
                    sends.incrementAndGet();
                    throw stillLoading();
                  },
                  () -> false,
                  60000,
                  REPLICA_URL,
                  CORE_NAME);
              interruptPreserved.set(Thread.currentThread().isInterrupted());
            });
    t.start();
    // Wait until the first attempt has happened, then interrupt the 60s wait.
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (sends.get() == 0 && System.nanoTime() < deadlineNanos) {
      Thread.sleep(10);
    }
    assertEquals(1, sends.get());
    t.interrupt();
    t.join(10000);
    assertFalse("retry thread should have stopped after interrupt", t.isAlive());
    assertEquals(1, sends.get());
    assertTrue("interrupt status should be restored", interruptPreserved.get());
  }
}
