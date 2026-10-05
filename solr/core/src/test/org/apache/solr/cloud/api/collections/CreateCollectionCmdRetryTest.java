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
package org.apache.solr.cloud.api.collections;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZooKeeperException;

/** Unit tests for the bounded retry {@link CreateCollectionCmd} puts around the alias write. */
public class CreateCollectionCmdRetryTest extends SolrTestCase {

  public void testSucceedsAfterOneZkFailure() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    CreateCollectionCmd.runWithBoundedRetries(
        () -> {
          if (attempts.incrementAndGet() < 2) {
            throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "transient");
          }
        },
        3,
        1);
    assertEquals(2, attempts.get());
  }

  public void testZkFailurePropagatesAfterAllAttempts() {
    AtomicInteger attempts = new AtomicInteger();
    ZooKeeperException thrown =
        expectThrows(
            ZooKeeperException.class,
            () ->
                CreateCollectionCmd.runWithBoundedRetries(
                    () -> {
                      attempts.incrementAndGet();
                      throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "still down");
                    },
                    3,
                    1));
    assertEquals("still down", thrown.getMessage());
    assertEquals(3, attempts.get());
  }

  public void testNonZkFailureIsNotRetried() {
    AtomicInteger attempts = new AtomicInteger();
    expectThrows(
        SolrException.class,
        () ->
            CreateCollectionCmd.runWithBoundedRetries(
                () -> {
                  attempts.incrementAndGet();
                  throw new SolrException(ErrorCode.SERVER_ERROR, "not a zk error");
                },
                3,
                1));
    assertEquals(1, attempts.get());
  }

  public void testInterruptDuringPauseStopsRetrying() {
    AtomicInteger attempts = new AtomicInteger();
    Thread.currentThread().interrupt();
    try {
      expectThrows(
          InterruptedException.class,
          () ->
              CreateCollectionCmd.runWithBoundedRetries(
                  () -> {
                    attempts.incrementAndGet();
                    throw new ZooKeeperException(ErrorCode.SERVER_ERROR, "transient");
                  },
                  3,
                  60_000));
      assertEquals(1, attempts.get());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      // do not leak the interrupt flag into the rest of the suite
      Thread.interrupted();
    }
  }
}
