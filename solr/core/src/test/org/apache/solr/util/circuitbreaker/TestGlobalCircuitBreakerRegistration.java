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

package org.apache.solr.util.circuitbreaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.After;
import org.junit.Test;

/**
 * Attempts to reproduce the race condition described in SOLR-18146
 *
 * <p>The race: {@link CircuitBreakerRegistry} relies on a static map of global (i.e.
 * non-core-specific) CircuitBreakers. If Solr was loading multiple cores on startup, these
 * concurrent core-loads could race when initializing this static state leading to
 * ConcurrentModificationExceptions, ArrayIndexOutOfBoundsExceptions, dropped circuit breaker
 * entries, etc.
 *
 * <p>These issues should be resolved by SOLR-18146, but the test remains to catch them if they
 * recur. Often useful to run in larger iterations with {@code -Ptests.iters=20}.
 */
public class TestGlobalCircuitBreakerRegistration extends SolrTestCaseJ4 {

  @After
  public void cleanup() {
    CircuitBreakerRegistry.deregisterGlobal();
  }

  @Test
  public void testConcurrentGlobalRegistrationDoesNotThrow() throws Exception {
    final int threadCount = 50;
    final int itersPerThread = 20;
    final int total = threadCount * itersPerThread;
    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
    final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());

    // Pre-create all circuit breakers before the barrier to make the race as "hot" as possible
    List<List<CircuitBreaker>> breakersPerThread = new ArrayList<>(threadCount);
    for (int t = 0; t < threadCount; t++) {
      List<CircuitBreaker> threadBreakers = new ArrayList<>(itersPerThread);
      for (int i = 0; i < itersPerThread; i++) {
        CircuitBreaker b =
            new CircuitBreaker() {
              @Override
              public boolean isTripped() {
                return false;
              }

              @Override
              public String getErrorMessage() {
                return "test";
              }
            };
        b.setRequestTypes(List.of("query"));
        threadBreakers.add(b);
      }
      breakersPerThread.add(threadBreakers);
    }

    ExecutorService executor =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            threadCount, new SolrNamedThreadFactory("test-concurrent-cb-registration"));
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threadCount; t++) {
        final List<CircuitBreaker> myBreakers = breakersPerThread.get(t);
        futures.add(
            executor.submit(
                () -> {
                  try {
                    // Release all threads simultaneously to maximize contention on
                    // the shared ArrayList for the QUERY key in globalCircuitBreakerMap.
                    barrier.await();
                    for (CircuitBreaker b : myBreakers) {
                      CircuitBreakerRegistry.registerGlobal(b);
                    }
                  } catch (BrokenBarrierException | InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (Throwable ex) {
                    // Captures ArrayIndexOutOfBoundsException (or similar) from
                    // concurrent ArrayList.add() when the race is triggered.
                    failures.add(ex);
                  }
                }));
      }
      for (Future<?> f : futures) {
        f.get();
      }
    } finally {
      ExecutorUtil.shutdownAndAwaitTermination(executor);
    }

    assertTrue(
        "Exceptions thrown during concurrent registerGlobal: " + failures, failures.isEmpty());

    // Even without a thrown exception the backing ArrayList can be silently
    // corrupted: concurrent adds may produce null slots or overwrite each other,
    // losing registrations.  Verify the invariant that all registered breakers are
    // non-null and that none were lost.
    Set<CircuitBreaker> registered = CircuitBreakerRegistry.listGlobal();
    assertFalse(
        "globalCircuitBreakerMap contains null entries — ArrayList corrupted by concurrent add()",
        registered.contains(null));
    assertEquals(
        "Expected "
            + total
            + " registered circuit breakers but found "
            + registered.size()
            + " — data was lost due to concurrent ArrayList.add() corruption",
        total,
        registered.size());
  }
}
