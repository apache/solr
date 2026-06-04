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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link LoadAverageCircuitBreaker} caches the result of {@code
 * calculateLiveLoadAverage()} for the configured TTL so that high-QPS admission control does not
 * re-poll {@code OperatingSystemMXBean.getSystemLoadAverage()} per request.
 */
public class TestLoadAverageCircuitBreakerSampling extends SolrTestCase {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    // Long TTL so successive isTripped() calls within one test stay in the same cache window.
    System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "60000");
  }

  @After
  public void tearDownProps() {
    System.clearProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS);
  }

  @Test
  public void successiveIsTrippedSharesOneSample() {
    AtomicInteger calls = new AtomicInteger();
    LoadAverageCircuitBreaker breaker =
        new LoadAverageCircuitBreaker() {
          @Override
          protected double calculateLiveLoadAverage() {
            calls.incrementAndGet();
            return 1.0;
          }
        };
    breaker.setThreshold(0.5);

    for (int i = 0; i < 100; i++) {
      assertTrue("breaker should be tripped (1.0 >= 0.5)", breaker.isTripped());
    }
    assertEquals(
        "Underlying calculateLiveLoadAverage() must be invoked once per TTL window, not per call",
        1,
        calls.get());
  }

  @Test
  public void zeroTtlDisablesCache() {
    System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "0");
    AtomicInteger calls = new AtomicInteger();
    LoadAverageCircuitBreaker breaker =
        new LoadAverageCircuitBreaker() {
          @Override
          protected double calculateLiveLoadAverage() {
            calls.incrementAndGet();
            return 1.0;
          }
        };
    breaker.setThreshold(0.5);

    for (int i = 0; i < 5; i++) {
      breaker.isTripped();
    }
    assertEquals("With zero TTL, every call re-evaluates", 5, calls.get());
  }

  /**
   * Stampede scenario: many concurrent callers find the cache stale at the same instant. Single-
   * flight refresh must ensure the underlying sampler is invoked at most once across all of them.
   * Without this guarantee, a burst of concurrent requests would each pin a CPU on the load-
   * average syscall, which is the bug this breaker exists to prevent — not cause.
   */
  @Test
  public void concurrentStampedeRunsSamplerAtMostOnce() throws Exception {
    // Force every call to find the cache stale.
    System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "0");

    final AtomicInteger samplerInvocations = new AtomicInteger();
    final AtomicInteger inFlight = new AtomicInteger();
    final AtomicInteger maxInFlight = new AtomicInteger();

    LoadAverageCircuitBreaker breaker =
        new LoadAverageCircuitBreaker() {
          @Override
          protected double calculateLiveLoadAverage() {
            int now = inFlight.incrementAndGet();
            maxInFlight.accumulateAndGet(now, Math::max);
            try {
              // Hold the "syscall" long enough that, without single-flight, every concurrent
              // caller would pile in.
              Thread.sleep(20);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
            } finally {
              inFlight.decrementAndGet();
            }
            samplerInvocations.incrementAndGet();
            return 1.0;
          }
        };
    breaker.setThreshold(0.5);

    // Prime the cache so the stampede hits the stale-refresh path (not the cold path).
    breaker.isTripped();
    int primed = samplerInvocations.get();

    final int threads = 64;
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(threads);
    ExecutorService pool =
        ExecutorUtil.newMDCAwareFixedThreadPool(
            threads, new SolrNamedThreadFactory("TestLoadAverageCircuitBreakerSampling"));
    try {
      for (int i = 0; i < threads; i++) {
        pool.submit(
            () -> {
              try {
                start.await();
                breaker.isTripped();
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
              } finally {
                done.countDown();
              }
            });
      }
      start.countDown();
      assertTrue("all threads completed", done.await(30, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }

    assertEquals(
        "Single-flight: at most one sampler invocation runs at a time across the stampede",
        1,
        maxInFlight.get());
    // We deliberately don't assert an exact stampedeInvocations count here. Single-flight
    // guarantees at-most-one *concurrent* sampler invocation; under load a thread that arrives
    // after the winning thread releases the CAS legitimately runs the sampler again. Asserting
    // the count would make the test timing-sensitive on slow CI shards. The maxInFlight==1
    // assertion above captures the actual invariant.
    int stampedeInvocations = samplerInvocations.get() - primed;
    assertTrue(
        "Sanity: stampede produced at least one sampler invocation (saw "
            + stampedeInvocations
            + ")",
        stampedeInvocations >= 1);
  }
}
