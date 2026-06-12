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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link MemoryCircuitBreaker} reads post-GC live data — not the raw, garbage-
 * inflated heap usage — and that {@link GcOverheadCircuitBreaker} computes a sensible GC ratio that
 * trips on sustained collection pressure.
 */
public class TestHeapPressureCircuitBreakers extends SolrTestCase {

  @Before
  public void setUpProps() {
    System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "0");
  }

  @After
  public void tearDownProps() {
    System.clearProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS);
    System.clearProperty(GcOverheadCircuitBreaker.SYSPROP_WINDOW_SECONDS);
  }

  // -------- MemoryCircuitBreaker --------

  @Test
  public void memoryBreakerReadsRealHeapPools() {
    // Sanity: the in-process JVM exposes at least one heap pool. samplePostGcLiveBytes() must
    // return a non-negative value computed from real pool data, regardless of which collector
    // is active. (Pre-first-GC, getCollectionUsage() can be zero on every pool — accept that.)
    boolean hasHeapPool = false;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP) {
        hasHeapPool = true;
        break;
      }
    }
    assertTrue("JVM must expose at least one HEAP MemoryPoolMXBean", hasHeapPool);

    double bytes = MemoryCircuitBreaker.samplePostGcLiveBytes();
    assertTrue("post-GC live bytes must be non-negative, got " + bytes, bytes >= 0);
  }

  @Test
  public void memoryBreakerTripsWhenOverThreshold() {
    MemoryCircuitBreaker breaker =
        new MemoryCircuitBreaker() {
          @Override
          protected long getAvgMemoryUsage() {
            return Long.MAX_VALUE; // simulate "heap is exhausted post-GC"
          }
        };
    breaker.setThreshold(50.0);
    assertTrue(breaker.isTripped());
  }

  @Test
  public void memoryBreakerDoesNotTripWhenUnderThreshold() {
    MemoryCircuitBreaker breaker =
        new MemoryCircuitBreaker() {
          @Override
          protected long getAvgMemoryUsage() {
            return 0L; // simulate "post-GC live data is empty"
          }
        };
    breaker.setThreshold(50.0);
    assertFalse(breaker.isTripped());
  }

  // -------- GcOverheadCircuitBreaker --------

  @Test
  public void gcOverheadBreakerSetThresholdValidates() {
    GcOverheadCircuitBreaker breaker = new GcOverheadCircuitBreaker();
    expectThrows(IllegalArgumentException.class, () -> breaker.setThreshold(0));
    expectThrows(IllegalArgumentException.class, () -> breaker.setThreshold(-1));
    expectThrows(IllegalArgumentException.class, () -> breaker.setThreshold(101));
  }

  @Test
  public void gcOverheadFirstCallReturnsFalse() {
    GcOverheadCircuitBreaker breaker = new GcOverheadCircuitBreaker();
    breaker.setThreshold(1.0);
    assertFalse("first call has no anchor sample yet — must not trip", breaker.isTripped());
  }

  @Test
  public void gcOverheadComputesRatioOverWindow() throws Exception {
    // Use a fake breaker that controls GC time so we can assert ratios without depending on
    // real GC behavior.
    final AtomicLong gcMs = new AtomicLong(0);
    GcOverheadCircuitBreaker breaker =
        new GcOverheadCircuitBreaker() {
          @Override
          protected long totalGcCollectionMs() {
            return gcMs.get();
          }
        };
    breaker.setThreshold(20.0);

    // First call installs the anchor and returns 0%.
    double first = breaker.computeGcOverheadPercent();
    assertEquals("first call returns 0", 0.0, first, 0.001);

    // Second call: time has advanced, GC has not — expect ~0%.
    Thread.sleep(20);
    double second = breaker.computeGcOverheadPercent();
    assertEquals("no GC time elapsed: ratio is 0%", 0.0, second, 0.001);
  }

  @Test
  public void gcOverheadFakeBreakerRatioMath() {
    // Drive a controllable breaker: spoof totalGcCollectionMs() to grow predictably. Use a
    // very long window so the anchor never slides during the test.
    System.setProperty(GcOverheadCircuitBreaker.SYSPROP_WINDOW_SECONDS, "3600");
    AtomicLong gcMs = new AtomicLong(0);
    GcOverheadCircuitBreaker breaker =
        new GcOverheadCircuitBreaker() {
          @Override
          protected long totalGcCollectionMs() {
            return gcMs.get();
          }
        };
    breaker.setThreshold(50.0);

    // Anchor at 0 GC, then add GC time and let real wall time elapse.
    breaker.computeGcOverheadPercent(); // installs anchor
    sleepMs(50);
    gcMs.set(40); // 40ms of GC over ~50ms of wall time → ~80% — over threshold.
    assertTrue("80% > 50%, breaker should trip", breaker.isTripped());

    // Now zero-out the additional GC delta over a fresh window: install a new fake.
    AtomicLong gcMs2 = new AtomicLong(0);
    GcOverheadCircuitBreaker quiet =
        new GcOverheadCircuitBreaker() {
          @Override
          protected long totalGcCollectionMs() {
            return gcMs2.get();
          }
        };
    quiet.setThreshold(50.0);
    quiet.computeGcOverheadPercent(); // anchor at 0
    sleepMs(50);
    // gcMs2 stays at 0 → 0% overhead → must not trip.
    assertFalse(quiet.isTripped());
  }

  private static void sleepMs(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
  }
}
