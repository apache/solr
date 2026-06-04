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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link MemoryCircuitBreaker} reads post-GC live data — not the raw, garbage-
 * inflated heap usage — so it does not trip on transient pre-collection peaks that GC will reclaim.
 */
public class TestHeapPressureCircuitBreakers extends SolrTestCase {

  @Before
  public void setUpProps() {
    System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "0");
  }

  @After
  public void tearDownProps() {
    System.clearProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS);
  }

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

    long bytes = new MemoryCircuitBreaker().samplePostGcLiveBytes();
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

  @Test
  public void samplePostGcLiveBytesIsCachedThroughGetAvgMemoryUsage() {
    // With a long TTL, getAvgMemoryUsage() should consult samplePostGcLiveBytes() at most once
    // across many isTripped() invocations — i.e. the cache wraps the overridable hook, not just
    // the static heap-pool walk.
    System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "60000");
    try {
      AtomicInteger calls = new AtomicInteger();
      MemoryCircuitBreaker breaker =
          new MemoryCircuitBreaker() {
            @Override
            protected long samplePostGcLiveBytes() {
              calls.incrementAndGet();
              return 0L;
            }
          };
      breaker.setThreshold(50.0);
      for (int i = 0; i < 50; i++) {
        breaker.isTripped();
      }
      assertEquals(
          "samplePostGcLiveBytes() must be invoked at most once per TTL window", 1, calls.get());
    } finally {
      System.setProperty(CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS, "0");
    }
  }
}
