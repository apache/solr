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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.solr.SolrTestCase;
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
}
