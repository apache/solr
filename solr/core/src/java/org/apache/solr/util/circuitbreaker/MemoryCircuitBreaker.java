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
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.Locale;
import org.apache.solr.common.util.EnvUtils;

/**
 * Trips when post-collection live data in the JVM heap exceeds a configured percentage of the
 * maximum heap size.
 *
 * <p>The signal is read from {@link MemoryPoolMXBean#getCollectionUsage()} on the old/tenured heap
 * pool, which reports memory usage immediately after the most recent collection that affected that
 * pool. This is the only memory reading that distinguishes "live data" from "garbage waiting to be
 * collected."
 *
 * <p>Earlier versions of this breaker sampled {@link MemoryMXBean#getHeapMemoryUsage()} on a
 * 30-second moving average, which produced a high signal during normal operation: with a
 * generational collector, {@code used} climbs toward {@code max} between collections — that's the
 * steady-state shape, not a problem. The new signal updates only when an old-gen GC runs, which is
 * the only point at which "how full is the heap really?" has a defined answer.
 *
 * <p>Pool selection by collector:
 *
 * <ul>
 *   <li><b>G1 / Parallel / Serial / generational ZGC:</b> uses the pool whose name contains {@code
 *       Old} or {@code Tenured}.
 *   <li><b>Non-generational ZGC and Shenandoah:</b> single combined heap pool — the breaker sums
 *       {@code getCollectionUsage()} across every {@link MemoryType#HEAP} pool instead.
 * </ul>
 *
 * <p>The threshold semantics are unchanged: configure a percentage of the maximum heap size, and
 * the breaker trips when post-GC live data exceeds that percentage.
 */
public class MemoryCircuitBreaker extends CircuitBreaker {
  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

  private long heapMemoryThreshold;
  private final TtlSampledMetric heapLiveCache =
      new TtlSampledMetric(
          EnvUtils.getPropertyAsLong(
              CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS,
              CircuitBreakerRegistry.DEFAULT_SAMPLE_TTL_MS));

  private static final ThreadLocal<Long> seenMemory = ThreadLocal.withInitial(() -> 0L);
  private static final ThreadLocal<Long> allowedMemory = ThreadLocal.withInitial(() -> 0L);

  public MemoryCircuitBreaker() {
    super();
  }

  public MemoryCircuitBreaker setThreshold(double thresholdValueInPercentage) {
    long currentMaxHeap = MEMORY_MX_BEAN.getHeapMemoryUsage().getMax();
    if (currentMaxHeap <= 0) {
      throw new IllegalArgumentException("Invalid JVM state for the max heap usage");
    }

    double thresholdInFraction = thresholdValueInPercentage / 100.0;
    heapMemoryThreshold = (long) (currentMaxHeap * thresholdInFraction);

    if (heapMemoryThreshold <= 0) {
      throw new IllegalStateException("Memory limit cannot be less than or equal to zero");
    }
    return this;
  }

  @Override
  public boolean isTripped() {
    long localAllowedMemory = heapMemoryThreshold;
    long localSeenMemory = getAvgMemoryUsage();

    allowedMemory.set(localAllowedMemory);
    seenMemory.set(localSeenMemory);

    return localSeenMemory >= localAllowedMemory;
  }

  /**
   * Returns post-GC live bytes in the old-gen pool (or the sum across all heap pools when no
   * dedicated old-gen pool exists). Cached for {@link CircuitBreakerRegistry#SYSPROP_SAMPLE_TTL_MS}
   * so high-QPS callers don't repeatedly walk {@link MemoryPoolMXBean} list.
   *
   * <p>The historical name is preserved for source-compatibility with subclasses that override the
   * value source for testing; the implementation no longer averages anything.
   */
  protected long getAvgMemoryUsage() {
    return (long) heapLiveCache.get(MemoryCircuitBreaker::samplePostGcLiveBytes);
  }

  /**
   * Sum of {@code getCollectionUsage().getUsed()} across the old/tenured heap pool, falling back to
   * the union of all {@link MemoryType#HEAP} pools when no pool name contains "Old" or "Tenured"
   * (non-generational ZGC, Shenandoah).
   *
   * @return post-GC live bytes, or {@code 0} if no GC has yet run on a heap pool
   */
  static double samplePostGcLiveBytes() {
    long oldGenTotal = 0;
    boolean foundOld = false;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() != MemoryType.HEAP) {
        continue;
      }
      String name = pool.getName();
      if (name != null && (name.contains("Old") || name.contains("Tenured"))) {
        foundOld = true;
        MemoryUsage cu = pool.getCollectionUsage();
        if (cu != null) {
          oldGenTotal += cu.getUsed();
        }
      }
    }
    if (foundOld) {
      return oldGenTotal;
    }

    long allHeapTotal = 0;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() != MemoryType.HEAP) {
        continue;
      }
      MemoryUsage cu = pool.getCollectionUsage();
      if (cu != null) {
        allHeapTotal += cu.getUsed();
      }
    }
    return allHeapTotal;
  }

  @Override
  public String getErrorMessage() {
    return "Memory Circuit Breaker triggered as JVM heap usage values are greater than allocated threshold. "
        + "Seen JVM heap memory usage "
        + seenMemory.get()
        + " and allocated threshold "
        + allowedMemory.get();
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%s(threshold=%d, warnOnly=%b)",
        getClass().getSimpleName(),
        heapMemoryThreshold,
        isWarnOnly());
  }
}
