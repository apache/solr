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

import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Trips when post-GC live heap data exceeds a configured percentage of the maximum heap size.
 *
 * <p>"Live data" means heap occupancy measured immediately after a garbage collection, read from
 * {@link MemoryPoolMXBean#getCollectionUsage()} on the old/tenured heap pool. This is the only heap
 * reading that distinguishes live data from garbage waiting to be collected, and reading it is the
 * point of this breaker: raw {@link MemoryMXBean#getHeapMemoryUsage()} reports whole-heap occupancy
 * including uncollected garbage, which on a generational collector climbs toward {@code max}
 * between collections during normal operation. That raw value can sit at 99% one second before a
 * harmless GC drops it to 10%, so measuring it trips the breaker on transient pre-GC peaks the next
 * collection is about to reclaim. Post-GC live data is what lets the breaker answer "is the heap
 * actually full?" instead of "how much garbage has piled up since the last GC?".
 *
 * <p>Pool selection by collector. {@code getCollectionUsage()} is per-pool, so the breaker must
 * pick the pool that holds long-lived data — the old/tenured generation — and collectors name it
 * differently:
 *
 * <ul>
 *   <li><b>G1 / Parallel / Serial / generational ZGC:</b> uses the pool whose name matches the
 *       word-boundary pattern {@code \b(Old|Tenured)\b} (e.g. {@code "G1 Old Gen"}, {@code "PS Old
 *       Gen"}, {@code "Tenured Gen"}, {@code "ZGC Old Generation"}).
 *   <li><b>Non-generational ZGC and Shenandoah:</b> a single combined heap pool with no old-gen
 *       name to match — the breaker sums {@code getCollectionUsage()} across every {@link
 *       MemoryType#HEAP} pool instead.
 * </ul>
 *
 * <p>Pre-first-GC, {@link MemoryPoolMXBean#getCollectionUsage()} can return {@code null} on every
 * pool; in that case the breaker reports {@code 0} live bytes and will not trip until the JVM has
 * performed at least one collection on a heap pool.
 *
 * <p>Configure a percentage of the maximum heap size; the breaker trips when post-GC live data
 * exceeds that percentage.
 */
public class MemoryCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

  /**
   * Word-boundary match for the old/tenured generation pool name across HotSpot collectors.
   * Examples that match: {@code "G1 Old Gen"}, {@code "PS Old Gen"}, {@code "Tenured Gen"}, {@code
   * "ZGC Old Generation"}. Word boundaries prevent false positives such as a hypothetical pool
   * literally named {@code "ColdCache"}. Non-generational ZGC and Shenandoah expose a single
   * combined heap pool whose name matches none of these — the fallback path sums every HEAP pool
   * instead.
   */
  private static final Pattern OLD_GEN_NAME = Pattern.compile("\\b(Old|Tenured)\\b");

  /**
   * Lazily-initialized snapshot of the JVM heap pools. Pool resolution is deferred to the first
   * call to {@link #samplePostGcLiveBytes()} — i.e. the first request after the breaker is in use —
   * rather than performed at class load. This avoids any risk that a collector lazily creates its
   * old-generation pool after Solr's class loader has already touched {@link MemoryCircuitBreaker}
   * but before the GC subsystem has fully initialized. Once resolved the snapshot is stable for the
   * life of the JVM (the {@code MemoryPoolMXBean} list is fixed).
   */
  private static final class HeapPools {
    static final List<MemoryPoolMXBean> OLD_GEN;
    static final List<MemoryPoolMXBean> ALL_HEAP;

    static {
      List<MemoryPoolMXBean> oldGen = new ArrayList<>(2);
      List<MemoryPoolMXBean> allHeap = new ArrayList<>(4);
      List<String> heapNames = new ArrayList<>(4);
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        if (pool.getType() != MemoryType.HEAP) {
          continue;
        }
        allHeap.add(pool);
        String name = pool.getName();
        heapNames.add(name);
        if (name != null && OLD_GEN_NAME.matcher(name).find()) {
          oldGen.add(pool);
        }
      }
      OLD_GEN = List.copyOf(oldGen);
      ALL_HEAP = List.copyOf(allHeap);
      if (OLD_GEN.isEmpty() && ALL_HEAP.size() > 1) {
        // Recognized non-generational collectors (non-generational ZGC, Shenandoah) expose a
        // single combined heap pool, so the fallback sum is just that one pool. More than one heap
        // pool with no old-gen name is an unrecognized layout: summing post-GC usage across all of
        // them can include young-generation data and over-report live heap, which would make the
        // breaker trip on garbage the next GC reclaims. Warn so it is diagnosable rather than
        // silent.
        log.warn(
            "No heap pool name matched the old/tenured pattern {} but the JVM exposes {} heap "
                + "pools {}; MemoryCircuitBreaker will sum post-GC usage across all of them, which "
                + "can over-report live heap on this unrecognized collector layout.",
            OLD_GEN_NAME.pattern(),
            ALL_HEAP.size(),
            heapNames);
      }
    }
  }

  private long heapMemoryThreshold;

  // Per-instance (not shared): tests subclass this breaker and override getCurrentMemoryUsage() (or
  // samplePostGcLiveBytes()). See CircuitBreaker#newSampleCache().
  private final TtlSampledMetric<Long> heapLiveCache = newSampleCache();

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
    long localSeenMemory = getCurrentMemoryUsage();

    allowedMemory.set(localAllowedMemory);
    seenMemory.set(localSeenMemory);

    return localSeenMemory >= localAllowedMemory;
  }

  /**
   * Returns the post-GC live heap bytes used by {@link #isTripped()}, cached for {@link
   * CircuitBreakerRegistry#SYSPROP_SAMPLE_TTL_MS} ms so high-QPS callers don't repeatedly walk the
   * heap pool list.
   *
   * <p>Tests that inject a synthetic value typically override this method directly (which bypasses
   * the cache); tests that want to feed a synthetic sample <em>through</em> the cache should
   * override {@link #samplePostGcLiveBytes()} instead.
   */
  protected long getCurrentMemoryUsage() {
    return heapLiveCache.get(this::samplePostGcLiveBytes);
  }

  /**
   * Sum of {@code getCollectionUsage().getUsed()} across the old/tenured heap pool, falling back to
   * the union of all {@link MemoryType#HEAP} pools when no pool name matches {@link #OLD_GEN_NAME}
   * (non-generational ZGC, Shenandoah). Pool resolution is deferred to the first call to this
   * method — see {@link HeapPools}.
   *
   * <p>Visible for subclassing in tests that want to feed a synthetic sample through the TTL cache.
   * Most tests override {@link #getCurrentMemoryUsage()} instead and bypass the cache entirely.
   *
   * @return post-GC live bytes, or {@code 0} if no GC has yet run on a heap pool
   */
  protected long samplePostGcLiveBytes() {
    List<MemoryPoolMXBean> pools =
        HeapPools.OLD_GEN.isEmpty() ? HeapPools.ALL_HEAP : HeapPools.OLD_GEN;
    long total = 0;
    for (MemoryPoolMXBean pool : pools) {
      MemoryUsage cu = pool.getCollectionUsage();
      if (cu != null) {
        total += cu.getUsed();
      }
    }
    return total;
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
