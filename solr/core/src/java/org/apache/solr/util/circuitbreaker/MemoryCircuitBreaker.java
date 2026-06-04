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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import org.apache.solr.common.util.EnvUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 *   <li><b>G1 / Parallel / Serial / generational ZGC:</b> uses the pool whose name matches the
 *       word-boundary pattern {@code \b(Old|Tenured)\b}.
 *   <li><b>Non-generational ZGC and Shenandoah:</b> single combined heap pool — the breaker sums
 *       {@code getCollectionUsage()} across every {@link MemoryType#HEAP} pool instead.
 * </ul>
 *
 * <p>Pre-first-GC, {@link MemoryPoolMXBean#getCollectionUsage()} can return {@code null} on every
 * pool; in that case the breaker reports {@code 0} live bytes and will not trip until the JVM has
 * performed at least one collection on a heap pool.
 *
 * <p>The threshold semantics are unchanged: configure a percentage of the maximum heap size, and
 * the breaker trips when post-GC live data exceeds that percentage.
 */
public class MemoryCircuitBreaker extends CircuitBreaker {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

  /** Guards the one-shot deprecation warning emitted by {@link #MemoryCircuitBreaker(int, int)}. */
  private static final AtomicBoolean DEPRECATED_CTOR_WARNED = new AtomicBoolean(false);

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
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
        if (pool.getType() != MemoryType.HEAP) {
          continue;
        }
        allHeap.add(pool);
        String name = pool.getName();
        if (name != null && OLD_GEN_NAME.matcher(name).find()) {
          oldGen.add(pool);
        }
      }
      OLD_GEN = List.copyOf(oldGen);
      ALL_HEAP = List.copyOf(allHeap);
    }
  }

  private long heapMemoryThreshold;

  // Per-instance cache: tests subclass this breaker and override getAvgMemoryUsage() (and may in
  // future override samplePostGcLiveBytes()); a single shared cache would return values produced
  // by whichever instance won the first CAS to all subsequent instances. The TTL still bounds the
  // rate of underlying MemoryPoolMXBean walks per instance.
  private final TtlSampledMetric<Long> heapLiveCache =
      new TtlSampledMetric<>(
          EnvUtils.getPropertyAsLong(
              CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS,
              CircuitBreakerRegistry.DEFAULT_SAMPLE_TTL_MS));

  private static final ThreadLocal<Long> seenMemory = ThreadLocal.withInitial(() -> 0L);
  private static final ThreadLocal<Long> allowedMemory = ThreadLocal.withInitial(() -> 0L);

  public MemoryCircuitBreaker() {
    super();
  }

  /**
   * @deprecated Retained only for backwards source-compatibility with subclasses that called the
   *     pre-Solr-10.1 constructor. The arguments are ignored: the breaker no longer averages
   *     samples — see the class javadoc. Subclasses should use {@link #MemoryCircuitBreaker()}.
   */
  @Deprecated(forRemoval = true)
  protected MemoryCircuitBreaker(int numSamples, int sampleInterval) {
    this();
    if (DEPRECATED_CTOR_WARNED.compareAndSet(false, true)) {
      log.warn(
          "MemoryCircuitBreaker(int, int) is deprecated and its arguments (numSamples={}, "
              + "sampleInterval={}) are ignored: the breaker no longer averages samples and now "
              + "reads post-GC live heap data directly. See the MemoryCircuitBreaker javadoc. "
              + "Switch to the no-arg constructor.",
          numSamples,
          sampleInterval);
    }
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
   * Returns post-GC live bytes for use by {@link #isTripped()}, cached for {@link
   * CircuitBreakerRegistry#SYSPROP_SAMPLE_TTL_MS} ms so high-QPS callers don't repeatedly walk the
   * heap pool list.
   *
   * <p>The historical name is preserved for source-compatibility. Tests that need to inject a
   * synthetic value typically override this method directly (which bypasses the cache); tests that
   * want to feed a synthetic sample <em>through</em> the cache should override {@link
   * #samplePostGcLiveBytes()} instead. The implementation no longer averages anything.
   */
  protected long getAvgMemoryUsage() {
    return heapLiveCache.get(this::samplePostGcLiveBytes);
  }

  /**
   * Sum of {@code getCollectionUsage().getUsed()} across the old/tenured heap pool, falling back to
   * the union of all {@link MemoryType#HEAP} pools when no pool name matches {@link #OLD_GEN_NAME}
   * (non-generational ZGC, Shenandoah). Pool resolution is deferred to the first call to this
   * method — see {@link HeapPools}.
   *
   * <p>Visible for subclassing in tests that want to feed a synthetic sample through the TTL cache.
   * Most tests override {@link #getAvgMemoryUsage()} instead and bypass the cache entirely.
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
