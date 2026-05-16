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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.solr.common.util.EnvUtils;

/**
 * Trips when the JVM is spending more than a configured percentage of wall-clock time in garbage
 * collection over a sliding window.
 *
 * <p>Complementary to {@link MemoryCircuitBreaker}: that breaker fires when post-GC live data is
 * exhausting the heap; this one fires when GC is keeping up (live data may even be small) but
 * consuming so much CPU that the application is starving. Both conditions usually precede an OOM
 * but each catches the other's blind spot.
 *
 * <p>The percentage is computed as
 *
 * <pre>{@code
 * sum(GarbageCollectorMXBean.getCollectionTime()) over window
 *   ─────────────────────────────────────────────────────────  × 100
 *                       wall-clock window
 * }</pre>
 *
 * <p>The window length defaults to {@link #DEFAULT_WINDOW_SECONDS} seconds and is configurable via
 * {@value #SYSPROP_WINDOW_SECONDS}. Within one window the breaker reports the GC ratio over the
 * time elapsed since the anchor sample; once the window length is exceeded, the anchor slides
 * forward to the most recent sample so the ratio reflects only recent behavior.
 *
 * <p>{@link #isTripped()} is rate-limited by the same {@code TtlSampledMetric} used by the
 * load-average and CPU breakers ({@value CircuitBreakerRegistry#SYSPROP_SAMPLE_TTL_MS}), so
 * concurrent admission-control callers share one ratio computation per cache window.
 */
public class GcOverheadCircuitBreaker extends CircuitBreaker {

  public static final String SYSPROP_WINDOW_SECONDS =
      CircuitBreakerRegistry.SYSPROP_PREFIX + "gcoverhead.windowSeconds";
  public static final long DEFAULT_WINDOW_SECONDS = 30L;

  private static final List<GarbageCollectorMXBean> GC_BEANS =
      List.copyOf(ManagementFactory.getGarbageCollectorMXBeans());

  private double overheadThresholdPercent;
  private final long windowNanos;
  private final TtlSampledMetric ratioCache;
  private final AtomicReference<Sample> anchor = new AtomicReference<>();

  private static final ThreadLocal<Double> seenOverheadPercent = ThreadLocal.withInitial(() -> 0.0);
  private static final ThreadLocal<Double> allowedOverheadPercent =
      ThreadLocal.withInitial(() -> 0.0);

  public GcOverheadCircuitBreaker() {
    super();
    long windowSeconds = EnvUtils.getPropertyAsLong(SYSPROP_WINDOW_SECONDS, DEFAULT_WINDOW_SECONDS);
    if (windowSeconds <= 0) {
      throw new IllegalArgumentException(
          "Invalid GC overhead window: " + windowSeconds + "s (must be positive)");
    }
    this.windowNanos = TimeUnit.SECONDS.toNanos(windowSeconds);
    this.ratioCache =
        new TtlSampledMetric(
            EnvUtils.getPropertyAsLong(
                CircuitBreakerRegistry.SYSPROP_SAMPLE_TTL_MS,
                CircuitBreakerRegistry.DEFAULT_SAMPLE_TTL_MS));
  }

  public GcOverheadCircuitBreaker setThreshold(double thresholdValueInPercentage) {
    if (thresholdValueInPercentage <= 0 || thresholdValueInPercentage > 100) {
      throw new IllegalArgumentException(
          "GC overhead threshold must be in (0, 100]; got " + thresholdValueInPercentage);
    }
    this.overheadThresholdPercent = thresholdValueInPercentage;
    return this;
  }

  public double getOverheadThresholdPercent() {
    return overheadThresholdPercent;
  }

  @Override
  public boolean isTripped() {
    double localAllowed = overheadThresholdPercent;
    double localSeen = ratioCache.get(this::computeGcOverheadPercent);

    allowedOverheadPercent.set(localAllowed);
    seenOverheadPercent.set(localSeen);

    return localSeen >= localAllowed;
  }

  /**
   * Compute the percentage of wall time spent in GC since the anchor sample. The first call after
   * construction installs the anchor and returns 0; once the window length is exceeded, the anchor
   * advances to the current sample so future readings reflect only recent activity.
   */
  protected double computeGcOverheadPercent() {
    long nowNs = System.nanoTime();
    long nowGcMs = totalGcCollectionMs();

    Sample existing = anchor.get();
    if (existing == null) {
      anchor.compareAndSet(null, new Sample(nowNs, nowGcMs));
      return 0.0;
    }

    long elapsedNs = nowNs - existing.nanos;
    if (elapsedNs <= 0) {
      return 0.0;
    }
    long deltaGcMs = nowGcMs - existing.gcMs;
    double pct = 100.0 * deltaGcMs * 1_000_000.0 / elapsedNs;

    if (elapsedNs >= windowNanos) {
      // Slide the anchor forward so subsequent readings cover only recent activity.
      anchor.compareAndSet(existing, new Sample(nowNs, nowGcMs));
    }
    return pct;
  }

  protected long totalGcCollectionMs() {
    long sum = 0;
    for (GarbageCollectorMXBean b : GC_BEANS) {
      long t = b.getCollectionTime();
      if (t > 0) {
        sum += t;
      }
    }
    return sum;
  }

  @Override
  public String getErrorMessage() {
    return "GC Overhead Circuit Breaker triggered: JVM has spent "
        + String.format(Locale.ROOT, "%.1f", seenOverheadPercent.get())
        + "% of wall time in garbage collection (allowed threshold "
        + String.format(Locale.ROOT, "%.1f", allowedOverheadPercent.get())
        + "%)";
  }

  @Override
  public String toString() {
    return String.format(
        Locale.ROOT,
        "%s(threshold=%f%%, windowSeconds=%d, warnOnly=%b)",
        getClass().getSimpleName(),
        overheadThresholdPercent,
        TimeUnit.NANOSECONDS.toSeconds(windowNanos),
        isWarnOnly());
  }

  private static final class Sample {
    final long nanos;
    final long gcMs;

    Sample(long nanos, long gcMs) {
      this.nanos = nanos;
      this.gcMs = gcMs;
    }
  }
}
