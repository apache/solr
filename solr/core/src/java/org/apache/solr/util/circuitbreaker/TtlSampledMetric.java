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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Single-flight, time-bounded cache around an expensive metric sample.
 *
 * <ul>
 *   <li><b>Fresh:</b> within the TTL window every caller returns the cached value with one volatile
 *       read.
 *   <li><b>Stale:</b> exactly one thread runs the underlying sampler at a time (chosen by CAS); all
 *       other concurrent callers immediately return the most recent published value rather than
 *       queueing behind the refresh or piling onto the sampler.
 *   <li><b>Cold (no sample yet):</b> guarded by a one-time {@code synchronized (this)} block so
 *       only the first caller computes; the rest see the result. The monitor is held across the
 *       sampler invocation, so concurrent first-callers will block — but this path runs at most
 *       once per instance, and only callers that arrive before any value has been published are
 *       affected.
 * </ul>
 *
 * <p>Used by {@link LoadAverageCircuitBreaker} and {@link MemoryCircuitBreaker} so that high-QPS
 * admission control cannot stampede the OS load-average syscall or the post-GC heap-pool walk: even
 * under thousands of concurrent {@code isTripped()} callers, the underlying sampler is invoked at
 * most once per TTL window.
 *
 * <p><b>Exception behavior:</b> if the {@code source} supplier throws, the exception propagates to
 * the calling thread and no new sample is published. Any previously-published value remains and
 * other concurrent callers continue to see it; the next caller to find the entry stale will retry
 * the supplier. The {@code refreshing} flag is always released, so a thrown sampler does not wedge
 * the single-flight latch.
 */
final class TtlSampledMetric<T> {

  private final long ttlNanos;
  private final AtomicReference<Sample<T>> sample = new AtomicReference<>();
  private final AtomicBoolean refreshing = new AtomicBoolean(false);

  TtlSampledMetric(long ttlMs) {
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMs);
  }

  T get(Supplier<T> source) {
    long now = System.nanoTime();
    Sample<T> s = sample.get();
    if (s != null && (now - s.nanos) < ttlNanos) {
      return s.value;
    }
    if (s != null) {
      // Stale value present: single-flight refresh — exactly one thread runs the sampler;
      // every other concurrent caller returns the most recent published value immediately.
      if (refreshing.compareAndSet(false, true)) {
        try {
          T v = source.get();
          sample.set(new Sample<>(System.nanoTime(), v));
          return v;
        } finally {
          refreshing.set(false);
        }
      }
      // Re-read so we return whatever the winning thread may have just published, rather than
      // the older snapshot we captured at the top of this call.
      Sample<T> latest = sample.get();
      return latest != null ? latest.value : s.value;
    }
    // No sample yet — one-time cold path. Synchronize so the first wave of concurrent callers
    // does not all run the sampler. The monitor is held across the sampler invocation; this is
    // acceptable because the cold path runs at most once per successful publish, and only callers
    // that arrive before any value has been published are affected.
    synchronized (this) {
      s = sample.get();
      if (s != null) {
        return s.value;
      }
      T v = source.get();
      sample.set(new Sample<>(System.nanoTime(), v));
      return v;
    }
  }

  private static final class Sample<T> {
    final long nanos;
    final T value;

    Sample(long nanos, T value) {
      this.nanos = nanos;
      this.value = value;
    }
  }
}
