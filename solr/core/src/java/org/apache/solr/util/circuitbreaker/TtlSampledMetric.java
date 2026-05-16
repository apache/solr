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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.DoubleSupplier;

/**
 * Time-bounded cache around an expensive double-valued metric sample. Concurrent readers within one
 * TTL window share a single computed value; on cache miss, multiple threads may each compute before
 * the latest store wins, which is bounded and acceptable.
 *
 * <p>Used by {@link LoadAverageCircuitBreaker} and {@link CPUCircuitBreaker} so that high-QPS
 * admission control does not poll OS load-average syscalls or Prometheus metric scans more often
 * than the underlying signals can move.
 */
final class TtlSampledMetric {

  private final long ttlNanos;
  private final AtomicReference<Sample> sample = new AtomicReference<>();

  TtlSampledMetric(long ttlMs) {
    this.ttlNanos = TimeUnit.MILLISECONDS.toNanos(ttlMs);
  }

  double get(DoubleSupplier source) {
    long now = System.nanoTime();
    Sample s = sample.get();
    if (s != null && (now - s.nanos) < ttlNanos) {
      return s.value;
    }
    double v = source.getAsDouble();
    sample.set(new Sample(now, v));
    return v;
  }

  private static final class Sample {
    final long nanos;
    final double value;

    Sample(long nanos, double value) {
      this.nanos = nanos;
      this.value = value;
    }
  }
}
