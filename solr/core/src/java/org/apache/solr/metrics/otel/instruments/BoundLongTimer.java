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
package org.apache.solr.metrics.otel.instruments;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongHistogram;
import java.util.concurrent.TimeUnit;

/**
 * A thread-local timer built on top of an OpenTelemetry {@link LongHistogram}.
 *
 * <p>This class tracks the elapsed time between calls to {@link #start()} and {@link #stop()} on a
 * per-thread basis. It records the elapsed time (in milliseconds) to the underlying histogram with
 * a fixed set of {@link Attributes}. Each thread maintains its own start time using a {@link
 * ThreadLocal}, allowing concurrent timing operations on separate threads without interference.
 */
public class BoundLongTimer extends BoundLongHistogram {

  private final ThreadLocal<Long> startTimeNanos = new ThreadLocal<>();

  public BoundLongTimer(LongHistogram histogram, Attributes attributes) {
    super(histogram, attributes);
  }

  public void start() {
    if (startTimeNanos.get() != null) {
      throw new IllegalStateException("Timer already started on this thread");
    }
    startTimeNanos.set(System.nanoTime());
  }

  public void stop() {
    Long start = startTimeNanos.get();
    if (start == null) {
      throw new IllegalStateException("Must call start() before stop()");
    }

    try {
      long elapsedNanos = System.nanoTime() - start;
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(elapsedNanos);
      histogram.record(elapsedMs, attributes);
    } finally {
      startTimeNanos.remove();
    }
  }
}
