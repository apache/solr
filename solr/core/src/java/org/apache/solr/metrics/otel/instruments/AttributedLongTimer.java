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
 * This class records the elapsed time (in milliseconds) to a {@link LongHistogram}. Each thread
 * measures its own timer allowing concurrent timing operations on separate threads..
 */
public class AttributedLongTimer extends AttributedLongHistogram {

  private final ThreadLocal<Long> startTimeNanos = new ThreadLocal<>();

  public AttributedLongTimer(LongHistogram histogram, Attributes attributes) {
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
