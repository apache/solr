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
import org.apache.solr.util.RTimer;

/**
 * This class records the elapsed time (in milliseconds) to a {@link LongHistogram} with the use of
 * a {@link MetricTimer}
 */
public class AttributedLongTimer extends AttributedLongHistogram {

  public AttributedLongTimer(LongHistogram histogram, Attributes attributes) {
    super(histogram, attributes);
  }

  /**
   * Return a {@link MetricTimer} and starts the timer. When the timer calls {@link
   * MetricTimer#stop()}, the elapsed time is recorded
   */
  public MetricTimer start() {
    return new MetricTimer(this);
  }

  public static class MetricTimer extends RTimer {
    private final AttributedLongTimer bound;

    private MetricTimer(AttributedLongTimer bound) {
      this.bound = bound;
    }

    @Override
    public double stop() {
      double elapsedTime = super.stop();
      bound.record(Double.valueOf(elapsedTime).longValue());
      return elapsedTime;
    }
  }
}
